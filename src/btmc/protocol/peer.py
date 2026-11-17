"""One connection to a peer"""

import math
from collections.abc import Callable
from enum import StrEnum
from typing import Literal, Self, TypedDict, overload

import anyio
from anyio.streams.buffered import BufferedByteStream
from torrent_models.types.v2 import BLOCK_SIZE

from btmc.exceptions import MalformedMessage, PeerError
from btmc.protocol.messages import (
    Bitfield,
    BitfieldMessage,
    Cancel,
    Choke,
    Handshake,
    Have,
    Interested,
    KeepAlive,
    NotInterested,
    PeerMessage,
    PeerMessageId,
    Piece,
    Request,
    Unchoke,
)
from btmc.protocol.shared import Shared
from btmc.types import Addr, Block

PEER_ERRORS = (
    OSError,
    anyio.EndOfStream,
    anyio.IncompleteRead,
    anyio.BrokenResourceError,
    anyio.ClosedResourceError,
    PeerError,
)
"""What ends a connection to a peer"""


class CloseReason(StrEnum):
    """Why a connection to a peer closed"""

    CONNECT_FAILED = "couldn't connect"
    CONNECTION_LOST = "connection lost"
    PROTOCOL_ERROR = "broke the protocol"
    MALFORMED_MESSAGE = "sent a malformed message"
    NO_HANDSHAKE = "no handshake"
    REQUESTS_TIMED_OUT = "requests timed out"
    IDLE = "idle"
    BANNED = "banned"
    NEEDED_ELSEWHERE = "connection needed by another torrent"
    STOPPING = "stopping"

    @property
    def penalize(self) -> bool:
        """Whether it's the peer's fault, so we wait longer before retrying it"""
        return self not in (CloseReason.NEEDED_ELSEWHERE, CloseReason.STOPPING)

    @property
    def ban(self) -> bool:
        """Whether the peer did something no working client does, so it gets banned"""
        return self is CloseReason.MALFORMED_MESSAGE


type PeerEvent = Literal["message", "released", "connected", "disconnected"]
"""
What callbacks can be added for, see :meth:`.Peer.add_callback`

- ``"message"``: a message arrived that changes something. Called with the peer and the message.
- ``"released"``: the peer won't answer requests we made, because it choked us or the
  connection closed. Called with the peer and the blocks we'd asked for, in order.
- ``"connected"``: both handshakes are done, so messages can go back and forth.
  Called with the peer.
- ``"disconnected"``: a connection that was ``"connected"`` has closed, and
  :attr:`.Peer.close_reason` says why. Called with the peer.
"""

type MessageCallback = Callable[[Peer, PeerMessage], None]
type ReleasedCallback = Callable[[Peer, list[Block]], None]
type ConnectionCallback = Callable[[Peer], None]


class _Callbacks(TypedDict):
    message: list[MessageCallback]
    released: list[ReleasedCallback]
    connected: list[ConnectionCallback]
    disconnected: list[ConnectionCallback]


class Peer:
    """
    Our end of a connection to a peer.

    :meth:`.run` opens the connection, moves messages until it closes, and closes it.
    Messages that arrive go to the callbacks added with :meth:`.add_callback`, except the ones
    that change nothing: keepalives, haves for pieces we knew the peer had,
    and blocks we didn't ask for. So do the requests it won't answer any more,
    when it chokes us or the connection closes. :meth:`.send` queues a message,
    and :meth:`.close` ends the connection, from anywhere.
    A message whose length is wrong for its kind closes the connection before its body is read.
    Messages of extensions we don't speak are skipped, as if they never came.

    It keeps track of the handshake, whether the peer is choking us, whether we told it
    we're interested, which pieces it has, and which of our requests it hasn't answered.

    Args:
        addr: the peer's address
        info_hash: the info hash to handshake with
        n_pieces: the number of pieces in the torrent
        shared: what the client's torrents share: our peer id, settings,
            and the limit on connections, which a connection holds a place in while it's open
        now: when the connection was made, or when we started making it
        connection: an open connection. Without one, :meth:`.run` connects to the peer.
            For a peer that connected to us, use :meth:`.from_incoming`.
    """

    def __init__(
        self,
        addr: Addr,
        info_hash: bytes,
        n_pieces: int,
        shared: Shared,
        now: float,
        connection: BufferedByteStream | None = None,
    ):
        self.addr = addr
        self.info_hash = info_hash
        self.shared = shared
        self.config = shared.config
        self.remote_peer_id: bytes | None = None
        """Set once the peer's handshake arrives"""
        self.connected_at: float | None = None
        """When both handshakes were done, so messages could go back and forth"""
        self.has = Bitfield(n_pieces)
        self.choked = True
        self.interested = False
        self.requests: set[Block] = set()
        """outstanding requests we have made"""
        self.received = 0
        """Bytes of blocks received"""
        self.close_reason: CloseReason | None = None
        """Set when the connection should end, or has ended"""
        self.close_error: Exception | None = None
        """What went wrong, if the connection ended because of an error"""
        self.received_at = now
        """When the peer last sent anything, or when we started connecting"""
        self._progress_at = now
        self._progress_bytes = 0
        self._connection = connection
        self._run_scope: anyio.CancelScope | None = None
        """Set once :meth:`.run` starts, so :meth:`.close` can stop it wherever it's got to"""
        self._outgoing, self._outgoing_reader = anyio.create_memory_object_stream[bytes](math.inf)
        """Encoded messages waiting to be sent. Closed when the connection should close."""
        self._outgoing.send_nowait(Handshake(info_hash, shared.peer_id).encode())
        self._callbacks: _Callbacks = {
            "message": [],
            "released": [],
            "connected": [],
            "disconnected": [],
        }

    @classmethod
    def from_incoming(
        cls,
        addr: Addr,
        handshake: Handshake,
        n_pieces: int,
        shared: Shared,
        now: float,
        connection: BufferedByteStream,
    ) -> Self:
        """
        A peer that connected to us, whose handshake we've read from the connection.
        Raises PeerError if we can't accept the handshake, like when it's our own.
        """
        peer = cls(addr, handshake.info_hash, n_pieces, shared, now, connection)
        try:
            peer._receive_handshake(handshake, now)
        except PeerError as e:
            peer.close(CloseReason.PROTOCOL_ERROR, e)
            raise
        return peer

    def __repr__(self) -> str:
        return f"<Peer {self.addr}>"

    @property
    def closed(self) -> bool:
        return self.close_reason is not None

    @property
    def penalize(self) -> bool:
        """Whether the connection ending counts against the peer when retrying it"""
        return self.close_reason is not None and self.close_reason.penalize

    @overload
    def add_callback(self, event: Literal["message"], callback: MessageCallback) -> None: ...

    @overload
    def add_callback(self, event: Literal["released"], callback: ReleasedCallback) -> None: ...

    @overload
    def add_callback(
        self, event: Literal["connected", "disconnected"], callback: ConnectionCallback
    ) -> None: ...

    def add_callback(
        self, event: PeerEvent, callback: MessageCallback | ReleasedCallback | ConnectionCallback
    ) -> None:
        """Call ``callback`` whenever ``event`` happens"""
        if event not in self._callbacks:
            raise ValueError(f"no such event: {event}")
        # the overloads make sure the callback is the right kind for the event
        self._callbacks[event].append(callback)  # type: ignore[arg-type]

    # ---- the connection ----

    async def run(self) -> None:
        """
        Open the connection, move messages until it closes, and close it,
        holding a place in the client's connection limit while it's open.
        Sends a keepalive whenever there's been nothing to send for a while,
        and closes the connection if the peer goes quiet for too long.
        """
        connection = self._connection
        try:
            with anyio.CancelScope() as self._run_scope:
                if self.closed:
                    return
                async with self.shared.connections:
                    if connection is None:
                        with anyio.fail_after(self.config.connect_timeout):
                            connection = BufferedByteStream(
                                await anyio.connect_tcp(str(self.addr.ip), self.addr.port)
                            )
                    async with anyio.create_task_group() as tg:
                        tg.start_soon(self._send_loop, connection, tg.cancel_scope)
                        tg.start_soon(self._timeout_loop)
                        await self._receive_loop(connection)
                        tg.cancel_scope.cancel()
        except PEER_ERRORS as e:
            # the loops handle their own errors, so this is from connecting
            self.close(CloseReason.CONNECT_FAILED, e)
        except anyio.get_cancelled_exc_class():
            self.close(CloseReason.STOPPING)
            raise
        finally:
            self.close(CloseReason.CONNECTION_LOST)
            try:
                # last, because awaiting raises again if we're being cancelled
                if connection is not None:
                    await anyio.aclose_forcefully(connection)
            finally:
                if self.connected_at is not None:
                    for callback in self._callbacks["disconnected"]:
                        callback(self)

    async def _receive_loop(self, connection: BufferedByteStream) -> None:
        try:
            if self.remote_peer_id is None:
                handshake = Handshake.decode(await connection.receive_exactly(Handshake.LENGTH))
                self._receive_handshake(handshake, anyio.current_time())
            self.connected_at = anyio.current_time()
            for callback in self._callbacks["connected"]:
                callback(self)
            while not self.closed:
                length = int.from_bytes(await connection.receive_exactly(4))
                if length:
                    message_id = (await connection.receive_exactly(1))[0]
                    # checked before reading the body, so a peer can't make us hold a long one
                    if not PeerMessage.valid_length(message_id, length - 1, self.has.n):
                        raise MalformedMessage(f"message {message_id} has invalid length of {length} bytes")
                    if message_id not in PeerMessageId:
                        # an extension we don't speak: skipped without holding it, however long
                        left = length - 1
                        while left:
                            left -= len(await connection.receive(min(left, BLOCK_SIZE)))
                        continue
                    body = await connection.receive_exactly(length - 1)
                    received = PeerMessage.from_body(message_id, body)
                else:
                    received = KeepAlive()
                msg = self._receive(received, anyio.current_time())
                if msg is not None:
                    for message_callback in self._callbacks["message"]:
                        message_callback(self, msg)
        except MalformedMessage as e:
            self.close(CloseReason.MALFORMED_MESSAGE, e)
        except PeerError as e:
            self.close(CloseReason.PROTOCOL_ERROR, e)
        except PEER_ERRORS as e:
            self.close(CloseReason.CONNECTION_LOST, e)

    async def _send_loop(self, connection: BufferedByteStream, done: anyio.CancelScope) -> None:
        try:
            while not self.closed:
                data = KeepAlive().encode()
                with anyio.move_on_after(self.config.keepalive_interval):
                    data = await self._outgoing_reader.receive()
                # drain the rest of the queue
                # to send it out in fewer larger packets rather than many smaller ones
                await connection.send(data + self._take_queued())
        except PEER_ERRORS as e:
            self.close(CloseReason.CONNECTION_LOST, e)
        finally:
            # the receive loop is waiting for bytes, and won't see the connection closing
            done.cancel()

    async def _timeout_loop(self) -> None:
        while not self.closed:
            await anyio.sleep(self.config.timeout_check_interval)
            self._check_timeouts(anyio.current_time())

    def _take_queued(self) -> bytes:
        """Take everything queued to send, without waiting"""
        chunks = []
        while True:
            try:
                chunks.append(self._outgoing_reader.receive_nowait())
            except (anyio.WouldBlock, anyio.EndOfStream, anyio.ClosedResourceError):
                return b"".join(chunks)

    # ---- protocol ----

    def _receive_handshake(self, handshake: Handshake, now: float) -> None:
        """The peer's handshake arrived. Raises PeerError if it's not for this connection."""
        self.received_at = now
        if handshake.info_hash != self.info_hash:
            raise PeerError("peer answered with a different info hash")
        if handshake.peer_id == self.shared.peer_id:
            raise PeerError("connected to ourselves")
        self.remote_peer_id = handshake.peer_id

    def _receive(self, msg: PeerMessage, now: float) -> PeerMessage | None:
        """
        A message arrived after the handshake, whose length was checked already.
        Returns it if it changes anything. Raises PeerError if the peer broke the protocol.
        """
        self.received_at = now
        if isinstance(msg, KeepAlive):
            return None
        match msg:
            case Choke():
                self.choked = True
                # a peer throws away our requests when it chokes us
                self._release_requests()
            case Unchoke():
                self.choked = False
            case Have(index=index):
                if index >= self.has.n:
                    raise PeerError(f"have for piece {index} out of range")
                if index in self.has:
                    return None
                self.has.add(index)
            case BitfieldMessage(bits=bits):
                # the spec says it comes first, but like libtorrent, we take a later one too
                self.has = bits.for_pieces(self.has.n)
            case Piece(index=index, begin=begin, block=data):
                block = Block(index, begin, len(data))
                if block not in self.requests:
                    return None
                self.requests.remove(block)
                self.received += len(data)
        return msg

    def send(self, msg: PeerMessage) -> None:
        match msg:
            case Interested():
                self.interested = True
            case NotInterested():
                self.interested = False
            case Request(index=index, begin=begin, length=length):
                self.requests.add(Block(index, begin, length))
            case Cancel(index=index, begin=begin, length=length):
                self.requests.discard(Block(index, begin, length))
        if not self.closed:
            self._outgoing.send_nowait(msg.encode())

    def set_interested(self, interested: bool) -> None:
        """Tell the peer whether we're interested, if that's changed"""
        if interested != self.interested:
            self.send(Interested() if interested else NotInterested())

    def close(self, reason: CloseReason, error: Exception | None = None) -> None:
        """
        End the connection, and stop :meth:`.run` wherever it's got to:
        waiting for a place in the connection limit, connecting, or connected.
        Only the first reason given counts.

        Args:
            reason: why
            error: what went wrong, if it was an error
        """
        if not self.closed:
            self.close_reason = reason
            self.close_error = error
            self._outgoing.close()
            self._outgoing_reader.close()
            self._release_requests()
            if self._run_scope is not None:
                self._run_scope.cancel()

    def _check_timeouts(self, now: float) -> None:
        """
        Close the connection if the peer has gone quiet: no handshake, no blocks for our
        pending requests, or nothing at all, for too long
        """
        if not self.requests or self.received != self._progress_bytes:
            self._progress_at, self._progress_bytes = now, self.received
        if self.remote_peer_id is None:
            if now - self.received_at > self.config.connect_timeout + self.config.handshake_timeout:
                self.close(CloseReason.NO_HANDSHAKE)
        elif self.requests and now - self._progress_at > self.config.request_timeout:
            self.close(CloseReason.REQUESTS_TIMED_OUT)
        elif now - self.received_at > self.config.peer_idle_timeout:
            self.close(CloseReason.IDLE)

    def _release_requests(self) -> None:
        """Forget our pending requests, and hand them to the ``"released"`` callbacks"""
        if not self.requests:
            return
        released = sorted(self.requests)
        self.requests.clear()
        for callback in self._callbacks["released"]:
            callback(self, released)
