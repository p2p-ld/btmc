import contextlib
import logging
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any, Literal, TypedDict, overload

import anyio
from anyio.abc import TaskGroup
from anyio.streams.buffered import BufferedByteStream
from torrent_models import Torrent

from btmc.exceptions import TrackerError, TrackerRefused
from btmc.protocol.messages import (
    AnnounceEvent,
    AnnounceRequest,
    Bitfield,
    BitfieldMessage,
    Handshake,
    Have,
    PeerMessage,
    Piece,
    Request,
    Unchoke,
)
from btmc.protocol.peer import CloseReason, ConnectionCallback, Peer
from btmc.protocol.picker import PiecePicker, PieceState
from btmc.protocol.shared import BanReason, Shared
from btmc.protocol.tracker import announce, check_url
from btmc.types import Addr, AnyAddr, Block, DomainAddr, PieceIndex
from btmc.utils import info_hashes

log = logging.getLogger(__name__)

type SwarmEvent = Literal["block", "connected", "disconnected"]
"""
What callbacks can be added for, see :meth:`.Swarm.add_callback`

- ``"block"``: a block we asked for arrived. Called with the block, its data,
  and the address of the peer that sent it.
- ``"connected"``: a peer's connection was made, see :data:`.PeerEvent`. Called with the peer.
- ``"disconnected"``: a peer that was ``"connected"`` has gone, and isn't in
  :attr:`.Swarm.peers` any more. Called with the peer.
"""

type BlockCallback = Callable[[Block, bytes, Addr], None]


class _Callbacks(TypedDict):
    block: list[BlockCallback]
    connected: list[ConnectionCallback]
    disconnected: list[ConnectionCallback]


@dataclass
class _Candidate:
    """A peer we could connect to"""

    info_hashes: list[bytes]
    failures: int = 0
    retry_at: float = 0


class Swarm:
    """
    Manages the peers of one torrent

    Handles announcing, connecting and creating peers from the listener socket.
    Distributes wanted blocks and pieces from the :class:`.PiecePicker`,
    and forwards blocks up to the :class:`.TorrentState` for storage and checking.

    Does resource management, ensuring that checked-out connections
    don't get stuck on inactive peers.

    Treats the v1 and v2 swarms of a hybrid torrent as interchangeable,
    depending on what peers support.

    Its tasks, all in one task group until it's stopped:

    - one per connection, running the peer, which connects, hands each message that arrives
      to :meth:`._handle_message` and the requests it won't answer to :meth:`._released`,
      and closes itself
    - one per tracker and info hash, announcing while anything is wanted
    - one per known peer given as a host name, looking it up
    - a heartbeat, every :attr:`.Config.connect_interval`, for what depends on time:
      retrying peers whose backoff has run out, and giving up connections
      other torrents are waiting for

    Args:
        torrent: the torrent
        picker: the torrent's pieces
        shared: what this torrent shares with the client's other torrents
        trackers: tracker tiers to use instead of the torrent's own
        known_peers: peers to connect to without asking a tracker
    """

    def __init__(
        self,
        torrent: Torrent[Any],
        picker: PiecePicker,
        shared: Shared,
        *,
        trackers: list[list[str]] | None = None,
        known_peers: Sequence[AnyAddr] = (),
    ):
        self.picker = picker
        picker.add_callback(PieceState.OPEN, self._opened)
        picker.add_callback(PieceState.HAVE, self._had)
        self.shared = shared
        shared.add_callback("ban", self._banned)

        self.config = shared.config
        self.n_pieces = torrent.layout.n_pieces
        self.piece_length = torrent.info.piece_length
        self.info_hashes = info_hashes(torrent)
        self.trackers: list[str] = self._checked_trackers(torrent.flat_trackers if trackers is None else trackers)
        """
        Every tracker's url, apart from ones we can't announce to.
        We announce to all of them at once, like qBittorrent, rather than one tier at a time.
        """
        self.known_peers = list(known_peers)
        """Peers to connect to without asking a tracker, which become candidates when it runs"""

        self.peers: dict[Addr, Peer] = {}
        self.announced: set[tuple[str, bytes]] = set()
        """(tracker, info hash) that know about us, so need telling when we stop"""
        self._downloaded = 0
        """Bytes of every block that's arrived, for announces"""

        self._callbacks: _Callbacks = {"block": [], "connected": [], "disconnected": []}
        self._candidates: dict[Addr, _Candidate] = {}
        self._tg: TaskGroup | None = None
        self._stopping = False
        self._stopped = anyio.Event()

    @overload
    def add_callback(self, event: Literal["block"], callback: BlockCallback) -> None: ...

    @overload
    def add_callback(
        self, event: Literal["connected", "disconnected"], callback: ConnectionCallback
    ) -> None: ...

    def add_callback(self, event: SwarmEvent, callback: BlockCallback | ConnectionCallback) -> None:
        """Call ``callback`` whenever ``event`` happens"""
        if event not in self._callbacks:
            raise ValueError(f"no such event: {event}")
        # the overloads make sure the callback is the right kind for the event
        self._callbacks[event].append(callback)  # type: ignore[arg-type]

    @overload
    def remove_callback(self, event: Literal["block"], callback: BlockCallback) -> None: ...

    @overload
    def remove_callback(
        self, event: Literal["connected", "disconnected"], callback: ConnectionCallback
    ) -> None: ...

    def remove_callback(
        self, event: SwarmEvent, callback: BlockCallback | ConnectionCallback
    ) -> None:
        """Stop calling a callback, like when whatever added it goes away before the torrent"""
        if event not in self._callbacks:
            raise ValueError(f"no such event: {event}")
        self._callbacks[event].remove(callback)  # type: ignore[arg-type]

    async def run(self) -> None:
        """Run until stopped, then tell the trackers we stopped"""
        try:
            async with anyio.create_task_group() as self._tg:
                if self._stopping:
                    self._tg.cancel_scope.cancel()
                for url in self.trackers:
                    for info_hash in self.info_hashes:
                        self._tg.start_soon(self._announce_loop, url, info_hash)
                for addr in self.known_peers:
                    if isinstance(addr, DomainAddr):
                        self._tg.start_soon(self._resolve_candidate, addr, self.info_hashes)
                        continue
                    for info_hash in self.info_hashes:
                        self.add_candidate(addr, info_hash)
                self._connect(anyio.current_time())
                while True:
                    await anyio.sleep(self.config.connect_interval)
                    self._heartbeat(anyio.current_time())
        finally:
            self._tg = None
            # the client's other torrents can still ban peers after this one is gone
            self.shared.remove_callback("ban", self._banned)
            with anyio.CancelScope(shield=True):
                await self._announce_all_stopped()
            self._stopped.set()

    async def stop(self) -> None:
        """Close every connection, and tell the trackers we stopped"""
        self._stopping = True
        if self._tg is not None:
            self._tg.cancel_scope.cancel()
        await self._stopped.wait()

    def accept_connection(
        self, connection: BufferedByteStream, addr: Addr, handshake: Handshake
    ) -> bool:
        """
        Take an incoming connection, whose handshake was read already.
        False if we don't want it. Raises PeerError if we can't accept the handshake.
        """
        if not self._should_connect(addr):
            return False
        now = anyio.current_time()
        self._start_peer(
            Peer.from_incoming(addr, handshake, self.n_pieces, self.shared, now, connection)
        )
        return True

    def _heartbeat(self, now: float) -> None:
        self._give_up_connections()
        self._connect(now)

    def _connect(self, now: float) -> None:
        """Connect to the candidates that are ready, while we want more connections"""
        for addr, info_hash in self._ready_candidates(now):
            if self._should_connect(addr):
                self._start_peer(Peer(addr, info_hash, self.n_pieces, self.shared, now))

    def _give_up_connections(self) -> None:
        """If nothing is wanted and other torrents are waiting for a connection, close ours"""
        if not self.picker.n_wanted and self.shared.connections.statistics().tasks_waiting:
            for peer in self.peers.values():
                self.remove_peer(peer, CloseReason.NEEDED_ELSEWHERE)

    # ---- peers ----

    def add_candidate(self, addr: Addr, info_hash: bytes) -> None:
        """
        A peer we could connect to, in the swarm for an info hash,
        unless it's banned or blocked
        """
        if self.shared.is_banned(addr) or self.shared.is_blocked(addr):
            return
        candidate = self._candidates.setdefault(addr, _Candidate(info_hashes=[]))
        if info_hash not in candidate.info_hashes:
            candidate.info_hashes.append(info_hash)
            candidate.info_hashes.sort(key=self.info_hashes.index)

    async def _resolve_candidate(self, name: DomainAddr, info_hashes: Sequence[bytes]) -> None:
        """Look up a peer given by host name, and add the address it points to as a candidate"""
        try:
            with anyio.fail_after(self.config.connect_timeout):
                addr = await name.resolve()
        # including the TimeoutError from fail_after
        except OSError as e:
            log.debug("couldn't look up %s: %r", name, e)
            return
        for info_hash in info_hashes:
            self.add_candidate(addr, info_hash)
        self._connect(anyio.current_time())

    def remove_peer(self, peer: Peer, reason: CloseReason) -> None:
        """Close a connection. It's forgotten once it has finished closing."""
        peer.close(reason)

    def _peer_ended(self, peer: Peer, now: float) -> None:
        """
        A connection ended: forget it, say so if it had connected,
        and wait before trying the peer again
        """
        del self.peers[peer.addr]
        if peer.connected_at is not None:
            for callback in self._callbacks["disconnected"]:
                callback(peer)
        if peer.close_reason is not None and peer.close_reason.ban:
            self.shared.ban(peer.addr, BanReason.MALFORMED_MESSAGE)
        candidate = self._candidates.get(peer.addr)
        if candidate is None or not peer.penalize:
            return
        if peer.received:
            candidate.failures = 0
            candidate.retry_at = now + self.config.peer_retry
        else:
            candidate.failures += 1
            candidate.retry_at = now + self.config.peer_backoff * 2**candidate.failures
            if candidate.failures > self.config.max_peer_failures:
                del self._candidates[peer.addr]

    def _ready_candidates(self, now: float) -> list[tuple[Addr, bytes]]:
        """
        Candidates whose wait after their last connection is over, fewest failures first,
        then in the order they were found, and the info hash to use with each
        """
        ready = sorted(
            ((addr, c) for addr, c in self._candidates.items() if c.retry_at <= now),
            key=lambda ready: ready[1].failures,
        )
        connect = []
        for addr, c in ready:
            # if a peer is in both swarms and fails on one hash, try the other next time
            connect.append((addr, c.info_hashes[c.failures % len(c.info_hashes)]))
        return connect

    def _should_connect(self, addr: Addr) -> bool:
        """Whether to open a connection with a peer, whichever side starts it"""
        return (
            self._tg is not None
            and self.picker.n_wanted > 0
            and len(self.peers) < self.config.max_peers
            and addr not in self.peers
            and not self.shared.is_banned(addr)
        )

    def _start_peer(self, peer: Peer) -> None:
        assert self._tg is not None
        peer.add_callback("message", self._handle_message)
        peer.add_callback("released", self._released)
        peer.add_callback("connected", self._peer_connected)
        self.peers[peer.addr] = peer
        self._tg.start_soon(self._run_peer, peer)

    async def _run_peer(self, peer: Peer) -> None:
        """Run a connection to a peer until it closes, then connect to another one"""
        try:
            await peer.run()
        finally:
            self._peer_ended(peer, anyio.current_time())
            log.debug("%s closed: %s %r", peer, peer.close_reason, peer.close_error)
        # not reached when we're stopping, since run raises the cancellation
        self._connect(anyio.current_time())

    # --- callbacks ---

    def _opened(self, pieces: list[PieceIndex]) -> None:
        """
        Pieces are open to fetch, because they were just wanted or failed their check:
        tell the peers that have any of them we're interested, ask peers for blocks, and connect.
        A failed piece makes room for another to start, so every peer is asked.
        """
        opened = Bitfield.from_pieces(self.n_pieces, pieces)
        for peer in self.peers.values():
            if opened.intersects(peer.has):
                peer.set_interested(True)
        for peer in self.peers.values():
            self._fill(peer)
        self._connect(anyio.current_time())

    def _had(self, pieces: list[PieceIndex]) -> None:
        """Pieces passed, making room for others to start, so ask every peer for blocks"""
        for peer in self.peers.values():
            self._fill(peer)
        self._give_up_connections()

    def _banned(self, addr: Addr) -> None:
        self._candidates.pop(addr, None)
        if (peer := self.peers.get(addr)) is not None:
            self.remove_peer(peer, CloseReason.BANNED)

    def _released(self, peer: Peer, blocks: list[Block]) -> None:
        """A peer won't send blocks we asked it for, so ask the other peers"""
        self.picker.release_blocks(blocks)
        for other in self.peers.values():
            self._fill(other)

    def _peer_connected(self, peer: Peer) -> None:
        for callback in self._callbacks["connected"]:
            callback(peer)

    def _handle_message(self, peer: Peer, msg: PeerMessage) -> None:
        """Handle a message from a peer. A piece this completes goes on the queue to be checked."""
        match msg:
            case Unchoke():
                self._fill(peer)
            case Have(index=index):
                if self.picker.is_wanted(index):
                    peer.set_interested(True)
                self._fill(peer)
            case BitfieldMessage():
                # bit of a code smell here - the peer already got the message,
                # it just can't check for interest because it doesn't hold the piece picker
                # if this keeps getting tangled the peers might need the piece picker.
                peer.set_interested(self.picker.peer_has_wanted_pieces(peer.has))
            case Piece(index=index, begin=begin, block=data):
                self._downloaded += len(data)
                for callback in self._callbacks["block"]:
                    callback(Block(index, begin, len(data)), data, peer.addr)
                self._fill(peer)

    def _fill(self, peer: Peer) -> None:
        """Request blocks until the peer's pipeline is full or it has nothing we want"""
        if peer.choked or not peer.interested or peer.closed:
            return
        while len(peer.requests) < self.config.pipeline_depth:
            block = self.picker.pick(peer.has)
            if block is None:
                if not peer.requests:
                    peer.set_interested(self.picker.peer_has_wanted_pieces(peer.has))
                return
            peer.send(Request(*block))

    # --- trackers ---

    async def _announce_loop(self, url: str, info_hash: bytes) -> None:
        """
        Announce to a tracker every so often while anything is wanted.
        While nothing is wanted, just sleep.
        Then apply any tracker backoffs, and announce that thing.
        If we stop wanting more pieces, also sends a 'stopped' message to the tracker
        """
        while True:
            await self.picker.wait_for_wanted()
            # another torrent might have found the tracker down while we waited
            await anyio.sleep_until(self.shared.tracker_ready_at(url))
            await anyio.sleep_until(await self._announce(url, info_hash))
            if not self.picker.n_wanted and (url, info_hash) in self.announced:
                await self._announce_stopped(url, info_hash)

    async def _announce(self, url: str, info_hash: bytes) -> float:
        """
        Announce to a tracker, saying we started if it doesn't know about us yet,
        and return when to announce to it again
        """
        started = (url, info_hash) in self.announced
        event = AnnounceEvent.NONE if started else AnnounceEvent.STARTED
        request = self._announce_request(info_hash, event)
        try:
            async with self.shared.announces:
                response = await announce(url, request, http=self.shared.http, config=self.config)
        except TrackerRefused as e:
            log.debug("%s refused %s: %s", url, info_hash.hex(), e)
            now = anyio.current_time()
            self.shared.announced(url, now)
            return now + self.config.announce_retry
        except TrackerError as e:
            log.debug("announce to %s failed: %s", url, e)
            self.shared.announced(url, anyio.current_time(), answered=False)
            return self.shared.tracker_ready_at(url)
        now = anyio.current_time()
        self.shared.announced(url, now)
        log.debug("%s gave %d peers for %s", url, len(response.peers), info_hash.hex())
        self.announced.add((url, info_hash))
        async with anyio.create_task_group() as tg:
            for addr in response.peers:
                if isinstance(addr, DomainAddr):
                    tg.start_soon(self._resolve_candidate, addr, [info_hash])
                    continue
                self.add_candidate(addr, info_hash)
            self._connect(now)
        if response.interval is None:
            return now + self.config.announce_retry
        return now + min(
            max(response.interval, self.config.min_announce_interval),
            self.config.max_announce_interval,
        )

    async def _announce_stopped(self, url: str, info_hash: bytes) -> None:
        request = self._announce_request(info_hash, AnnounceEvent.STOPPED)
        with contextlib.suppress(TrackerError):
            async with self.shared.announces:
                await announce(url, request, http=self.shared.http, config=self.config)
        self.announced.discard((url, info_hash))

    async def _announce_all_stopped(self) -> None:
        with anyio.move_on_after(self.config.stopped_announce_timeout):
            async with anyio.create_task_group() as tg:
                for url, info_hash in list(self.announced):
                    tg.start_soon(self._announce_stopped, url, info_hash)

    def _announce_request(self, info_hash: bytes, event: AnnounceEvent) -> AnnounceRequest:
        left = (self.n_pieces - self.picker.have.count()) * self.piece_length
        return AnnounceRequest(
            info_hash=info_hash,
            peer_id=self.shared.peer_id,
            port=self.shared.port,
            downloaded=self._downloaded,
            left=max(left, 0),
            event=event,
            numwant=self.config.numwant,
        )

    def _checked_trackers(self, tiers: list[list[str]]) -> list[str]:
        trackers = []
        for url in dict.fromkeys(url for tier in tiers for url in tier):
            try:
                check_url(url)
            except TrackerError as e:
                log.info("skipping tracker %r: %s", url, e)
            else:
                trackers.append(url)
        return trackers
