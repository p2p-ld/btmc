import contextlib
from collections.abc import AsyncGenerator, Iterator
from unittest.mock import patch

import anyio
import pytest
from anyio.streams.buffered import BufferedByteStream

from btmc import Config, Peer
from btmc.exceptions import MalformedMessage, PeerError
from btmc.protocol.messages import (
    Bitfield,
    BitfieldMessage,
    Choke,
    Handshake,
    Have,
    Interested,
    KeepAlive,
    NotInterested,
    PeerMessage,
    Piece,
    Request,
    Unchoke,
    Unknown,
)
from btmc.protocol.peer import CloseReason
from btmc.protocol.shared import Shared
from btmc.types import Addr, Block

from ..fixtures import MakePipe

ADDR = Addr.parse("10.0.0.1", 6881)
INFO_HASH = bytes(range(20))
OUR_ID = b"A" * 20
THEIR_ID = b"B" * 20
OUR_HANDSHAKE = Handshake(INFO_HASH, OUR_ID).encode()
THEIR_HANDSHAKE = Handshake(INFO_HASH, THEIR_ID).encode()
CONFIG = Config()


class Peers:
    """Makes the peers for a test, and closes them when it's over"""

    def __init__(self, make_pipe: MakePipe) -> None:
        self._make_pipe = make_pipe
        self._peers: list[Peer] = []

    def new(self, *, incoming: bool = False, config: Config = CONFIG) -> Peer:
        """
        A peer that won't be run, made at time 0. An incoming one is connected,
        and has had the remote's handshake. Otherwise, it isn't connected yet.
        """
        shared = Shared(config, OUR_ID)
        if incoming:
            connection, _ = self._make_pipe()
            handshake = Handshake(INFO_HASH, THEIR_ID)
            peer = Peer.from_incoming(ADDR, handshake, 10, shared, 0, connection)
        else:
            peer = Peer(ADDR, INFO_HASH, 10, shared, 0)
        self._peers.append(peer)
        return peer

    def linked(
        self, *, config: Config = CONFIG, incoming: bool = False
    ) -> tuple[Peer, BufferedByteStream]:
        """
        A peer connected to a remote end in memory, just now.
        An incoming one has had the remote's handshake.
        """
        connection, remote = self._make_pipe()
        shared = Shared(config, OUR_ID)
        now = anyio.current_time()
        if incoming:
            handshake = Handshake(INFO_HASH, THEIR_ID)
            peer = Peer.from_incoming(ADDR, handshake, 10, shared, now, connection)
        else:
            peer = Peer(ADDR, INFO_HASH, 10, shared, now, connection)
        self._peers.append(peer)
        return peer, remote

    def close(self) -> None:
        for peer in self._peers:
            peer.close(CloseReason.STOPPING)


@pytest.fixture
def peers(make_pipe: MakePipe) -> Iterator[Peers]:
    made = Peers(make_pipe)
    yield made
    made.close()


@contextlib.asynccontextmanager
async def running(peer: Peer) -> AsyncGenerator[None]:
    """Run the peer for the body of the block, then close it"""
    with anyio.fail_after(1):
        async with anyio.create_task_group() as tg:
            tg.start_soon(peer.run)
            yield
            peer.close(CloseReason.STOPPING)


# ---- the connection ----


async def test_run(peers: Peers):
    """
    What arrives is handed over, what's sent goes out after our handshake,
    the connection holds a place in the connection limit,
    and the remote end closing ends the connection
    """
    peer, remote = peers.linked()
    received: list[PeerMessage] = []
    peer.add_callback("message", lambda _, msg: received.append(msg))
    async with running(peer):
        # handshaking
        assert await remote.receive_exactly(Handshake.LENGTH) == OUR_HANDSHAKE
        assert peer.shared.connections.borrowed_tokens == 1
        await remote.send(THEIR_HANDSHAKE + Unchoke().encode())
        await anyio.wait_all_tasks_blocked()

        # unchoking
        assert received == [Unchoke()]
        peer.send(Interested())
        interested = Interested().encode()
        assert await remote.receive_exactly(len(interested)) == interested

        # disconnect
        await remote.send_eof()
        await anyio.wait_all_tasks_blocked()
    assert peer.close_reason is CloseReason.CONNECTION_LOST
    assert isinstance(peer.close_error, anyio.IncompleteRead)
    assert peer.shared.connections.borrowed_tokens == 0


async def test_split_anywhere(peers: Peers):
    """Bytes arrive in whatever chunks the network gives us"""
    peer, remote = peers.linked()
    received: list[PeerMessage] = []
    peer.add_callback("message", lambda _, msg: received.append(msg))
    bits = BitfieldMessage(Bitfield(16, b"\xa0\x40"))
    data = THEIR_HANDSHAKE + bits.encode() + Unchoke().encode()
    async with running(peer):
        for i in range(0, len(data), 7):
            await remote.send(data[i : i + 7])
        await anyio.wait_all_tasks_blocked()
    assert received == [bits, Unchoke()]
    assert peer.remote_peer_id == THEIR_ID
    assert list(peer.has) == [0, 2, 9]
    assert not peer.choked


async def test_close(peers: Peers):
    """Closing the peer from outside ends the connection and closes the stream"""
    peer, remote = peers.linked()
    async with running(peer):
        await anyio.wait_all_tasks_blocked()
        peer.close(CloseReason.IDLE)
    assert peer.close_reason is CloseReason.IDLE
    await remote.receive_exactly(Handshake.LENGTH)
    with pytest.raises(anyio.EndOfStream):
        await remote.receive()


async def test_close_while_waiting_for_a_place(peers: Peers):
    """Closing a peer stops it running, even while it's waiting for a place in the limit"""
    peer = peers.new(config=Config(max_connections=1))
    with anyio.fail_after(1):
        async with peer.shared.connections, anyio.create_task_group() as tg:
            tg.start_soon(peer.run)
            await anyio.wait_all_tasks_blocked()
            assert peer.shared.connections.statistics().tasks_waiting == 1
            peer.close(CloseReason.IDLE)
    assert peer.close_reason is CloseReason.IDLE


async def test_closed_before_running(peers: Peers):
    """A peer closed before it runs doesn't take a place in the connection limit"""
    peer = peers.new()
    peer.close(CloseReason.STOPPING)
    with patch.object(peer.shared, "connections") as connections, anyio.fail_after(1):
        await peer.run()
    connections.__aenter__.assert_not_called()


@pytest.mark.parametrize(
    "data",
    [
        Handshake(bytes(20), THEIR_ID).encode(),
        Handshake(INFO_HASH, OUR_ID).encode(),
        b"\x13BitTorrent protocox" + THEIR_HANDSHAKE[20:],
    ],
    ids=["other info hash", "ourselves", "not bittorrent"],
)
@pytest.mark.validation
async def test_protocol_error_ends_run(peers: Peers, data: bytes):
    peer, remote = peers.linked()
    await remote.send(data)
    with anyio.fail_after(1):
        await peer.run()
    assert peer.close_reason is CloseReason.PROTOCOL_ERROR
    assert isinstance(peer.close_error, PeerError)


@pytest.mark.parametrize(
    "data",
    [
        (2).to_bytes(4) + b"\x00\x00",
        (4).to_bytes(4) + b"\x04\x00\x00\x00",
        # 10 pieces take 2 bytes
        (2).to_bytes(4) + b"\x05\x80",
        (4).to_bytes(4) + b"\x05\x80\x00\x00",
        # only the start of these is sent, since the rest is never read
        (1 + 8 + 16384 + 1).to_bytes(4) + b"\x07",
        b"\x7f\xff\xff\xff\x00",
    ],
    ids=[
        "choke with a body",
        "short have",
        "short bitfield",
        "long bitfield",
        "piece longer than a block",
        "huge choke",
    ],
)
@pytest.mark.validation
async def test_malformed_message_ends_run(peers: Peers, data: bytes):
    """A message of the wrong length for its kind closes the connection before its body is read"""
    peer, remote = peers.linked()
    await remote.send(THEIR_HANDSHAKE + data)
    with anyio.fail_after(1):
        await peer.run()
    assert peer.close_reason is CloseReason.MALFORMED_MESSAGE
    assert isinstance(peer.close_error, MalformedMessage)


@pytest.mark.limits
async def test_unknown_messages_are_skipped(peers: Peers):
    """
    Messages of extensions we don't speak are skipped however long they are,
    and don't count as the peer being alive
    """
    peer, remote = peers.linked()
    received: list[PeerMessage] = []
    peer.add_callback("message", lambda _, msg: received.append(msg))
    async with running(peer):
        await remote.send(THEIR_HANDSHAKE)
        await anyio.wait_all_tasks_blocked()
        handshaken_at = peer.received_at
        await anyio.sleep(0.01)
        await remote.send(Unknown(20, bytes(100_000)).encode())
        await anyio.wait_all_tasks_blocked()
        assert peer.received_at == handshaken_at
        await remote.send(Unchoke().encode())
        await anyio.wait_all_tasks_blocked()
    assert received == [Unchoke()]


async def test_incoming(peers: Peers):
    """A peer that connected to us has sent its handshake already, so messages come next"""
    peer, remote = peers.linked(incoming=True)
    received: list[PeerMessage] = []
    peer.add_callback("message", lambda _, msg: received.append(msg))
    assert peer.remote_peer_id == THEIR_ID
    async with running(peer):
        assert await remote.receive_exactly(Handshake.LENGTH) == OUR_HANDSHAKE
        await remote.send(Unchoke().encode())
        await anyio.wait_all_tasks_blocked()
    assert received == [Unchoke()]


async def test_connection_events(peers: Peers):
    """A peer says when both handshakes are done, and when that connection ends"""
    peer, remote = peers.linked()
    events: list[str] = []
    peer.add_callback("connected", lambda _: events.append("connected"))
    peer.add_callback("disconnected", lambda _: events.append("disconnected"))
    async with running(peer):
        await remote.receive_exactly(Handshake.LENGTH)
        await anyio.wait_all_tasks_blocked()
        assert events == []
        await remote.send(THEIR_HANDSHAKE)
        await anyio.wait_all_tasks_blocked()
        assert events == ["connected"]
    assert events == ["connected", "disconnected"]


async def test_disconnected_says_why(peers: Peers):
    """By the time a peer says it's disconnected, it knows why"""
    peer, remote = peers.linked(incoming=True)
    reasons: list[CloseReason | None] = []
    peer.add_callback("disconnected", lambda _: reasons.append(peer.close_reason))
    with anyio.fail_after(1):
        async with anyio.create_task_group() as tg:
            tg.start_soon(peer.run)
            await remote.receive_exactly(Handshake.LENGTH)
            await remote.send_eof()
    assert reasons == [CloseReason.CONNECTION_LOST]


async def test_no_connection_events_without_a_handshake(peers: Peers):
    peer, remote = peers.linked()
    events: list[str] = []
    peer.add_callback("connected", lambda _: events.append("connected"))
    peer.add_callback("disconnected", lambda _: events.append("disconnected"))
    await remote.send(Handshake(bytes(20), THEIR_ID).encode())
    with anyio.fail_after(1):
        await peer.run()
    assert events == []


@pytest.mark.validation
def test_incoming_from_ourselves(make_pipe: MakePipe):
    connection, _ = make_pipe()
    handshake = Handshake(INFO_HASH, OUR_ID)
    with pytest.raises(PeerError):
        Peer.from_incoming(ADDR, handshake, 10, Shared(CONFIG, OUR_ID), 0, connection)


async def test_keepalive(peers: Peers):
    """Keepalives are sent when there's been nothing else to send for a while"""
    peer, remote = peers.linked(config=Config(keepalive_interval=0.01))
    async with running(peer):
        await remote.receive_exactly(Handshake.LENGTH)
        assert await remote.receive_exactly(4) == KeepAlive().encode()


@pytest.mark.limits
async def test_times_out_on_its_own(peers: Peers):
    """A running peer closes itself when the peer goes quiet for too long"""
    config = Config(connect_timeout=0, handshake_timeout=0.05, timeout_check_interval=0.01)
    peer, _ = peers.linked(config=config)
    with anyio.fail_after(1):
        await peer.run()
    assert peer.close_reason is CloseReason.NO_HANDSHAKE


# ---- protocol ----


async def test_sending(peers: Peers):
    """Our handshake goes first, then what we send, in the order it was sent, in one write"""
    peer, remote = peers.linked()
    peer.send(Interested())
    peer.send(Request(1, 0, 3))
    assert peer.interested
    assert peer.requests == {Block(1, 0, 3)}
    async with running(peer):
        expected = OUR_HANDSHAKE + Interested().encode() + Request(1, 0, 3).encode()
        assert await remote.receive() == expected


async def test_interest_is_only_sent_when_it_changes(peers: Peers):
    peer, remote = peers.linked()
    for interested in (True, True, False, False, True):
        peer.set_interested(interested)
    async with running(peer):
        interest = Interested().encode() + NotInterested().encode() + Interested().encode()
        assert await remote.receive() == OUR_HANDSHAKE + interest


async def test_nothing_sent_after_closing(peers: Peers):
    peer, remote = peers.linked()
    async with running(peer):
        await remote.receive_exactly(Handshake.LENGTH)
        peer.close(CloseReason.IDLE)
        peer.send(Interested())
    with pytest.raises(anyio.EndOfStream):
        await remote.receive()


def test_choking(peers: Peers):
    peer = peers.new(incoming=True)
    assert peer.choked
    peer._receive(Unchoke(), 0)
    assert not peer.choked
    peer._receive(Choke(), 0)
    assert peer.choked


def test_have(peers: Peers):
    """Repeated haves change nothing, so they aren't returned"""
    peer = peers.new(incoming=True)
    assert peer._receive(Have(3), 0) == Have(3)
    assert peer._receive(Have(3), 0) is None
    assert list(peer.has) == [3]
    with pytest.raises(PeerError):
        peer._receive(Have(10), 0)


def test_later_bitfield_replaces_what_the_peer_has(peers: Peers):
    peer = peers.new(incoming=True)
    peer._receive(Have(3), 0)
    bits = BitfieldMessage(Bitfield(16, b"\x80\x00"))
    assert peer._receive(bits, 0) == bits
    assert list(peer.has) == [0]


@pytest.mark.validation
def test_requests(peers: Peers):
    """
    Blocks we asked for are returned once, and anything else is dropped,
    including blocks of a different length from what we asked for
    """
    peer = peers.new(incoming=True)
    peer.send(Request(1, 0, 3))
    peer.send(Request(1, 16384, 3))
    assert peer._receive(Piece(1, 0, b"abc"), 0) == Piece(1, 0, b"abc")
    assert peer._receive(Piece(1, 0, b"abc"), 0) is None
    assert peer._receive(Piece(2, 0, b"x"), 0) is None
    assert peer._receive(Piece(1, 16384, b"ab"), 0) is None
    assert peer.received == 3
    assert peer.requests == {Block(1, 16384, 3)}


def test_choke_releases_requests(peers: Peers):
    """A peer throws away our requests when it chokes us, so they're handed back, in order"""
    peer = peers.new(incoming=True)
    calls: list[list[Block]] = []
    peer.add_callback("released", lambda _, blocks: calls.append(blocks))
    peer._receive(Unchoke(), 0)
    peer.send(Request(2, 0, 3))
    peer.send(Request(1, 0, 3))
    peer._receive(Choke(), 0)
    assert calls == [[Block(1, 0, 3), Block(2, 0, 3)]]
    assert peer.requests == set()
    # nothing more to hand back after that
    peer._receive(Choke(), 0)
    assert len(calls) == 1


def test_close_releases_requests(peers: Peers):
    peer = peers.new(incoming=True)
    calls: list[list[Block]] = []
    peer.add_callback("released", lambda _, blocks: calls.append(blocks))
    peer.send(Request(1, 0, 3))
    peer.close(CloseReason.IDLE)
    assert calls == [[Block(1, 0, 3)]]


# ---- timeouts ----


@pytest.mark.limits
def test_no_handshake(peers: Peers):
    peer = peers.new()
    limit = CONFIG.connect_timeout + CONFIG.handshake_timeout
    peer._check_timeouts(limit)
    assert not peer.closed
    peer._check_timeouts(limit + 1)
    assert peer.close_reason is CloseReason.NO_HANDSHAKE


@pytest.mark.limits
def test_requests_time_out(peers: Peers):
    """Pending requests time out when no block arrives, even if the peer sends other things"""
    peer = peers.new(incoming=True)
    peer._check_timeouts(0)
    peer.send(Request(1, 0, 3))
    peer._receive(Have(2), CONFIG.request_timeout)
    peer._check_timeouts(CONFIG.request_timeout)
    assert not peer.closed
    peer._check_timeouts(CONFIG.request_timeout + 1)
    assert peer.close_reason is CloseReason.REQUESTS_TIMED_OUT


@pytest.mark.limits
def test_idle(peers: Peers):
    peer = peers.new(incoming=True)
    peer._check_timeouts(CONFIG.peer_idle_timeout)
    assert not peer.closed
    peer._check_timeouts(CONFIG.peer_idle_timeout + 1)
    assert peer.close_reason is CloseReason.IDLE
