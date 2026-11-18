"""
The swarm, running, with remote peers connected over in-memory connections,
and its decisions about connecting and announcing
"""

import contextlib
import itertools
import os
import socket
from collections.abc import AsyncGenerator, Awaitable, Callable, Sequence
from ipaddress import ip_network
from unittest.mock import AsyncMock, patch

import anyio
import pytest
from anyio.streams.buffered import BufferedByteStream
from torrent_models import Torrent

from btmc import Config, Peer
from btmc.protocol.messages import (
    AnnounceResponse,
    Bitfield,
    BitfieldMessage,
    Choke,
    Handshake,
    Interested,
    PeerMessage,
    Piece,
    Request,
    Unchoke,
)
from btmc.protocol.peer import CloseReason
from btmc.protocol.picker import PiecePicker
from btmc.protocol.shared import BanReason, Shared
from btmc.protocol.swarm import Swarm
from btmc.types import Addr, AnyAddr, Block, DomainAddr

from ..fixtures import MakePipe

OUR_ID = b"A" * 20
B = 16384
"""Block size. The test torrents' pieces are two blocks."""


type MakeSwarm = Callable[..., Swarm]
type Connect = Callable[..., Awaitable[tuple[Peer, BufferedByteStream]]]


@pytest.fixture
def make_swarm() -> MakeSwarm:
    def make(
        torrent: Torrent,
        shared: Shared | None = None,
        trackers: list[list[str]] | None = None,
        known_peers: Sequence[AnyAddr] = (),
    ) -> Swarm:
        shared = shared or Shared(Config(), OUR_ID)
        picker = PiecePicker(torrent.layout, shared.config.max_pieces_checking)
        return Swarm(torrent, picker, shared, trackers=trackers or [], known_peers=known_peers)

    return make


@pytest.fixture
def names(monkeypatch: pytest.MonkeyPatch) -> dict[str, str]:
    """Host names that resolve, to the IP each points to. Other names don't resolve."""
    names: dict[str, str] = {}

    async def resolve(self: DomainAddr) -> Addr:
        if self.name not in names:
            raise socket.gaierror(socket.EAI_NONAME, "not found")
        return Addr.parse(names[self.name], self.port)

    monkeypatch.setattr(DomainAddr, "resolve", resolve)
    return names


@pytest.fixture
def connect(make_pipe: MakePipe) -> Connect:
    """
    Connects a remote peer to a running swarm, which sends its bitfield:
    every piece, or just ``has``. Returns our peer, and the remote's end of the connection,
    once it has read our handshake.
    """

    async def connect(
        swarm: Swarm, port: int, has: list[int] | None = None
    ) -> tuple[Peer, BufferedByteStream]:
        connection, remote = make_pipe()
        addr = Addr.parse("10.0.0.1", port)
        handshake = Handshake(swarm.info_hashes[0], bytes([port]) * 20)
        assert swarm.accept_connection(connection, addr, handshake)
        await remote.receive_exactly(Handshake.LENGTH)
        if has is None:
            bits = Bitfield.full(swarm.n_pieces)
        else:
            bits = Bitfield.from_pieces(swarm.n_pieces, has)
        await remote.send(BitfieldMessage(bits).encode())
        await anyio.wait_all_tasks_blocked()
        return swarm.peers[addr], remote

    return connect


@contextlib.asynccontextmanager
async def running(swarm: Swarm) -> AsyncGenerator[None]:
    """Run the swarm for the body of the block, then stop it"""
    with anyio.fail_after(5):
        async with anyio.create_task_group() as tg:
            tg.start_soon(swarm.run)
            await anyio.wait_all_tasks_blocked()
            yield
            await swarm.stop()


async def assert_sent(remote: BufferedByteStream, *messages: PeerMessage) -> None:
    """The next thing the remote reads is these messages"""
    expected = b"".join(msg.encode() for msg in messages)
    assert await remote.receive_exactly(len(expected)) == expected


async def test_callbacks(torrent, make_swarm: MakeSwarm, connect: Connect):
    """Callbacks are added and removed per event, and there's no adding one for an unknown event"""
    swarm = make_swarm(torrent)
    swarm.picker.want([5])
    connected: list[Peer] = []
    swarm.add_callback("connected", connected.append)
    async with running(swarm):
        peer, _ = await connect(swarm, 1)
        swarm.remove_callback("connected", connected.append)
        await connect(swarm, 2)
    assert connected == [peer]
    with pytest.raises(ValueError):
        swarm.add_callback("nope", connected.append)  # type: ignore[call-overload]


async def test_disconnected_once_the_peer_is_gone(torrent, make_swarm: MakeSwarm, connect: Connect):
    """When a connected peer leaves, we hear about it once the swarm has let it go"""
    swarm = make_swarm(torrent)
    swarm.picker.want([5])
    in_swarm: list[bool] = []
    gone = anyio.Event()

    def disconnected(peer: Peer) -> None:
        in_swarm.append(peer.addr in swarm.peers)
        gone.set()

    swarm.add_callback("disconnected", disconnected)
    async with running(swarm):
        peer, remote = await connect(swarm, 1)
        await remote.aclose()
        await gone.wait()
    assert peer.addr not in swarm.peers


async def test_requests_in_the_order_pieces_are_wanted(
    torrent, make_swarm: MakeSwarm, connect: Connect
):
    swarm = make_swarm(torrent)
    swarm.picker.want([5, 6, 1, 2])
    async with running(swarm):
        _, remote = await connect(swarm, 1)
        await assert_sent(remote, Interested())
        await remote.send(Unchoke().encode())
        await assert_sent(
            remote,
            Request(5, 0, B),
            Request(5, B, B),
            Request(6, 0, B),
            Request(6, B, B),
            Request(1, 0, B),
            Request(1, B, B),
            Request(2, 0, B),
            Request(2, B, B),
        )


async def test_only_what_the_peer_has(torrent, make_swarm: MakeSwarm, connect: Connect):
    swarm = make_swarm(torrent)
    swarm.picker.want([5, 6])
    async with running(swarm):
        _, remote = await connect(swarm, 1, has=[6])
        await remote.send(Unchoke().encode())
        await assert_sent(remote, Interested(), Request(6, 0, B), Request(6, B, B))


async def test_only_peers_with_new_pieces_are_told(
    torrent, make_swarm: MakeSwarm, connect: Connect
):
    swarm = make_swarm(torrent)
    swarm.picker.want([5])
    async with running(swarm):
        peer, remote = await connect(swarm, 1, has=[6])
        await remote.send(Unchoke().encode())
        await anyio.wait_all_tasks_blocked()
        assert not peer.interested
        swarm.picker.want([6])
        await assert_sent(remote, Interested(), Request(6, 0, B), Request(6, B, B))


async def test_choked_requests_go_to_the_next_peer(
    torrent, make_swarm: MakeSwarm, connect: Connect
):
    swarm = make_swarm(torrent)
    swarm.picker.want([5])
    async with running(swarm):
        _, remote_a = await connect(swarm, 1)
        b, remote_b = await connect(swarm, 2)
        await remote_a.send(Unchoke().encode())
        await assert_sent(remote_a, Interested(), Request(5, 0, B), Request(5, B, B))
        await remote_b.send(Unchoke().encode())
        await assert_sent(remote_b, Interested())
        await anyio.wait_all_tasks_blocked()
        assert not b.requests
        await remote_a.send(Choke().encode())
        await assert_sent(remote_b, Request(5, 0, B), Request(5, B, B))


async def test_blocks_are_handed_on(torrent, make_swarm: MakeSwarm, connect: Connect):
    """Each block that arrives goes to the callbacks, with who sent it"""
    swarm = make_swarm(torrent)
    blocks: list[tuple[Block, bytes, Addr]] = []
    swarm.add_callback("block", lambda *block: blocks.append(block))
    swarm.picker.want([5])
    async with running(swarm):
        peer, remote = await connect(swarm, 1)
        await remote.send(Unchoke().encode())
        await assert_sent(remote, Interested(), Request(5, 0, B), Request(5, B, B))
        await remote.send(Piece(5, B, b"b" * B).encode() + Piece(5, 0, b"a" * B).encode())
        await anyio.wait_all_tasks_blocked()
    assert blocks == [
        (Block(5, B, B), b"b" * B, peer.addr),
        (Block(5, 0, B), b"a" * B, peer.addr),
    ]


async def test_failed_piece_is_asked_for_again(torrent, make_swarm: MakeSwarm, connect: Connect):
    swarm = make_swarm(torrent)
    swarm.picker.want([5])
    async with running(swarm):
        _, remote = await connect(swarm, 1)
        await remote.send(Unchoke().encode())
        await assert_sent(remote, Interested(), Request(5, 0, B), Request(5, B, B))
        await remote.send(Piece(5, 0, bytes(B)).encode() + Piece(5, B, bytes(B)).encode())
        await anyio.wait_all_tasks_blocked()
        # what the torrent does with the blocks
        for begin in (0, B):
            swarm.picker.received(Block(5, begin, B))
        swarm.picker.failed(5)
        await assert_sent(remote, Request(5, 0, B), Request(5, B, B))


async def test_peers_are_asked_again_once_a_piece_makes_room(
    torrent, make_swarm: MakeSwarm, connect: Connect
):
    """
    While the most pieces allowed are waiting for their check, no new piece is asked for,
    and one passing gets peers asked again
    """
    swarm = make_swarm(torrent, Shared(Config(max_pieces_checking=1), OUR_ID))
    swarm.picker.want([5])
    async with running(swarm):
        peer, remote = await connect(swarm, 1)
        await remote.send(Unchoke().encode())
        await assert_sent(remote, Interested(), Request(5, 0, B), Request(5, B, B))
        await remote.send(Piece(5, 0, bytes(B)).encode() + Piece(5, B, bytes(B)).encode())
        await anyio.wait_all_tasks_blocked()
        # what the torrent does with the blocks
        for begin in (0, B):
            swarm.picker.received(Block(5, begin, B))
        swarm.picker.want([6])
        assert not peer.requests
        swarm.picker.passed(5)
        await assert_sent(remote, Request(6, 0, B), Request(6, B, B))


async def test_banned_peers(torrents, make_swarm: MakeSwarm, connect: Connect):
    """
    A peer banned by any torrent is closed right away and leaves the swarm,
    and no swarm sharing the ban list connects to it again
    """
    shared = Shared(Config(), OUR_ID)
    swarms = [make_swarm(torrent, shared) for torrent in torrents.values()]
    swarms[0].picker.want([5])
    gone = anyio.Event()
    swarms[0].add_callback("disconnected", lambda _: gone.set())
    async with running(swarms[0]):
        peer, _ = await connect(swarms[0], 1)
        for swarm in swarms:
            swarm.add_candidate(peer.addr, swarm.info_hashes[0])
        shared.ban(peer.addr, BanReason.BAD_DATA)
        assert peer.close_reason is CloseReason.BANNED
        await gone.wait()
        assert peer.addr not in swarms[0].peers
    for swarm in swarms:
        assert peer.addr not in swarm._candidates
        swarm.add_candidate(peer.addr, swarm.info_hashes[0])
        assert peer.addr not in swarm._candidates


async def test_malformed_messages_get_a_ban(torrent, make_swarm: MakeSwarm, connect: Connect):
    swarm = make_swarm(torrent)
    swarm.picker.want([5])
    gone = anyio.Event()
    swarm.add_callback("disconnected", lambda _: gone.set())
    async with running(swarm):
        peer, remote = await connect(swarm, 1)
        # a choke with a body
        await remote.send((2).to_bytes(4) + b"\x00\x00")
        await gone.wait()
    assert peer.close_reason is CloseReason.MALFORMED_MESSAGE
    assert swarm.shared.is_banned(peer.addr)


def test_trackers_we_cant_announce_to_are_skipped(torrent, make_swarm: MakeSwarm):
    trackers = [
        ["udp://tracker.example/announce", "http://tracker.example/announce"],
        ["ftp://tracker.example/announce", "http://tracker.example/announce"],
    ]
    swarm = make_swarm(torrent, trackers=trackers)
    assert swarm.trackers == ["http://tracker.example/announce"]


async def test_should_connect(torrent, make_swarm: MakeSwarm, connect: Connect):
    """
    We connect with a peer, whichever side starts it, only while running and wanting pieces,
    with room for another connection, and if we aren't connected to it already or banned it
    """
    swarm = make_swarm(torrent, Shared(Config(max_peers=2), OUR_ID))
    new = Addr.parse("10.0.0.9", 1)
    swarm.picker.want([0])
    gone = anyio.Event()
    swarm.add_callback("disconnected", lambda _: gone.set())
    assert not swarm._should_connect(new)
    async with running(swarm):
        assert swarm._should_connect(new)
        peer, _ = await connect(swarm, 1)
        assert not swarm._should_connect(peer.addr)
        await connect(swarm, 2)
        assert not swarm._should_connect(new)
        swarm.remove_peer(peer, CloseReason.STOPPING)
        await gone.wait()
        assert swarm._should_connect(new)
        swarm.shared.ban(new, BanReason.BAD_DATA)
        assert not swarm._should_connect(new)
        swarm.picker.found([0])
        assert not swarm._should_connect(Addr.parse("10.0.0.10", 1))


@pytest.mark.parametrize("version", ["hybrid"], indirect=True)
async def test_ready_candidates(torrent, make_swarm: MakeSwarm):
    """
    Peers in both of a hybrid's swarms get the v2 hash first,
    and after a failed connection, the v1 hash, once they've waited out the backoff
    """
    swarm = make_swarm(torrent)
    v2, v1 = swarm.info_hashes
    addr = Addr.parse("10.0.0.9", 1)
    swarm.add_candidate(addr, v1)
    swarm.add_candidate(addr, v2)
    assert swarm._ready_candidates(now=0) == [(addr, v2)]
    peer = Peer(addr, v2, swarm.n_pieces, swarm.shared, 0)
    swarm.peers[addr] = peer
    peer.close(CloseReason.CONNECT_FAILED)
    swarm._peer_ended(peer, now=0)
    assert swarm._ready_candidates(now=29) == []
    assert swarm._ready_candidates(now=30) == [(addr, v1)]


async def test_announces_while_anything_is_wanted(torrent, tracker, make_swarm: MakeSwarm):
    """
    Nothing is announced until something is wanted. Then we say we started,
    once nothing is wanted we say we stopped, and we start again as soon as something is.
    """
    peer_id = os.urandom(20)
    tracker.interval = 0
    shared = Shared(Config(min_announce_interval=0.05), peer_id)
    swarm = make_swarm(torrent, shared, trackers=[[tracker.urls.http]])
    info_hash = swarm.info_hashes[0]

    async with anyio.create_task_group() as tg:
        tg.start_soon(swarm._announce_loop, tracker.urls.http, info_hash)
        await anyio.wait_all_tasks_blocked()
        assert not tracker.swarms_of(peer_id)
        swarm.picker.want([5])
        assert await tracker.next_event(peer_id) == (info_hash, "started")
        swarm.picker.found([5])
        assert await tracker.next_event(peer_id) == (info_hash, "stopped")
        swarm.picker.want([6])
        assert await tracker.next_event(peer_id) == (info_hash, "started")
        tg.cancel_scope.cancel()


async def test_only_trackers_that_dont_answer_are_backed_off(
    torrent, tracker, make_swarm: MakeSwarm
):
    """
    A tracker that doesn't answer is backed off, and announced to again once that's over.
    One that refuses a torrent is up, so it isn't backed off, and we try again later.
    """
    swarm = make_swarm(torrent)
    config = swarm.config
    info_hash = swarm.info_hashes[0]
    down = "http://127.0.0.1:1/announce"
    before = anyio.current_time()
    at = await swarm._announce(down, info_hash)
    assert at == swarm.shared.tracker_ready_at(down)
    assert before + config.tracker_backoff <= at <= anyio.current_time() + config.tracker_backoff

    tracker.refuse.add(info_hash)
    before = anyio.current_time()
    at = await swarm._announce(tracker.urls.http, info_hash)
    assert before + config.announce_retry <= at <= anyio.current_time() + config.announce_retry
    assert swarm.shared.tracker_ready_at(tracker.urls.http) == 0
    assert not swarm.announced


async def test_backoff_grows_while_a_tracker_doesnt_answer(torrent, tracker, make_swarm: MakeSwarm):
    """Each announce to a tracker that doesn't answer waits twice as long as the last"""
    peer_id = os.urandom(20)
    config = Config(announce_timeout=0.05, tracker_backoff=0.1)
    swarm = make_swarm(torrent, Shared(config, peer_id), trackers=[[tracker.urls.http]])
    info_hash = swarm.info_hashes[0]
    tracker.unresponsive = True
    swarm.picker.want([5])
    announced_at = []
    async with anyio.create_task_group() as tg:
        tg.start_soon(swarm._announce_loop, tracker.urls.http, info_hash)
        for _ in range(4):
            assert await tracker.next_event(peer_id) == (info_hash, "started")
            announced_at.append(anyio.current_time())
        tg.cancel_scope.cancel()
    gaps = [later - earlier for earlier, later in itertools.pairwise(announced_at)]
    assert all(gap >= 0.1 * 2**i for i, gap in enumerate(gaps))


@pytest.mark.parametrize(
    ("asked", "waited"),
    [
        (7200, 7200),
        (10, 60),
        (-1, 60),
        # far past what fits in a float, so it can't be slept for as it is
        (10**400, 86400),
    ],
    ids=["asked", "too short", "negative", "too long"],
)
async def test_trackers_interval_wins(
    torrent, tracker, make_swarm: MakeSwarm, asked: int, waited: float
):
    """We wait as long as a tracker asks between announces, within our minimum and maximum"""
    config = Config(min_announce_interval=60, max_announce_interval=86400)
    swarm = make_swarm(torrent, Shared(config, os.urandom(20)))
    tracker.interval = asked
    before = anyio.current_time()
    at = await swarm._announce(tracker.urls.http, swarm.info_hashes[0])
    assert before + waited <= at <= anyio.current_time() + waited


def test_blocked_candidates_are_skipped(torrent, make_swarm: MakeSwarm):
    """The configured blocklist replaces the defaults, which block addresses that can't be peers"""
    swarm = make_swarm(torrent)
    for blocked in ["0.0.0.0", "224.0.0.1", "255.255.255.255", "::", "fe80::1", "ff02::1"]:
        swarm.add_candidate(Addr.parse(blocked, 80), swarm.info_hashes[0])
    assert not swarm._candidates

    swarm = make_swarm(torrent, Shared(Config(ip_blocklist=[ip_network("10.0.0.0/8")]), OUR_ID))
    swarm.add_candidate(Addr.parse("10.0.0.1", 80), swarm.info_hashes[0])
    swarm.add_candidate(Addr.parse("0.0.0.0", 80), swarm.info_hashes[0])
    assert set(swarm._candidates) == {Addr.parse("0.0.0.0", 80)}


async def test_tracker_host_names_are_looked_up(torrent, make_swarm: MakeSwarm, names):
    """Peers a tracker gives as host names become candidates if they resolve to an allowed IP"""
    names.update({"peer.example": "10.0.0.1", "blocked.example": "0.0.0.0"})
    swarm = make_swarm(torrent)
    peers: list[AnyAddr] = [
        Addr.parse("10.0.0.2", 80),
        DomainAddr("peer.example", 80),
        DomainAddr("blocked.example", 80),
        DomainAddr("nowhere.example", 80),
    ]
    response = AsyncMock(return_value=AnnounceResponse(60, peers))
    with patch("btmc.protocol.swarm.announce", response):
        await swarm._announce("http://tracker.example/announce", swarm.info_hashes[0])
    assert set(swarm._candidates) == {Addr.parse("10.0.0.2", 80), Addr.parse("10.0.0.1", 80)}


async def test_known_host_names_are_looked_up(torrent, make_swarm: MakeSwarm, names):
    """Known peers become candidates when the swarm runs, for every info hash"""
    names["peer.example"] = "10.0.0.1"
    known = [Addr.parse("10.0.0.2", 80), DomainAddr("peer.example", 81)]
    swarm = make_swarm(torrent, known_peers=known)
    assert not swarm._candidates
    async with running(swarm):
        candidates = {addr: c.info_hashes for addr, c in swarm._candidates.items()}
    assert candidates == {
        Addr.parse("10.0.0.2", 80): swarm.info_hashes,
        Addr.parse("10.0.0.1", 81): swarm.info_hashes,
    }


async def test_stopped_announces_wait_their_turn(torrent, tracker, make_swarm: MakeSwarm):
    """Saying we stopped takes a place in the limit on announces, like any other announce"""
    peer_id = os.urandom(20)
    swarm = make_swarm(torrent, Shared(Config(announce_concurrency=1), peer_id))
    info_hash = swarm.info_hashes[0]

    async with anyio.create_task_group() as tg:
        async with swarm.shared.announces:
            tg.start_soon(swarm._announce_stopped, tracker.urls.http, info_hash)
            await anyio.wait_all_tasks_blocked()
            assert tracker.events[peer_id].empty()
        assert await tracker.next_event(peer_id) == (info_hash, "stopped")
