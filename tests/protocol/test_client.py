import asyncio
import json
from collections import Counter, defaultdict
from unittest.mock import Mock

import anyio
import pytest

from btmc import Client, Config, TorrentState
from btmc.protocol.shared import BanReason
from btmc.types import Addr
from btmc.utils import info_hashes

from ..fixtures import Stuck

pytestmark = pytest.mark.libtorrent


def senders_by_piece(block: Mock) -> dict[int, set[Addr]]:
    """Who sent the blocks of each piece, from a swarm's ``"block"`` callback"""
    senders = defaultdict(set)
    for (b, _, addr), _ in block.call_args_list:
        senders[b.piece].add(addr)
    return senders


async def test_http_iterative(client, torrent, seeders, tracker):
    """
    Fetching in rounds reuses the same connections,
    and spreads the work over both seeders
    """
    t = client.add(torrent, trackers=[[tracker.urls.http]])
    connected, block = Mock(), Mock()
    t.swarm.add_callback("connected", connected)
    t.swarm.add_callback("block", block)
    for batch in ([0], [t.n_pieces - 1], None):
        await client.fetch(t, batch)
        assert connected.call_count == 2
    received = Counter()
    for (_, data, addr), _ in block.call_args_list:
        received[addr] += len(data)
    assert set(received) == {s.addr for s in seeders}
    assert all(received.values())
    assert torrent.check(t.store.root)


async def test_udp(client, torrent, seeders, tracker):
    t = client.add(torrent, trackers=[[tracker.urls.udp]])
    await client.fetch(t)
    assert torrent.check(t.store.root)


async def test_resume(client, config, seeded, torrent):
    """
    A second client using the same directories already has everything,
    from the resume file, without checking the data
    """
    await client.fetch(seeded)
    async with Client(config=config) as second:
        t = second.add(torrent, trackers=[])
        assert t._disk_check is None
        await second.fetch(t)
    assert t.downloaded == 0


@pytest.mark.parametrize("version", ["hybrid"], indirect=True)
@pytest.mark.parametrize("swarm", ["v1", "v2"])
async def test_hybrid_single_version_peer(client, torrent, tracker, fake_seeder, swarm):
    """
    We can fetch a hybrid torrent from a peer that only knows one of its info hashes,
    and is only in that swarm, like a v1-only or v2-only client would be
    """
    v2_hash, v1_hash = info_hashes(torrent)
    info_hash = {"v1": v1_hash, "v2": v2_hash}[swarm]
    fake = await fake_seeder(torrent, accept={info_hash})
    tracker.only = {v1_hash: set(), v2_hash: set()}
    tracker.only[info_hash] = {fake.port}
    tracker.announce(info_hash, fake.peer_id, "127.0.0.1", fake.port, "started")

    t = client.add(torrent, trackers=[[tracker.urls.http]])
    await client.fetch(t)
    assert torrent.check(t.store.root)
    assert fake.refused == 0


@pytest.mark.parametrize("config", [{"pipeline_depth": 1}], indirect=True)
@pytest.mark.parametrize("version", ["hybrid"], indirect=True)
async def test_hybrid_mixed(client, torrent, seeders, tracker, fake_seeder):
    """
    libtorrent seeders in both of a hybrid's swarms, and a v1-only peer in the v1 swarm:
    peers in both swarms get the v2 info hash, and the v1-only peer gets the v1 one.
    Asking each peer for one block at a time, pieces are made of blocks from both swarms.
    """
    v2_hash, v1_hash = info_hashes(torrent)
    fake = await fake_seeder(torrent, accept={v1_hash})
    tracker.announce(v1_hash, fake.peer_id, "127.0.0.1", fake.port, "started")

    t = client.add(torrent, trackers=[[tracker.urls.http]])
    block = Mock()
    t.swarm.add_callback("block", block)
    await client.fetch(t)
    used = {addr: peer.info_hash for addr, peer in t.swarm.peers.items()}
    assert torrent.check(t.store.root)
    assert fake.refused == 0
    assert used == {seeders[0].addr: v2_hash, seeders[1].addr: v2_hash, fake.addr: v1_hash}
    lt_addrs = {s.addr for s in seeders}
    assert any(fake.addr in s and s & lt_addrs for s in senders_by_piece(block).values())


@pytest.mark.parametrize("config", [{"pipeline_depth": 1}], indirect=True)
async def test_piece_from_two_peers(client, seeded, torrent, seeders):
    """Asking each peer for one block at a time, pieces are made of blocks from both seeders"""
    block = Mock()
    seeded.swarm.add_callback("block", block)
    await client.fetch(seeded)
    assert torrent.check(seeded.store.root)
    assert {s.addr for s in seeders} in senders_by_piece(block).values()


@pytest.mark.bans
async def test_corrupt_peer(client, torrent, seeders, fake_seeder):
    """A peer sending bad data gets banned, and the peer sending good data doesn't"""
    fake = await fake_seeder(torrent, accept=set(info_hashes(torrent)), corrupt=True)
    good = seeders[0].addr
    t = client.add(torrent, trackers=[], known_peers=[fake.addr, good])
    await client.fetch(t)
    assert torrent.check(t.store.root)
    assert t.hash_failures
    assert t.shared.is_banned(fake.addr)
    assert not t.shared.is_banned(good)


@pytest.mark.parametrize("config", [{"pipeline_depth": 1}], indirect=True)
@pytest.mark.bans
async def test_corrupt_peer_in_shared_pieces(client, torrent, seeders, fake_seeder):
    """
    When pieces mix blocks from a peer sending bad data and a peer sending good data,
    they're fetched again until they pass, and only the bad peer is banned
    """
    fake = await fake_seeder(torrent, accept=set(info_hashes(torrent)), corrupt=True)
    good = seeders[0].addr
    t = client.add(torrent, trackers=[], known_peers=[fake.addr, good])
    block = Mock()
    t.swarm.add_callback("block", block)
    async with asyncio.timeout(30):
        await client.fetch(t)
    assert torrent.check(t.store.root)
    assert {fake.addr, good} in senders_by_piece(block).values()
    assert t.shared.is_banned(fake.addr)
    assert not t.shared.is_banned(good)


async def test_peer_drops_partway(client, torrent, seeders, fake_seeder):
    """
    A peer that closes the connection with requests unanswered isn't banned,
    and what it was asked for is fetched from another peer
    """
    fake = await fake_seeder(torrent, accept=set(info_hashes(torrent)), drop_after=1)
    good = seeders[0].addr
    t = client.add(torrent, trackers=[], known_peers=[fake.addr, good])
    block = Mock()
    t.swarm.add_callback("block", block)
    async with asyncio.timeout(30):
        await client.fetch(t)
    assert torrent.check(t.store.root)
    assert any(addr == fake.addr for (_, _, addr), _ in block.call_args_list)
    assert not t.shared.is_banned(fake.addr)


@pytest.mark.parametrize("config", [{"listen_host": "127.0.0.1", "port": 0}], indirect=True)
async def test_incoming(client, torrent, version, one_torrent, seeders):
    """With no trackers or known peers, we get the data from a peer that connects to us"""
    connected = Mock()
    one_torrent.swarm.add_callback("connected", connected)
    task = asyncio.create_task(client.fetch(one_torrent))
    seeders[0].handles[version].connect_peer(("127.0.0.1", client.port))
    await task
    connected.assert_called_once()
    assert torrent.check(one_torrent.store.root)


@pytest.mark.parametrize("config", [{"listen_host": "127.0.0.1", "port": 0}], indirect=True)
async def test_stopping_closes_connections_waiting_for_a_handshake(config):
    """A connection that hasn't sent its handshake when the client stops is closed"""
    async with (
        Client(config=config) as client,
        await anyio.connect_tcp("127.0.0.1", client.port) as remote,
    ):
        # the client takes the connection, and waits for the handshake
        await anyio.wait_all_tasks_blocked()
        await client.stop()
        with anyio.fail_after(1), pytest.raises(anyio.EndOfStream):
            await remote.receive()


@pytest.mark.parametrize(
    "config",
    [{"listen_host": "127.0.0.1", "port": 0, "ip_blocklist": ["127.0.0.0/8"]}],
    indirect=True,
)
@pytest.mark.network_policy
async def test_incoming_from_blocked_address(config):
    """A connection from a blocked address is closed without waiting for its handshake"""
    async with (
        Client(config=config) as client,
        await anyio.connect_tcp("127.0.0.1", client.port) as remote,
    ):
        with anyio.fail_after(1), pytest.raises(anyio.EndOfStream):
            await remote.receive()


async def test_fetch_timeout(client, one_torrent):
    """
    With no peers, fetch gives up after its timeout, and the pieces are still wanted,
    so they're fetched once there are peers
    """
    start = anyio.current_time()
    with pytest.raises(TimeoutError):
        await client.fetch(one_torrent, [0], timeout=0.2)
    assert anyio.current_time() - start >= 0.2
    assert one_torrent.picker.is_wanted(0)


@pytest.mark.parametrize("version", ["hybrid"], indirect=True)
async def test_add_returns_same_torrent(client, torrent, tmp_path):
    """Adding a torrent twice gives the same one, which we can get by any info hash"""
    t = client.add(torrent, trackers=[])
    assert client.add(torrent) is t
    assert client.get(torrent.v1_infohash) is t
    assert client.get(torrent.v2_infohash) is t
    assert client.get(info_hashes(torrent)[0]) is t
    assert client.torrents == [t]
    assert t.store.root == (tmp_path / torrent.v2_infohash).resolve()


async def test_remove(client, one_torrent):
    """Removing a torrent stops it, and fails anything waiting on it"""
    waiting = asyncio.create_task(client.fetch(one_torrent, [0]))
    await asyncio.sleep(0)
    await client.remove(one_torrent.name)
    with pytest.raises(RuntimeError):
        await waiting
    assert client.get(one_torrent.name) is None


@pytest.mark.parametrize("stuck", list(Stuck))
@pytest.mark.parametrize("how", ["remove", "stop client"])
async def test_stopping_closes_stuck_connections(client, torrent, fake_seeder, stuck, how):
    """
    Removing a torrent, or stopping the client, closes the torrent's connections at once,
    as the peer sees it, even ones waiting on a peer that's gone quiet,
    long before they'd time out. The client can be stopped from any task, in a time limit.
    """
    fake = await fake_seeder(torrent, accept=set(info_hashes(torrent)), stuck=stuck)
    t = client.add(torrent, trackers=[], known_peers=[fake.addr])
    fetching = asyncio.create_task(client.fetch(t))
    with anyio.fail_after(5):
        async with fake.counted:
            await fake.counted.wait_for(
                lambda: fake.open == 1 and (stuck is Stuck.HANDSHAKE or fake.requests > 0)
            )

    with anyio.fail_after(1):
        if how == "remove":
            await client.remove(t.name)
        else:
            await client.stop()
        async with fake.counted:
            await fake.counted.wait_for(lambda: fake.open == 0)
    with pytest.raises(RuntimeError):
        await fetching


async def test_many_torrents(client, torrents, seeders):
    """One client fetches several torrents at once, with one peer id"""
    added = [client.add(torrent) for torrent in torrents.values()]
    await asyncio.gather(*(client.fetch(t) for t in added))
    for t in added:
        assert t.shared.peer_id == client.peer_id
        assert t.torrent.check(t.store.root)


async def test_shared_fetches(client, seeded):
    """Two callers fetching overlapping pieces at once download each piece once"""
    await asyncio.gather(
        client.fetch(seeded, list(range(100))), client.fetch(seeded, list(range(50, 150)))
    )
    pieces = [seeded.layout.piece_range(i) for i in range(150)]
    assert seeded.downloaded == sum(piece.size - piece.padding for piece in pieces)
    assert all(i in seeded.picker.have for i in range(150))


async def test_added_and_removed(client, torrent):
    """The client says when a torrent is added and removed, once each"""
    added: list[TorrentState] = []
    removed: list[TorrentState] = []
    client.add_callback("added", added.append)
    client.add_callback("removed", removed.append)
    t = client.add(torrent, trackers=[])
    client.add(torrent)
    assert added == [t]
    assert removed == []
    await client.remove(t.name)
    # the torrent finishing running afterwards doesn't count again
    await asyncio.sleep(0.1)
    assert removed == [t]


async def test_want_and_have(client, seeded):
    """Wanting pieces doesn't wait for them, and have says which arrived"""
    client.want(seeded, [0, 1])
    assert list(client.have(seeded)) == []
    await client.fetch(seeded, [0, 1])
    assert list(client.have(seeded)) == [0, 1]
    # a copy, so changing it doesn't change what the torrent has
    client.have(seeded).add(5)
    assert 5 not in client.have(seeded)


async def test_remove_tells_trackers(client, torrent, seeders, tracker):
    """Removing a torrent closes its connections and tells its trackers it stopped"""
    t = client.add(torrent, trackers=[[tracker.urls.http]])
    await client.fetch(t, [0])
    assert t.swarm.peers
    assert tracker.swarms_of(client.peer_id) == set(t.info_hashes)
    await client.remove(t.name)
    assert not t.swarm.peers
    assert not tracker.swarms_of(client.peer_id)


@pytest.mark.bans
async def test_state_file(torrent, tmp_path):
    """The client loads banned peers when it starts, and saves them when it closes"""
    path = tmp_path / "state.json"
    path.write_text(json.dumps({"banned": [["10.0.0.1", 1]]}))
    async with Client(config=Config(download_dir=tmp_path, state_file=path)) as client:
        t = client.add(torrent, trackers=[])
        assert t.shared.is_banned(Addr.parse("10.0.0.1", 1))
        t.shared.ban(Addr.parse("10.0.0.2", 2), BanReason.BAD_DATA)
    assert json.loads(path.read_text()) == {"banned": [["10.0.0.1", 1], ["10.0.0.2", 2]]}


async def test_start_and_stop(torrent, tmp_path):
    """
    A client can be started and stopped without being a contextmanager,
    but only started once
    """
    client = Client(config=Config(download_dir=tmp_path))
    assert not client.running
    await client.start()
    assert client.running
    try:
        t = client.add(torrent, trackers=[])
        with pytest.raises(RuntimeError):
            await client.start()
    finally:
        await client.stop()
    assert not client.running
    await client.stop()
    assert client.torrents == []
    assert client._shared is not None and client._shared.http.is_closed
    assert not t.swarm.peers
    with pytest.raises(RuntimeError):
        client.add(torrent, trackers=[])
    with pytest.raises(RuntimeError):
        await client.start()


async def test_failed_start_closes(tmp_path):
    """If starting fails partway, what was opened so far is closed, and the error goes to start"""
    path = tmp_path / "state.json"
    path.write_text("not json")
    config = Config(download_dir=tmp_path, state_file=path, listen_host="127.0.0.1", port=0)
    client = Client(config=config)
    with pytest.raises(json.JSONDecodeError):
        await client.start()
    assert client._shared is not None and client._shared.http.is_closed
    assert not client.running
    # and nowhere else, so stopping has nothing to raise
    await client.stop()


@pytest.mark.parametrize("config", [{"max_connections": 1}], indirect=True)
@pytest.mark.limits
async def test_connection_limit(client, torrents, seeders):
    """
    With one connection for the whole client, the torrents take turns with it:
    the one that wants pieces takes it from the ones that are done
    """
    peers = [s.addr for s in seeders]
    added = [client.add(torrent, trackers=[], known_peers=peers) for torrent in torrents.values()]
    connected = [Mock() for _ in added]
    for t, callback in zip(added, connected):
        t.swarm.add_callback("connected", callback)
    await asyncio.gather(*(client.fetch(t) for t in added))
    assert all(callback.called for callback in connected)
    assert all(t.torrent.check(t.store.root) for t in added)
