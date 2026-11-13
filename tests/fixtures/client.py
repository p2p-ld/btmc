from collections.abc import AsyncIterator
from pathlib import Path

import pytest
from torrent_models import Torrent

from btmc import Client, Config, TorrentState

from .seeders import LtSeeder
from .tracker import MiniTracker

__all__ = ["client", "config", "one_torrent", "seeded"]


@pytest.fixture
def config(request: pytest.FixtureRequest, tmp_path: Path) -> Config:
    """Downloads go in ``tmp_path``. Parametrize indirectly with a dict of other settings."""
    return Config(download_dir=tmp_path, **getattr(request, "param", {}))


@pytest.fixture
async def client(config: Config, tracker: MiniTracker) -> AsyncIterator[Client]:
    """
    A running client. After the test it stops, and every torrent it had
    has closed its connections and left the tracker's swarms.
    """
    added: list[TorrentState] = []
    async with Client(config=config) as client:
        client.add_callback("added", added.append)
        yield client
    for t in added:
        assert not t.swarm.peers
    assert not tracker.swarms_of(client.peer_id)


@pytest.fixture
async def one_torrent(client: Client, torrent: Torrent) -> TorrentState:
    """The torrent, added to the client with no trackers, so it has no peers unless given some"""
    return client.add(torrent, trackers=[])


@pytest.fixture
async def seeded(client: Client, torrent: Torrent, seeders: list[LtSeeder]) -> TorrentState:
    """The torrent, added to the client with no trackers, and the libtorrent seeders as peers"""
    return client.add(torrent, trackers=[], known_peers=[s.addr for s in seeders])
