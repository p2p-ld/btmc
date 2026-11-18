from collections.abc import AsyncIterator
from pathlib import Path

import platformdirs
import pytest
from torrent_models import Torrent

from btmc import Client, Config, TorrentState

from .seeders import LtSeeder
from .tracker import MiniTracker

__all__ = ["client", "config", "one_torrent", "seeded", "state_dir"]


@pytest.fixture(autouse=True)
def state_dir(tmp_path_factory: pytest.TempPathFactory, monkeypatch: pytest.MonkeyPatch) -> Path:
    """
    The default :attr:`.Config.state_dir`, a new one for each test, so tests never touch
    the real one, or see each other's resume files. Also for configs made inside btmc,
    like the CLI's.
    """
    path = tmp_path_factory.mktemp("state")
    monkeypatch.setattr(platformdirs, "user_state_path", lambda *args, **kwargs: path)
    return path


@pytest.fixture
def config(request: pytest.FixtureRequest, tmp_path: Path, state_dir: Path) -> Config:
    """Downloads go in ``tmp_path``. Parametrize indirectly with a dict of other settings."""
    return Config(download_dir=tmp_path, state_dir=state_dir, **getattr(request, "param", {}))


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
