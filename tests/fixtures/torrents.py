from pathlib import Path

import pytest
from torrent_models import Torrent, TorrentVersion

from .data import PIECE_LENGTH
from .tracker import TrackerUrls

try:
    import libtorrent as lt
except ImportError:
    lt = None

__all__ = ["torrent", "torrent_dir", "torrent_file", "torrent_files", "torrents", "version"]


def _make_torrent(src: Path, out: Path, flags: int, trackers: list[str]) -> Path:
    ct = lt.create_torrent(lt.list_files(str(src)), PIECE_LENGTH, flags)
    for tier, url in enumerate(trackers):
        ct.add_tracker(url, tier)
    lt.set_piece_hashes(ct, str(src.parent))
    out.write_bytes(lt.bencode(ct.generate()))
    return out

@pytest.fixture(scope="session")
def torrent_dir(tmp_path_factory: pytest.TempPathFactory) -> Path:
    return tmp_path_factory.mktemp("torrents")


@pytest.fixture(scope="session")
def torrent_files(
    test_data: Path, tracker_urls: TrackerUrls, torrent_dir: Path
) -> dict[TorrentVersion, Path]:
    """
    A .torrent file of the :func:`.test_data` for each version, made by libtorrent,
    announcing to the :func:`.tracker`, all in one directory.
    Clients that use their trackers need the :func:`.tracker` to be running.
    """
    if lt is None:
        pytest.skip("libtorrent isn't installed")
    urls = [tracker_urls.http, tracker_urls.udp]
    flags = {
        TorrentVersion.v1: lt.create_torrent.v1_only,
        TorrentVersion.v2: lt.create_torrent.v2_only,
        TorrentVersion.hybrid: 0,
    }
    return {
        version: _make_torrent(test_data / "testdata", torrent_dir / f"{version}.torrent", flag, urls)
        for version, flag in flags.items()
    }


@pytest.fixture(scope="session")
def torrents(torrent_files: dict[TorrentVersion, Path]) -> dict[TorrentVersion, Torrent]:
    """The :func:`.torrent_files`, read with torrent-models"""
    return {version: Torrent.read(path) for version, path in torrent_files.items()}


@pytest.fixture(params=list(TorrentVersion))
def version(request: pytest.FixtureRequest) -> TorrentVersion:
    """
    Each torrent version in turn, for :func:`.torrent` and :func:`.torrent_file`.
    Parametrize indirectly to use only some.
    """
    return TorrentVersion(request.param)


@pytest.fixture
def torrent_file(version: TorrentVersion, torrent_files: dict[TorrentVersion, Path]) -> Path:
    """The .torrent file of the test data for :func:`.version`"""
    return torrent_files[version]


@pytest.fixture
def torrent(version: TorrentVersion, torrents: dict[TorrentVersion, Torrent]) -> Torrent:
    """The torrent of the test data for :func:`.version`"""
    return torrents[version]
