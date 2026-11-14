from pathlib import Path

import pytest
from torrent_models import Torrent, TorrentCreate, TorrentVersion
from torrent_models.create import list_files

from .data import PIECE_LENGTH
from .tracker import TrackerUrls

__all__ = ["torrent", "torrent_dir", "torrent_file", "torrent_files", "torrents", "version"]


@pytest.fixture(scope="session")
def torrent_dir(tmp_path_factory: pytest.TempPathFactory) -> Path:
    return tmp_path_factory.mktemp("torrents")


@pytest.fixture(scope="session")
def torrent_files(
    test_data: Path, tracker_urls: TrackerUrls, torrent_dir: Path
) -> dict[TorrentVersion, Path]:
    """
    A .torrent file of the :func:`.test_data` for each version, made by torrent-models
    the way libtorrent would make it, announcing to the :func:`.tracker`, all in one directory.
    Clients that use their trackers need the :func:`.tracker` to be running.
    """
    src = test_data / "testdata"
    create = TorrentCreate(
        paths=list_files(src),
        path_root=src,
        piece_length=PIECE_LENGTH,
        trackers=[tracker_urls.http, tracker_urls.udp],
    )
    files = {}
    for version in TorrentVersion:
        files[version] = torrent_dir / f"{version}.torrent"
        create.generate(version).write(files[version])
    return files


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
