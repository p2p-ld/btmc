from collections.abc import AsyncIterator, Callable, Iterator, Sequence
from pathlib import Path
from typing import Any

import pytest
from torrent_models import Torrent

from btmc.fsspec.filesystem import TorrentFileSystem
from btmc.utils import torrent_name

from ..fixtures import LtSeeder

type MakeFs = Callable[..., TorrentFileSystem]
type MakeTorrentFs = Callable[..., tuple[TorrentFileSystem, str]]


@pytest.fixture
async def make_fs(anyio_backend: str) -> AsyncIterator[MakeFs]:
    """
    Makes filesystems, with any options, and closes them after the test.
    Asks for ``anyio_backend`` so sync tests can use it too.
    """
    made: list[TorrentFileSystem] = []

    def make(**options: Any) -> TorrentFileSystem:
        fs = TorrentFileSystem(**options, skip_instance_cache=True)
        made.append(fs)
        return fs

    yield make
    for fs in made:
        if fs.asynchronous:
            await fs._close()
        else:
            fs.close()


@pytest.fixture
def torrent_fs(
    make_fs: MakeFs, torrent: Torrent, seeders: list[LtSeeder], tmp_path: Path
) -> MakeTorrentFs:
    """
    Makes filesystems with the :func:`.torrent` in them, fetching from a seeder,
    and gives the torrent's path in each. Each gets its own download directory.
    """
    n_made = 0

    def make(
        zips: Sequence[str] = ("*.zip",), asynchronous: bool = False
    ) -> tuple[TorrentFileSystem, str]:
        nonlocal n_made
        n_made += 1
        fs = make_fs(
            download_dir=tmp_path / f"downloads-{n_made}",
            torrents=[torrent],
            zips=zips,
            trackers=[],
            peers=[seeders[0].addr],
            asynchronous=asynchronous,
        )
        return fs, torrent_name(torrent)

    return make


@pytest.fixture(autouse=True)
def close_cached_filesystems() -> Iterator[None]:
    """Closes the filesystems fsspec made and cached during a test, like for ``zip://`` urls"""
    yield
    for fs in list(TorrentFileSystem._cache.values()):
        fs.close()
    TorrentFileSystem.clear_instance_cache()
