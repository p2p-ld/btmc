"""
The fsspec filesystem, used through fsspec's synchronous api.

fsspec runs it on its own event loop, while the test session's loop (and the tracker on it)
sits idle, so these connect to the libtorrent seeders directly instead of using the tracker.
"""

import dataclasses
import json

import fsspec
import pytest
from torrent_models import LoadLimits

from btmc.config import Config
from btmc.fsspec import TorrentFileSystem
from btmc.utils import torrent_name

from ..fixtures import PAGES, PIECE_LENGTH

pytestmark = pytest.mark.libtorrent

TOP_LEVEL = [
    "archive.zip",
    "big.bin",
    "crawl.wacz",
    "empty.dat",
    "exactly_one_piece.bin",
    "small.txt",
    "sub",
]


@pytest.fixture
def fs_options(torrent_dir, seeders, tmp_path) -> dict:
    """Options for a filesystem that finds the test torrents by info hash"""
    return {
        "download_dir": str(tmp_path / "downloads"),
        "torrent_dirs": [str(torrent_dir)],
        "trackers": [],
        "peers": [str(s.addr) for s in seeders],
    }


@pytest.fixture
def fs(make_fs, fs_options) -> TorrentFileSystem:
    return make_fs(**fs_options)


def test_ls_root(fs, torrents):
    """The root lists every torrent the registry can find"""
    assert fs.ls("", detail=False) == sorted(torrent_name(t) for t in torrents.values())


def test_ls_and_info(fs, torrent):
    name = torrent_name(torrent)
    assert fs.ls(f"torrent://{name}", detail=False) == [f"{name}/{p}" for p in TOP_LEVEL]
    assert fs.ls(f"{name}/sub", detail=False) == [f"{name}/sub/mid.bin"]
    assert fs.info(f"{name}/sub/mid.bin") == {
        "name": f"{name}/sub/mid.bin",
        "size": 40000,
        "type": "file",
    }
    assert fs.isdir(f"{name}/sub")
    assert fs.isdir(name)
    with pytest.raises(FileNotFoundError):
        fs.info(f"{name}/nope")
    with pytest.raises(FileNotFoundError):
        fs.info("0" * 40 + "/big.bin")
    with pytest.raises(FileNotFoundError):
        fs.info("nope/big.bin")


def test_cat_file(fs, torrent, test_data):
    name = torrent_name(torrent)
    big = (test_data / "testdata" / "big.bin").read_bytes()
    assert fs.cat_file(f"{name}/big.bin", start=100_000, end=100_100) == big[100_000:100_100]
    assert fs.cat_file(f"{name}/big.bin", start=-10) == big[-10:]
    small = (test_data / "testdata" / "small.txt").read_bytes()
    assert fs.cat_file(f"torrent://{name}/small.txt") == small


def test_open_fetches_only_what_is_read(fs, torrent, test_data):
    """Seeking and reading an open file fetches just the pieces under each read"""
    name = torrent_name(torrent)
    big = (test_data / "testdata" / "big.bin").read_bytes()
    start = 5 * PIECE_LENGTH + 7
    with fs.open(f"{name}/big.bin") as f:
        f.seek(start)
        assert f.read(100) == big[start : start + 100]
        f.seek(-50, 2)
        assert f.read() == big[-50:]
    t = fs._client.get(name)
    reads = [(start, start + 100), (len(big) - 50, len(big))]
    under_reads = {
        piece.piece_idx for a, b in reads for piece in t.layout.byte_range_pieces("big.bin", a, b)
    }
    assert set(t.have) == under_reads


def test_get_file(fs, torrent, test_data, tmp_path):
    """Getting a file fetches all of it, and copies it out"""
    name = torrent_name(torrent)
    for path in ("sub/mid.bin", "empty.dat"):
        dest = tmp_path / "out" / path.replace("/", "_")
        dest.parent.mkdir(exist_ok=True)
        fs.get_file(f"{name}/{path}", str(dest))
        assert dest.read_bytes() == (test_data / "testdata" / path).read_bytes()


def test_torrent_file_path(fs, torrent_file, test_data):
    """A torrent can be named by the path to its .torrent file instead of its info hash"""
    sub = f"{torrent_file}/sub"
    assert fs.ls(f"torrent://{sub}", detail=False) == [f"{sub}/mid.bin"]
    small = (test_data / "testdata" / "small.txt").read_bytes()
    assert fs.cat_file(f"torrent://{torrent_file}/small.txt") == small


def test_zip_chaining(fs_options, torrent):
    """fsspec's zip filesystem reads a member of a WACZ in a torrent, fetching as it goes"""
    name = torrent_name(torrent)
    url = f"zip://pages/pages.jsonl::torrent://{name}/crawl.wacz"
    with fsspec.open(url, torrent=fs_options) as f:
        lines = f.read().decode().splitlines()
    assert [json.loads(line) for line in lines[1:]] == PAGES


@pytest.mark.limits
def test_torrents_past_the_limits(make_fs, torrent_file, torrent):
    """.torrent files are read with the config's limits, and ones past them aren't found"""
    config = Config(torrent_limits=LoadLimits(max_pieces=1))
    fs = make_fs(torrent_dirs=[torrent_file.parent], config=config)
    assert fs.registry.get(torrent_name(torrent)) is None
    assert make_fs(torrent_dirs=[torrent_file.parent]).registry.get(torrent_name(torrent))


def test_download_dir_goes_in_config(make_fs, tmp_path):
    """download_dir is kept on the config, replacing its download_dir and nothing else"""
    config = Config(max_peers=3)
    fs = make_fs(download_dir=tmp_path, config=config)
    assert fs.config == dataclasses.replace(config, download_dir=tmp_path)
    assert make_fs(config=config).config == config
