"""
Zip files in torrents shown as directories of what's in them,
and the filesystem's async files and status, which the mount uses
"""

import io
import zipfile

import pytest

from btmc.fsspec.filesystem import FetchMode, FileStatus, NodeKind, Stat, TorrentFileSystem, _Tree

from ..fixtures import ZIP_MEMBERS

pytestmark = pytest.mark.libtorrent


async def read_all(fs: TorrentFileSystem, path: str) -> bytes:
    file = await fs.open_async(path)
    try:
        return await file.read_at(0, file.size)
    finally:
        await file.close()


async def test_zip_dirs(torrent_fs):
    """Zips matching the patterns get a directory next to them of what's in them"""
    fs, t = torrent_fs(zips=["*.zip", "crawl.wacz"], asynchronous=True)
    names = await fs._ls(t, detail=False)
    expected = {"archive.zip", "archive.zip.d", "crawl.wacz", "crawl.wacz.d"}
    assert {f"{t}/{name}" for name in expected} <= set(names)
    assert await fs._info(f"{t}/archive.zip.d") == {
        "name": f"{t}/archive.zip.d",
        "size": 0,
        "type": "directory",
    }
    assert fs._zip_dirs == {}, "looking at a zip's directory doesn't read the zip"

    names = await fs._ls(f"{t}/archive.zip.d", detail=False)
    assert names == [f"{t}/archive.zip.d/{name}" for name in ["deflated.txt", "dir", "stored.bin"]]
    assert await fs._ls(f"{t}/archive.zip.d/dir", detail=False) == [
        f"{t}/archive.zip.d/dir/small.txt"
    ]
    info = await fs._info(f"{t}/archive.zip.d/stored.bin")
    assert info["size"] == ZIP_MEMBERS["stored.bin"]
    assert f"{t}/crawl.wacz.d/pages" in await fs._ls(f"{t}/crawl.wacz.d", detail=False)


async def test_zip_dirs_only_for_patterns(torrent_fs):
    fs, t = torrent_fs(zips=(), asynchronous=True)
    assert f"{t}/archive.zip.d" not in await fs._ls(t, detail=False)
    fs, t = torrent_fs(asynchronous=True)
    names = await fs._ls(t, detail=False)
    assert f"{t}/archive.zip.d" in names
    assert f"{t}/crawl.wacz.d" not in names


async def test_reads(torrent_fs, test_data, tmp_path):
    """Files in the torrent and in zips read the same as the originals, at any offset"""
    src = test_data / "testdata"
    fs, t = torrent_fs(asynchronous=True)
    assert await read_all(fs, f"{t}/sub/mid.bin") == (src / "sub" / "mid.bin").read_bytes()
    with zipfile.ZipFile(src / "archive.zip") as source:
        for name in ZIP_MEMBERS:
            path = f"{t}/archive.zip.d/{name}"
            expected = source.read(name)
            file = await fs.open_async(path)
            try:
                # forwards, then backwards, then past the end
                for offset in (10, len(expected) // 2, 3):
                    got = await file.read_at(offset, 1000)
                    assert got == expected[offset : offset + 1000], (name, offset)
                assert await file.read_at(len(expected) + 10, 100) == b""
            finally:
                await file.close()
            assert await fs._cat_file(path, 5, 50) == expected[5:50]
            await fs._get_file(path, str(tmp_path / "out"))
            assert (tmp_path / "out").read_bytes() == expected


def test_zip_dirs_sync(torrent_fs):
    """Zips' directories work through fsspec's synchronous api too"""
    fs, t = torrent_fs()
    path = f"{t}/archive.zip.d/dir/small.txt"
    with fs.open(path) as f:
        assert fs.cat_file(path) == f.read()
    assert len(fs.cat_file(path)) == ZIP_MEMBERS["dir/small.txt"]


async def test_status(torrent_fs):
    """Whether a file is on disk follows the pieces it's in"""
    fs, t = torrent_fs(asynchronous=True)
    assert await fs.status(f"{t}/big.bin") is FileStatus.NOT_FETCHED
    await fs._cat_file(f"{t}/big.bin", 0, 10)
    assert await fs.status(f"{t}/big.bin") is FileStatus.PARTLY_FETCHED
    await read_all(fs, f"{t}/big.bin")
    assert await fs.status(f"{t}/big.bin") is FileStatus.ON_DISK

    member = f"{t}/archive.zip.d/stored.bin"
    assert await fs.status(member) is FileStatus.NOT_FETCHED
    await read_all(fs, member)
    assert await fs.status(member) is FileStatus.ON_DISK

    assert await fs.status(f"{t}/empty.dat") is FileStatus.ON_DISK
    assert await fs.status(f"{t}/sub") is None
    assert await fs.status(f"{t}/archive.zip.d") is None


@pytest.mark.parametrize("fetch", list(FetchMode))
async def test_fetch_modes(torrent_fs, torrent, fetch):
    """
    Opening a file asks for all of its pieces with WHOLE_FILE, and doesn't with AS_READ.
    Closing it doesn't stop them.
    """
    fs, t = torrent_fs(asynchronous=True)
    state = await fs.torrent_state(torrent)
    for path in [f"{t}/big.bin", f"{t}/archive.zip.d/stored.bin"]:
        await (await fs.open_async(path, fetch=fetch)).close()
        pieces = fs._pieces(await fs._alocate(path))
        asked = [p for p in pieces if state.picker.is_wanted(p) or p in state.have]
        if fetch is FetchMode.WHOLE_FILE:
            assert asked == pieces, path
        elif path.endswith("big.bin"):
            assert asked == [], path


async def test_errors(torrent_fs):
    fs, t = torrent_fs(asynchronous=True)
    with pytest.raises(FileNotFoundError):
        await fs._info(f"{t}/nope")
    with pytest.raises(FileNotFoundError):
        await fs._info(f"{t}/archive.zip.d/nope")
    with pytest.raises(IsADirectoryError):
        await fs.open_async(f"{t}/archive.zip.d/dir")


@pytest.mark.filterwarnings("ignore:Duplicate name")
@pytest.mark.confinement
def test_tree_from_zip_leaves_out_unsafe_and_clashing_names():
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name in ["ok.txt", "../up.txt", "a/../b.txt", "/abs.txt", "a//b.txt", "d/", "f"]:
            zf.writestr(name, b"x")
        # a directory where there's already a file, and a file that's there twice
        zf.writestr("f/g.txt", b"x")
        zf.writestr("ok.txt", b"again")
    with zipfile.ZipFile(buf) as zf:
        tree = _Tree.from_zip(zf, "test.zip")
    assert sorted(tree.children[""]) == ["d", "f", "ok.txt"]
    assert tree.stats["d"] == Stat(NodeKind.DIRECTORY, 0)
    assert tree.children["d"] == []
    assert tree.stats["ok.txt"] == Stat(NodeKind.FILE, 1)
