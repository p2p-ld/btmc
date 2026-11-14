"""
The FUSE operations, called from outside the filesystem's event loop like FUSE's own threads,
without mounting anything
"""

import errno
import os
import plistlib
import stat
import sys
from collections.abc import Callable, Generator
from typing import Any

import pytest

try:
    import mfusepy

    from btmc.fsspec.fuse import FINDER_TAGS, _Operations
except OSError:
    pytest.skip("no FUSE library", allow_module_level=True)

from btmc.fsspec.filesystem import FetchMode

pytestmark = pytest.mark.libtorrent


@pytest.fixture
def ops(torrent_fs) -> Generator[_Operations]:
    """
    The operations for one of the test torrents, on a synchronous filesystem,
    unmounted after the test, which closes the files still open
    """
    ops = _Operations(*torrent_fs(), FetchMode.AS_READ)
    yield ops
    ops.destroy("/")


def fuse_errno(fn: Callable[..., Any], *args: Any) -> int:
    with pytest.raises(mfusepy.FuseOSError) as e:
        fn(*args)
    return e.value.errno


def test_fuse_listing_and_reading(ops, test_data):
    assert "archive.zip.d" in ops.readdir("/", 0)
    assert "small.txt" in ops.readdir("/archive.zip.d/dir", 0)
    assert stat.S_ISDIR(ops.getattr("/archive.zip.d")["st_mode"])
    file = ops.getattr("/small.txt")
    assert stat.S_ISREG(file["st_mode"])
    assert file["st_size"] == 3000

    fh = ops.open("/small.txt", os.O_RDONLY)
    data = ops.read("/small.txt", 100, 50, fh)
    ops.release("/small.txt", fh)
    assert data == (test_data / "testdata" / "small.txt").read_bytes()[50:150]


def test_fuse_unmounting_closes_open_files(ops):
    ops.open("/small.txt", os.O_RDONLY)
    ops.open("/archive.zip.d/stored.bin", os.O_RDONLY)
    files = list(ops._files.values())
    ops.destroy("/")
    assert ops._files == {}
    assert all(file.closed for file in files)


def test_fuse_errors(ops):
    assert fuse_errno(ops.getattr, "/nope") == errno.ENOENT
    assert fuse_errno(ops.open, "/sub", os.O_RDONLY) == errno.EISDIR
    assert fuse_errno(ops.open, "/small.txt", os.O_WRONLY) == errno.EROFS


@pytest.mark.skipif(sys.platform != "darwin", reason="Finder tags are only on macOS")
def test_fuse_finder_tags(ops):
    """Files are tagged with whether they're on disk, and directories aren't tagged"""
    assert ops.listxattr("/small.txt") == [FINDER_TAGS]
    assert plistlib.loads(ops.getxattr("/small.txt", FINDER_TAGS)) == ["Not fetched\n1"]

    fh = ops.open("/small.txt", os.O_RDONLY)
    ops.read("/small.txt", 3000, 0, fh)
    ops.release("/small.txt", fh)
    assert plistlib.loads(ops.getxattr("/small.txt", FINDER_TAGS)) == ["On disk\n2"]

    assert ops.listxattr("/sub") == []
    assert fuse_errno(ops.getxattr, "/sub", FINDER_TAGS) == mfusepy.ENOATTR
    assert fuse_errno(ops.getxattr, "/small.txt", "user.other") == mfusepy.ENOATTR
