"""
Serving a :class:`.TorrentFileSystem` with FUSE, through mfusepy.

Needs the ``mount`` extra, and FUSE: on macOS, macFUSE 5.2 or newer,
mounting with its FSKit backend so no kernel extension is needed; on Linux, libfuse.
Importing this module raises :class:`OSError` if there's no FUSE library.
"""

import errno
import itertools
import logging
import os
import plistlib
import stat
import sys
import time
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

import mfusepy
from fsspec.asyn import sync

from btmc.fsspec.filesystem import AsyncTorrentFile, FetchMode, NodeKind, TorrentFileSystem

log = logging.getLogger(__name__)

FINDER_TAGS = "com.apple.metadata:_kMDItemUserTags"
"""The extended attribute macOS Finder reads a file's tags from"""

type FileHandle = int
"""An open file, from FUSE's ``open``"""

_MODES = {NodeKind.DIRECTORY: stat.S_IFDIR | 0o555, NodeKind.FILE: stat.S_IFREG | 0o444}
_ERRNOS: dict[type[OSError], int] = {
    FileNotFoundError: errno.ENOENT,
    NotADirectoryError: errno.ENOTDIR,
    IsADirectoryError: errno.EISDIR,
}


def mount(
    fs: TorrentFileSystem, root: str, mountpoint: Path, volume_name: str, fetch: FetchMode
) -> None:
    """
    Serve a directory of a synchronous filesystem, like a torrent's info hash,
    at a mount point until it's unmounted or interrupted. Blocks.

    FUSE calls from threads of its own, which run the filesystem's methods on its event loop.
    Call this from the main thread, so FUSE can unmount on Ctrl-C.

    Args:
        fs: the filesystem
        root: the path in it to mount
        mountpoint: where to mount it
        volume_name: what to call the volume
        fetch: when an opened file's pieces are fetched
    """
    options: dict[str, Any] = {"ro": True, "fsname": "btmc", "volname": volume_name}
    if sys.platform == "darwin":
        options["backend"] = "fskit"
    mfusepy.FUSE(_Operations(fs, root, fetch), str(mountpoint), foreground=True, **options)


class _Operations(mfusepy.Operations):
    use_ns = True

    def __init__(self, fs: TorrentFileSystem, root: str, fetch: FetchMode):
        self._fs = fs
        self._root = root
        self._fetch = fetch
        self._files: dict[FileHandle, AsyncTorrentFile] = {}
        self._file_ids = itertools.count(1)
        self._mounted_at = time.time_ns()

    def getattr(self, path: str, fh: FileHandle | None = None) -> dict[str, Any]:
        info = self._call(self._fs._info, self._path(path))
        return {
            "st_mode": _MODES[NodeKind(info["type"])],
            "st_nlink": 1,
            "st_size": info["size"],
            "st_uid": os.getuid(),
            "st_gid": os.getgid(),
            "st_atime": self._mounted_at,
            "st_mtime": self._mounted_at,
            "st_ctime": self._mounted_at,
        }

    def readdir(self, path: str, fh: FileHandle) -> list[str]:
        names = self._call(self._fs._ls, self._path(path), False)
        return [".", "..", *(name.rpartition("/")[2] for name in names)]

    def open(self, path: str, flags: int) -> FileHandle:
        if flags & os.O_ACCMODE != os.O_RDONLY:
            raise mfusepy.FuseOSError(errno.EROFS)
        file = self._call(self._fs.open_async, self._path(path), "rb", self._fetch)
        fh = next(self._file_ids)
        self._files[fh] = file
        return fh

    def read(self, path: str, size: int, offset: int, fh: FileHandle) -> bytes:
        return self._call(self._files[fh].read_at, offset, size)

    def release(self, path: str, fh: FileHandle) -> int:
        self._call(self._files.pop(fh).close)
        return 0

    def destroy(self, path: str) -> None:
        """Unmounting: close the files still open"""
        for fh in list(self._files):
            self.release(path, fh)

    def listxattr(self, path: str) -> list[str]:
        info = self._call(self._fs._info, self._path(path))
        if sys.platform != "darwin" or NodeKind(info["type"]) is NodeKind.DIRECTORY:
            return []
        return [FINDER_TAGS]

    def getxattr(self, path: str, name: str, position: int = 0) -> bytes:
        status = self._call(self._fs.status, self._path(path)) if name == FINDER_TAGS else None
        if status is None:
            raise mfusepy.FuseOSError(mfusepy.ENOATTR)
        return plistlib.dumps([status.finder_tag], fmt=plistlib.FMT_BINARY)

    def _path(self, path: str) -> str:
        return self._root + path.rstrip("/")

    def _call[T](self, fn: Callable[..., Awaitable[T]], *args: Any) -> T:
        """Call an async method on the filesystem's event loop, turning errors into FUSE errors"""
        try:
            return sync(self._fs.loop, fn, *args)
        except OSError as e:
            raise mfusepy.FuseOSError(_ERRNOS.get(type(e), e.errno or errno.EIO)) from e
        except Exception as e:
            log.exception("%s%s failed", fn.__name__, args)
            raise mfusepy.FuseOSError(errno.EIO) from e
