"""
An fsspec filesystem for the files in torrents, fetching pieces as they're read.

Paths are ``torrent://{infohash}/{path within the torrent}``,
with the .torrent file found by a :class:`.TorrentRegistry`,
or ``torrent://{path to a .torrent file}/{path within the torrent}``.

Zip files can also be shown as a directory of what's in them, see ``zips``.

Read-only. Needs the ``fsspec`` extra.
"""

from __future__ import annotations

import asyncio
import dataclasses
import logging
import shutil
import weakref
import zipfile
from collections.abc import Sequence
from dataclasses import dataclass, field
from enum import Enum, StrEnum
from fnmatch import fnmatchcase
from pathlib import Path
from typing import Any, NamedTuple

import anyio
from fsspec.asyn import AbstractAsyncStreamedFile, AsyncFileSystem, sync
from fsspec.spec import AbstractBufferedFile
from torrent_models import Torrent

from btmc.config import Config
from btmc.files.zip import TorrentZip, ZipMember
from btmc.fsspec.registry import TorrentRegistry
from btmc.protocol.client import Client
from btmc.protocol.torrent import TorrentState
from btmc.types import PieceIndex, parse_addr
from btmc.utils import torrent_name

log = logging.getLogger(__name__)

ZIP_DIR_SUFFIX = ".d"
"""Added to a zip's name to name the directory of what's in it"""


class NodeKind(Enum):
    """Whether a path is a file or a directory. The values are the ``type`` in fsspec's info."""

    DIRECTORY = "directory"
    FILE = "file"


class Stat(NamedTuple):
    kind: NodeKind
    size: int
    """0 for directories"""


_DIRECTORY = Stat(NodeKind.DIRECTORY, 0)


class FileStatus(Enum):
    """How much of a file has been fetched"""

    NOT_FETCHED = "Not fetched"
    PARTLY_FETCHED = "Partly fetched"
    ON_DISK = "On disk"

    @property
    def finder_tag(self) -> str:
        """A macOS Finder tag: the name, and the number of its colour after a newline"""
        # Finder's colours: 1 grey, 2 green, 5 yellow
        color = {
            FileStatus.NOT_FETCHED: 1,
            FileStatus.PARTLY_FETCHED: 5,
            FileStatus.ON_DISK: 2,
        }[self]
        return f"{self.value}\n{color}"


class FetchMode(StrEnum):
    """When an open file's pieces are fetched"""

    AS_READ = "as-read"
    """Only the pieces under each read, when it's read"""
    WHOLE_FILE = "whole-file"
    """
    All of the file's pieces as soon as it's opened, so they arrive in parallel.
    They keep coming after the file's closed.
    """


class TorrentFileSystem(AsyncFileSystem):
    """
    The files in torrents, fetched a piece at a time as they're read.

    Pieces are kept in the config's :attr:`.Config.cache_dir`,
    so anything read once is read from disk after that.

    Args:
        cache_dir: where to keep fetched pieces, instead of the config's ``cache_dir``.
            Each torrent gets a directory named by its info hash.
        torrent_dirs: directories to look in for .torrent files,
            to find torrents by info hash. See :class:`.TorrentRegistry`
        torrents: torrents, or .torrent files, to find by info hash
        zips: patterns of paths within torrents, like ``"*.zip"`` or ``"crawl.wacz"``,
            of zip files to also show as a directory of what's in them,
            next to the zip with ``.d`` on the end of its name, like ``crawl.wacz.d``.
            See :func:`fnmatch.fnmatchcase`. The zip's index is read the first time
            its directory is looked in.
        trackers: tracker tiers to use instead of each torrent's own
        peers: peers to connect to without asking a tracker, as ``"host:port"`` or
            ``(host, port)``
        config: client settings, or a dict of them.
            By default, read from ``btmc.toml`` in the current directory if there is one,
            see :meth:`.Config.from_toml`
    """

    protocol = "torrent"
    root_marker = ""

    def __init__(
        self,
        cache_dir: Path | str | None = None,
        torrent_dirs: Sequence[Path | str] = (),
        torrents: Sequence[Torrent[Any] | Path | str] = (),
        zips: Sequence[str] = (),
        trackers: list[list[str]] | None = None,
        peers: Sequence[str | Sequence[Any]] = (),
        config: Config | dict[str, Any] | None = None,
        **kwargs: Any,
    ):
        super().__init__(**kwargs)
        self.registry = TorrentRegistry(dirs=torrent_dirs, files=torrents)
        self.zips = list(zips)
        self.trackers = trackers
        self.peers = [parse_addr(p) for p in peers]
        if config is None:
            config = Config.from_toml()
        elif isinstance(config, dict):
            config = Config(**config)
        if cache_dir is not None:
            config = dataclasses.replace(config, cache_dir=Path(cache_dir).expanduser())
        self.config = config
        self._client: Client | None = None
        self._client_lock = asyncio.Lock()
        self._host: tuple[asyncio.Task, anyio.Event] | None = None
        """The task the client runs in, and the event that stops it"""
        self._finalizer: weakref.finalize | None = None
        self._trees: dict[str, _Tree] = {}
        self._zip_dirs: dict[tuple[str, str], _ZipDir] = {}
        """(torrent name, path of the directory) -> zip directories that have been read"""

    # ---- fsspec ----

    async def _info(self, path: str, **kwargs: Any) -> dict[str, Any]:
        path = self._strip_protocol(path)
        if path == "":
            return self._entry("", _DIRECTORY)
        return self._entry(path, (await self._alocate(path)).stat)

    async def _ls(self, path: str, detail: bool = True, **kwargs: Any) -> list:
        path = self._strip_protocol(path)
        if path == "":
            torrents = await asyncio.to_thread(list, self.registry)
            listing = [self._entry(torrent_name(t), _DIRECTORY) for t in torrents]
        else:
            location = await self._alocate(path)
            if location.stat.kind is NodeKind.FILE:
                listing = [self._entry(path, location.stat)]
            else:
                tree, base, inside = self._tree(location.torrent), location.prefix, location.path
                if location.zip is not None:
                    tree, base = location.zip.tree, f"{location.prefix}/{location.zip.path}"
                elif location.path in tree.zip_dirs:
                    # a zip's directory is only read when it's listed, not when it's looked at
                    zip_dir = await self._zip_dir(location.torrent, location.path)
                    tree, base, inside = zip_dir.tree, f"{location.prefix}/{zip_dir.path}", ""
                listing = [
                    self._entry(f"{base}/{child}", tree.stats[child])
                    for child in tree.children[inside]
                ]
        listing.sort(key=lambda e: e["name"])
        return listing if detail else [e["name"] for e in listing]

    async def _cat_file(
        self, path: str, start: int | None = None, end: int | None = None, **kwargs: Any
    ) -> bytes:
        location = await self._alocate(path)
        if location.stat.kind is NodeKind.DIRECTORY:
            raise IsADirectoryError(path)
        start, end, _ = slice(start, end).indices(location.stat.size)
        if start >= end:
            return b""
        if location.zip is not None:
            member = await location.zip.zip.open_member(location.path)
            try:
                return await member.read(start, end - start)
            finally:
                member.close()
        t = await self.torrent_state(location.torrent)
        return await t.read(location.path, start, end)

    async def _get_file(self, rpath: str, lpath: str, **kwargs: Any) -> None:
        location = await self._alocate(rpath)
        if location.stat.kind is NodeKind.DIRECTORY:
            Path(lpath).mkdir(parents=True, exist_ok=True)
        elif location.zip is not None:
            await location.zip.zip.extract(location.path, Path(lpath))
        elif location.stat.size == 0:
            Path(lpath).write_bytes(b"")
        else:
            t = await self.torrent_state(location.torrent)
            source = await t.fetch_file(location.path)
            await asyncio.to_thread(shutil.copyfile, source, lpath)

    def _open(
        self,
        path: str,
        mode: str = "rb",
        block_size: int | None = None,
        autocommit: bool = True,
        cache_options: dict | None = None,
        **kwargs: Any,
    ) -> TorrentFile:
        """
        Open a file for reading. File objects are synchronous, like all fsspec files.
        Files in zips can only be opened like this on a synchronous filesystem,
        see :meth:`.open_async`.

        Reads aren't cached in memory by default, since fetched pieces are already on disk.
        """
        if mode != "rb":
            raise NotImplementedError("torrents are read-only")
        found = self._locate(path)
        location = found if isinstance(found, _Location) else sync(self.loop, self._alocate, path)
        stat = location.stat
        if stat.kind is NodeKind.DIRECTORY:
            raise IsADirectoryError(path)
        kwargs.setdefault("cache_type", "none")
        return TorrentFile(
            self,
            path,
            mode,
            block_size or "default",
            autocommit,
            cache_options=cache_options,
            size=stat.size,
            **kwargs,
        )

    async def open_async(
        self, path: str, mode: str = "rb", fetch: FetchMode = FetchMode.AS_READ, **kwargs: Any
    ) -> AsyncTorrentFile:
        """
        Open a file for reading on the event loop.

        Args:
            path: the file
            mode: only ``"rb"``
            fetch: when the file's pieces are fetched
        """
        if mode != "rb":
            raise NotImplementedError("torrents are read-only")
        location = await self._alocate(path)
        if location.stat.kind is NodeKind.DIRECTORY:
            raise IsADirectoryError(path)
        if fetch is FetchMode.WHOLE_FILE:
            (await self.torrent_state(location.torrent)).want(self._pieces(location))
        member = None
        if location.zip is not None:
            member = await location.zip.zip.open_member(location.path)
        return AsyncTorrentFile(self, path, member, size=location.stat.size, **kwargs)

    def ukey(self, path: str) -> str:
        _, torrent, inner = self.split(path)
        return f"{torrent_name(torrent)}/{inner}"

    # ---- public ----

    async def get_client(self) -> Client:
        """The client, started on the filesystem's event loop the first time it's needed"""
        async with self._client_lock:
            if self._client is None:
                ready, stop = anyio.Event(), anyio.Event()
                task = asyncio.create_task(self._host_client(ready, stop))
                self._host = (task, stop)
                await ready.wait()
                if self._client is None:
                    # raise whatever stopped the client from starting
                    await task
                if not self.asynchronous:
                    self._finalizer = weakref.finalize(self, self._stop_client, self.loop, stop)
        assert self._client is not None
        return self._client

    async def torrent_state(self, torrent: Torrent[Any]) -> TorrentState:
        """The torrent in the client, added the first time, which starts it fetching"""
        client = await self.get_client()
        return client.add(torrent, trackers=self.trackers, known_peers=self.peers)

    async def status(self, path: str) -> FileStatus | None:
        """
        How much of a file has been fetched, or None for a directory.
        Adds the torrent to the client, if it isn't yet.
        """
        location = await self._alocate(path)
        if location.stat.kind is NodeKind.DIRECTORY:
            return None
        pieces = self._pieces(location)
        have = (await self.torrent_state(location.torrent)).have
        n_have = sum(piece in have for piece in pieces)
        if n_have == len(pieces):
            return FileStatus.ON_DISK
        return FileStatus.PARTLY_FETCHED if n_have else FileStatus.NOT_FETCHED

    def split(self, path: str) -> tuple[str, Torrent[Any], str]:
        """
        Split a path into the part that names the torrent, the torrent,
        and the path within the torrent.

        Blocking: finding the torrent can read .torrent files.
        """
        path = self._strip_protocol(path)
        parts = path.split("/")
        for i, part in enumerate(parts):
            if part.endswith(".torrent"):
                torrent_path = "/".join(parts[: i + 1])
                return torrent_path, self.registry.add(torrent_path), "/".join(parts[i + 1 :])
        try:
            torrent = self.registry.get(parts[0])
        except ValueError:
            # not an info hash
            torrent = None
        if torrent is None:
            raise FileNotFoundError(f"no .torrent file found for {parts[0]}")
        return parts[0], torrent, "/".join(parts[1:])

    def close(self) -> None:
        """Close all connections. The filesystem starts a new client if it's used again."""
        sync(self.loop, self._close)

    # ---- private ----

    async def _host_client(self, ready: anyio.Event, stop: anyio.Event) -> None:
        # the client has to be opened and closed in the same task,
        # and fsspec runs each call in a task of its own
        try:
            async with Client(self.config.cache_dir, config=self.config) as client:
                self._client = client
                ready.set()
                await stop.wait()
        finally:
            self._client = None
            ready.set()

    @staticmethod
    def _stop_client(loop: asyncio.AbstractEventLoop | None, stop: anyio.Event) -> None:
        if loop is not None and loop.is_running():
            loop.call_soon_threadsafe(stop.set)

    async def _close(self) -> None:
        if self._finalizer is not None:
            self._finalizer.detach()
        # their zips read through the client being stopped
        self._zip_dirs.clear()
        if self._host is not None:
            task, stop = self._host
            self._host = None
            stop.set()
            await task

    def _tree(self, torrent: Torrent[Any]) -> _Tree:
        name = torrent_name(torrent)
        if name not in self._trees:
            self._trees[name] = _Tree.from_torrent(torrent, self.zips)
        return self._trees[name]

    def _locate(self, path: str) -> _Location | _InZip:
        """
        Like :meth:`.split`, and also what's at the path,
        unless it's in a zip, which needs the zip's index read first. Blocking.
        """
        prefix, torrent, inner = self.split(path)
        tree = self._tree(torrent)
        for zip_dir in tree.zip_dirs:
            if inner.startswith(zip_dir + "/"):
                return _InZip(prefix, torrent, zip_dir, inner[len(zip_dir) + 1 :])
        if inner not in tree.stats:
            raise FileNotFoundError(path)
        return _Location(prefix, torrent, inner, tree.stats[inner], None)

    async def _alocate(self, path: str) -> _Location:
        """Like :meth:`._locate`, reading the zip's index if the path is in a zip"""
        found = await asyncio.to_thread(self._locate, path)
        if isinstance(found, _Location):
            return found
        zip_dir = await self._zip_dir(found.torrent, found.zip_dir)
        if found.path not in zip_dir.tree.stats:
            raise FileNotFoundError(path)
        stat = zip_dir.tree.stats[found.path]
        return _Location(found.prefix, found.torrent, found.path, stat, zip_dir)

    async def _zip_dir(self, torrent: Torrent[Any], path: str) -> _ZipDir:
        """A zip's directory, reading the zip's index the first time"""
        key = (torrent_name(torrent), path)
        if key not in self._zip_dirs:
            zip_path = self._tree(torrent).zip_dirs[path]
            zip = TorrentZip(await self.get_client(), await self.torrent_state(torrent), zip_path)
            tree = _Tree.from_zip(await zip.open(), zip_path)
            # if two tasks read the index at once, keep the first
            self._zip_dirs.setdefault(key, _ZipDir(path, zip, tree))
        return self._zip_dirs[key]

    def _pieces(self, location: _Location) -> list[PieceIndex]:
        """The pieces a file is in"""
        tree = location.zip.tree if location.zip else self._tree(location.torrent)
        if location.path not in tree.pieces:
            if location.zip is not None:
                tree.pieces[location.path] = location.zip.zip.pieces(location.path)
            else:
                ranges = location.torrent.layout.byte_range_pieces(
                    location.path, 0, location.stat.size
                )
                tree.pieces[location.path] = [piece.piece_idx for piece in ranges]
        return tree.pieces[location.path]

    @staticmethod
    def _entry(name: str, stat: Stat) -> dict[str, Any]:
        return {"name": name, "size": stat.size, "type": stat.kind.value}


class TorrentFile(AbstractBufferedFile):
    """A file in a torrent, fetching the pieces each read needs"""

    fs: TorrentFileSystem

    def _fetch_range(self, start: int, end: int) -> bytes:
        return sync(self.fs.loop, self.fs._cat_file, self.path, start, end)


class AsyncTorrentFile(AbstractAsyncStreamedFile):
    """
    A file in a torrent, or in a zip in one, read on the event loop,
    fetching the pieces each read needs.

    A file in a zip keeps one reader of the zip open,
    so a read that carries on from the last one doesn't unzip from the start again.
    """

    fs: TorrentFileSystem

    def __init__(self, fs: TorrentFileSystem, path: str, member: ZipMember | None, **kwargs: Any):
        kwargs.setdefault("cache_type", "none")
        super().__init__(fs, path, mode="rb", **kwargs)
        self._member = member

    async def read_at(self, offset: int, size: int) -> bytes:
        """Read without moving the file's position, so several reads can run at once"""
        end = min(offset + size, self.size)
        if offset >= end:
            return b""
        return await self._fetch_range(offset, end)

    async def _fetch_range(self, start: int, end: int) -> bytes:
        if self._member is not None:
            return await self._member.read(start, end - start)
        return await self.fs._cat_file(self.path, start, end)

    async def close(self) -> None:
        if self._member is not None:
            self._member.close()
        await super().close()


@dataclass
class _Tree:
    """The files and directories in a torrent, or in a zip"""

    stats: dict[str, Stat] = field(default_factory=lambda: {"": _DIRECTORY})
    """path, "" for the top -> what's there"""
    children: dict[str, list[str]] = field(default_factory=lambda: {"": []})
    """directory path -> paths of what's in it"""
    zip_dirs: dict[str, str] = field(default_factory=dict)
    """path of a zip's directory -> path of the zip"""
    pieces: dict[str, list[PieceIndex]] = field(default_factory=dict)
    """path -> the pieces of the file there, worked out the first time they're needed"""

    @classmethod
    def from_torrent(cls, torrent: Torrent[Any], zips: Sequence[str]) -> _Tree:
        """
        Args:
            torrent: the torrent
            zips: patterns of paths of files to show a directory of what's in them for
        """
        tree = cls()
        for f in torrent.files:
            tree._add(f.path, Stat(NodeKind.FILE, f.length))
        for f in torrent.files:
            if not any(fnmatchcase(f.path, pattern) for pattern in zips):
                continue
            zip_dir = f.path + ZIP_DIR_SUFFIX
            if zip_dir in tree.stats:
                log.warning("not showing what's in %s, %s is in the torrent", f.path, zip_dir)
                continue
            tree._add(zip_dir, _DIRECTORY)
            tree.zip_dirs[zip_dir] = f.path
        return tree

    @classmethod
    def from_zip(cls, zf: zipfile.ZipFile, name: str) -> _Tree:
        """
        The members of a zip. Their names come from whoever made the zip,
        so ones that would leave the directory, or clash with another, are left out.
        """
        tree = cls()
        for info in zf.infolist():
            path = info.filename.removesuffix("/") if info.is_dir() else info.filename
            if not path or any(part in ("", ".", "..") for part in path.split("/")):
                log.warning("leaving out %r in %s, it's not a safe path", info.filename, name)
                continue
            stat = _DIRECTORY if info.is_dir() else Stat(NodeKind.FILE, info.file_size)
            if not tree._add(path, stat):
                log.warning("leaving out %r in %s, something else is there", info.filename, name)
        return tree

    def _add(self, path: str, stat: Stat) -> bool:
        """
        Add what's at a path, and the directories above it that aren't there yet.
        False if something else is there already, or a file is in the way.
        """
        if path in self.stats:
            return stat.kind is NodeKind.DIRECTORY and self.stats[path].kind is NodeKind.DIRECTORY
        parent = path.rpartition("/")[0]
        if not self._add(parent, _DIRECTORY):
            return False
        self.stats[path] = stat
        self.children[parent].append(path)
        if stat.kind is NodeKind.DIRECTORY:
            self.children[path] = []
        return True


class _ZipDir(NamedTuple):
    """A zip, shown as a directory of what's in it"""

    path: str
    """Of the directory, within the torrent"""
    zip: TorrentZip
    tree: _Tree


class _Location(NamedTuple):
    """Where a path is"""

    prefix: str
    """The part of the path that names the torrent"""
    torrent: Torrent[Any]
    path: str
    """Within the torrent, or within the zip if it's in one"""
    stat: Stat
    zip: _ZipDir | None
    """The zip it's in, if it's in one"""


class _InZip(NamedTuple):
    """A path in a zip's directory, before the zip's index is read to find out what's there"""

    prefix: str
    torrent: Torrent[Any]
    zip_dir: str
    path: str
    """Within the zip"""
