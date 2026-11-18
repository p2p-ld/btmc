"""Finding .torrent files by info hash"""

import logging
import threading
from collections.abc import Iterator, Sequence
from pathlib import Path
from typing import Any

from torrent_models import LoadLimits, Torrent

from btmc.utils import info_hashes, wire_hash

log = logging.getLogger(__name__)


class TorrentRegistry:
    """
    Finds torrents by info hash, from .torrent files added directly or found in directories.

    Directories are searched when a torrent is asked for, not up front.
    A file or directory named by the info hash right in the directory is checked first,
    like sciop's ``{infohash}/{name}.torrent`` layout,
    and every other .torrent file is read only if that finds nothing.

    Safe to use from several threads.

    Args:
        dirs: directories to look for .torrent files in, recursively
        files: torrents, or .torrent files, to add now
        limits: limits on the .torrent files it reads. By default there are none.
    """

    def __init__(
        self,
        dirs: Sequence[Path | str] = (),
        files: Sequence[Torrent[Any] | Path | str] = (),
        limits: LoadLimits | None = None,
    ):
        self.dirs = [Path(d) for d in dirs]
        self._limits = LoadLimits() if limits is None else limits
        self._torrents: dict[bytes, Torrent[Any]] = {}
        """info hash, see :func:`.wire_hash` -> torrent"""
        self._by_path: dict[Path, Torrent[Any] | None] = {}
        """.torrent file -> what was in it, or None if it couldn't be read"""
        self._lock = threading.RLock()
        for file in files:
            self.add(file)

    def add(self, torrent: Torrent[Any] | Path | str) -> Torrent[Any]:
        """Add a torrent, or the .torrent file at a path. Each file is only read once."""
        with self._lock:
            if not isinstance(torrent, Torrent):
                path = Path(torrent).resolve()
                cached = self._by_path.get(path)
                if cached is not None:
                    return cached
                torrent = Torrent.read(path, context={"limits": self._limits})
                self._by_path[path] = torrent
            for info_hash in info_hashes(torrent):
                self._torrents[info_hash] = torrent
            return torrent

    def get(self, info_hash: str | bytes) -> Torrent[Any] | None:
        """A torrent by any of its info hashes, see :func:`.wire_hash`"""
        with self._lock:
            key = wire_hash(info_hash)
            if key not in self._torrents:
                self._search(info_hash.hex() if isinstance(info_hash, bytes) else info_hash)
            return self._torrents.get(key)

    def __iter__(self) -> Iterator[Torrent[Any]]:
        """Every torrent, reading every .torrent file in the directories"""
        with self._lock:
            self._search(None)
            return iter(list({id(t): t for t in self._torrents.values()}.values()))

    def _search(self, hex_hash: str | None) -> None:
        for d in self.dirs:
            if hex_hash is not None:
                named = d / hex_hash.lower()
                for file in named.rglob("*.torrent") if named.is_dir() else [named]:
                    self._try_add(file)
                if wire_hash(hex_hash) in self._torrents:
                    return
            for file in d.rglob("*.torrent"):
                self._try_add(file)
                if hex_hash is not None and wire_hash(hex_hash) in self._torrents:
                    return

    def _try_add(self, file: Path) -> None:
        path = file.resolve()
        if path in self._by_path or path.suffix != ".torrent" or not path.is_file():
            return
        try:
            self.add(path)
        except Exception as e:
            log.warning("couldn't read %s: %r", path, e)
            self._by_path[path] = None
