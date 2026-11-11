"""Reading files inside a zip file inside a torrent, fetching only the pieces that hold them"""

import io
import zipfile
from collections.abc import AsyncIterator
from itertools import islice
from typing import IO

import anyio
import anyio.from_thread
import anyio.to_thread

from btmc.protocol.client import Client
from btmc.protocol.torrent import TorrentState


class TorrentZip:
    """
    A zip file inside a torrent.

    Reads the zip's index from the end of the file first,
    and then fetches only the pieces that hold the members that are read.

    Args:
        client: the client to fetch with
        torrent: the torrent, added to the client
        path: path of the zip file within the torrent, as in :attr:`.Torrent.files`
    """

    def __init__(self, client: Client, torrent: TorrentState, path: str):
        self.client = client
        self.torrent = torrent
        self.path = path
        self._file = _BlockingReader(client, torrent, path)
        self._zip: zipfile.ZipFile | None = None
        self._ranges: dict[str, tuple[int, int]] = {}
        self._opening = anyio.Lock()
        """So only one :class:`zipfile.ZipFile` is made, as two would share the file's position"""

    async def open(self) -> zipfile.ZipFile:
        """Read the zip's index, and open it"""
        async with self._opening:
            if self._zip is None:
                self._zip = await anyio.to_thread.run_sync(zipfile.ZipFile, self._file)
                self._ranges = self._member_ranges(self._zip)
        return self._zip

    async def namelist(self) -> list[str]:
        return (await self.open()).namelist()

    async def read(self, name: str) -> bytes:
        """Read a member of the zip, fetching only the pieces it's in"""
        zf = await self.open()
        await self._fetch_member(name)
        return await anyio.to_thread.run_sync(zf.read, name)

    async def lines(self, name: str, batch_size: int) -> AsyncIterator[list[bytes]]:
        """
        Lines of a member of the zip, ``batch_size`` at a time,
        so a big member doesn't have to be in memory all at once.

        Fetches all of the member's pieces to disk first, like :meth:`.read`.
        """
        zf = await self.open()
        await self._fetch_member(name)
        with await anyio.to_thread.run_sync(zf.open, name) as member:
            while batch := await anyio.to_thread.run_sync(_read_lines, member, batch_size):
                yield batch

    async def _fetch_member(self, name: str) -> None:
        """Fetch all of a member's pieces at once, not a piece at a time as zipfile reads it"""
        if name not in self._ranges:
            raise KeyError(f"there is no item named {name} in the zip")
        pieces = self.torrent.layout.byte_range_pieces(self.path, *self._ranges[name])
        await self.client.fetch(self.torrent, (piece.piece_idx for piece in pieces))

    @staticmethod
    def _member_ranges(zf: zipfile.ZipFile) -> dict[str, tuple[int, int]]:
        """
        Byte range of each member: its local header, name, extra field, and data.

        The local extra field can be a different length than the one in the central directory,
        so we can't compute the exact end, but it has to end before the next member starts.
        """
        infos = sorted(zf.infolist(), key=lambda i: i.header_offset)
        ends = [i.header_offset for i in infos[1:]] + [zf.start_dir]
        return {i.filename: (i.header_offset, end) for i, end in zip(infos, ends)}


class _BlockingReader(io.RawIOBase):
    """
    A readable, seekable file in a torrent, for blocking code like :mod:`zipfile`.

    Reads fetch the pieces they need and read them back from disk, so nothing is kept in memory.
    Only usable from a worker thread started with :func:`anyio.to_thread.run_sync`.
    Cancelling the task that started the thread cancels the fetch it's waiting on.
    """

    def __init__(self, client: Client, torrent: TorrentState, path: str):
        self._client = client
        self._torrent = torrent
        self._path = path
        self._length = torrent.layout.file(path).length
        self._pos = 0

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return True

    def tell(self) -> int:
        return self._pos

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        base = {io.SEEK_SET: 0, io.SEEK_CUR: self._pos, io.SEEK_END: self._length}[whence]
        self._pos = base + offset
        return self._pos

    def readinto(self, b: bytearray | memoryview) -> int:  # type: ignore[override]
        end = min(self._pos + len(b), self._length)
        if end <= self._pos:
            return 0
        data = anyio.from_thread.run(self._client.read, self._torrent, self._path, self._pos, end)
        b[: len(data)] = data
        self._pos += len(data)
        return len(data)



def _read_lines(f: IO[bytes], n: int) -> list[bytes]:
    return list(islice(f, n))
