"""Reading files inside a zip file inside a torrent, fetching only the pieces that hold them"""

import io
import shutil
import struct
import zipfile
from collections.abc import AsyncIterator
from itertools import islice
from pathlib import Path

import anyio
import anyio.from_thread
import anyio.to_thread

from btmc.protocol.client import Client
from btmc.protocol.torrent import TorrentState
from btmc.types import PieceIndex

_LOCAL_HEADER = struct.Struct("<4s2B4HL2L2H")
"""The header before each member's data, ending with the lengths of its name and extra field"""
_LOCAL_HEADER_SIGNATURE = b"PK\x03\x04"
_UTF8_NAME = 1 << 11
"""Flag bit saying a member's name is UTF-8, rather than cp437"""
_UNSUPPORTED_FLAGS = {
    1 << 0: "encryption",
    1 << 5: "compressed patched data",
    1 << 6: "strong encryption",
}
"""Flag bits of features :mod:`zipfile` can't read, or we can't without a password"""


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
        """So the index is only read once, by one :class:`zipfile.ZipFile`"""

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
        await self._fetch_member(name)
        with await self._open_file(name) as file:
            return await anyio.to_thread.run_sync(file.read)

    async def lines(self, name: str, batch_size: int) -> AsyncIterator[list[bytes]]:
        """
        Lines of a member of the zip, ``batch_size`` at a time,
        so a big member doesn't have to be in memory all at once.

        Fetches all of the member's pieces to disk first, like :meth:`.read`.
        """
        await self._fetch_member(name)
        with await self._open_file(name) as file:
            while batch := await anyio.to_thread.run_sync(_read_lines, file, batch_size):
                yield batch

    async def extract(self, name: str, dest: Path) -> None:
        """Write a member to a file, fetching all of its pieces first"""
        await self._fetch_member(name)
        with await self._open_file(name) as file:
            await anyio.to_thread.run_sync(_copy_to, file, dest)

    async def open_member(self, name: str) -> "ZipMember":
        """Open a member for reading at any offset, fetching just the pieces each read needs"""
        return ZipMember(await self._open_file(name))

    def pieces(self, name: str) -> list[PieceIndex]:
        """The pieces a member is in, once the zip is :meth:`.open`"""
        if name not in self._ranges:
            raise KeyError(f"there is no item named {name} in the zip")
        ranges = self.torrent.layout.byte_range_pieces(self.path, *self._ranges[name])
        return [piece.piece_idx for piece in ranges]

    async def _fetch_member(self, name: str) -> None:
        """Fetch all of a member's pieces at once, not a piece at a time as zipfile reads it"""
        await self.open()
        await self.client.fetch(self.torrent, self.pieces(name))

    async def _open_file(self, name: str) -> zipfile.ZipExtFile:
        """
        A member as a file, reading the zip through a file object of its own.
        Members opened by :class:`zipfile.ZipFile` share its file object, and so a lock,
        so reading one would wait while another waits for pieces.
        """
        info = (await self.open()).getinfo(name)
        _, end = self._ranges[name]
        reader = _BlockingReader(self.client, self.torrent, self.path)
        return await anyio.to_thread.run_sync(_member_file, reader, info, end)

    @staticmethod
    def _member_ranges(zf: zipfile.ZipFile) -> dict[str, tuple[int, int]]:
        """
        Byte range of each member: its local header, name, extra field, and data.

        The local extra field can be a different length than the one in the central directory,
        so we can't compute the exact end, but it has to end before the next member starts,
        or the central directory for the last one. Members that share a local header
        share its range. A name that's in the zip twice is the later one, like in zipfile.
        """
        offsets = sorted({i.header_offset for i in zf.infolist()})
        ends = dict(zip(offsets, [*offsets[1:], zf.start_dir], strict=True))
        return {i.filename: (i.header_offset, ends[i.header_offset]) for i in zf.infolist()}


class ZipMember:
    """
    An open member of a :class:`.TorrentZip`, read at any offset.

    Uncompressed members seek straight to the offset. Compressed ones are unzipped up to it,
    from the start again when seeking backwards.
    """

    def __init__(self, file: io.BufferedIOBase):
        self._file = file
        self._lock = anyio.Lock()
        """Reads have to take turns, since each one seeks first"""

    async def read(self, offset: int, size: int) -> bytes:
        async with self._lock:
            return await anyio.to_thread.run_sync(_read_at, self._file, offset, size)

    def close(self) -> None:
        self._file.close()


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


def _member_file(reader: "_BlockingReader", info: zipfile.ZipInfo, end: int) -> zipfile.ZipExtFile:
    """
    Like :meth:`zipfile.ZipFile.open`, but reading through ``reader``, with its checks:
    the member's local header has to have its name, and its data has to end by ``end``,
    where the next member starts, so members can't overlap like they do in zip bombs.
    Raises BadZipFile if they don't, and NotImplementedError for a member that's encrypted
    or uses a feature zipfile can't read.
    """
    for flag, feature in _UNSUPPORTED_FLAGS.items():
        if info.flag_bits & flag:
            raise NotImplementedError(f"{info.filename} uses {feature}, which isn't supported")
    reader.seek(info.header_offset)
    header = reader.read(_LOCAL_HEADER.size)
    if len(header) != _LOCAL_HEADER.size:
        raise zipfile.BadZipFile(f"truncated header for {info.filename}")
    signature, _, _, flags, *_, name_length, extra_length = _LOCAL_HEADER.unpack(header)
    if signature != _LOCAL_HEADER_SIGNATURE:
        raise zipfile.BadZipFile(f"bad header for {info.filename}")
    name = reader.read(name_length)
    try:
        matches = name.decode("utf-8" if flags & _UTF8_NAME else "cp437") == info.orig_filename
    except UnicodeDecodeError:
        matches = False
    if not matches:
        raise zipfile.BadZipFile(f"{info.filename}'s header has another name, {name!r}")
    reader.seek(extra_length, io.SEEK_CUR)
    if reader.tell() + info.compress_size > end:
        raise zipfile.BadZipFile(f"{info.filename} overlaps the next member")
    return zipfile.ZipExtFile(reader, "r", info, close_fileobj=True)


def _copy_to(f: io.BufferedIOBase, dest: Path) -> None:
    """Copy in one worker thread call, since reading and writing both block"""
    with dest.open("wb") as out:
        shutil.copyfileobj(f, out)


def _read_lines(f: io.BufferedIOBase, n: int) -> list[bytes]:
    """Read in one worker thread call, since iterating the file is what blocks"""
    return list(islice(f, n))


def _read_at(f: io.BufferedIOBase, offset: int, size: int) -> bytes:
    """Seek and read in one worker thread call, since both can block"""
    f.seek(offset)
    return f.read(size)
