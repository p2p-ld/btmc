import contextlib
import os
from pathlib import Path

from torrent_models.types.v1 import V1PieceRange
from torrent_models.types.v2 import V2PieceRange

from btmc.protocol.messages import Bitfield


def safe_join(root: Path, path: str) -> Path:
    """
    ``root / path``, refusing paths that would end up outside of root.

    Paths in torrents and zips come from strangers.
    """
    root = root.resolve()
    joined = (root / path).resolve()
    if not joined.is_relative_to(root):
        raise ValueError(f"{path} is outside of {root}")
    return joined


class PieceStore:
    """
    Writes verified pieces into the torrent's files, and saves which pieces we have.

    Files are created sparse, on first write. Which pieces we have is kept in a small
    resume file next to the data, so they aren't fetched again.
    The resume file is trusted as-is, without rechecking the data.

    Reading and writing block, so run them in a worker thread.
    Pieces can be written from several threads at once, since they never overlap,
    but call :meth:`.save_have` from one thread at a time.
    """

    def __init__(self, root: Path, n_pieces: int, name: str):
        self.root = root.resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self.n_pieces = n_pieces
        self.resume_path = self.root / f".{name}.have"
        self._paths: dict[str, Path] = {}

    def load_have(self) -> Bitfield:
        """The pieces we had last time, from the resume file, which is made if it isn't there"""
        have = Bitfield(self.n_pieces)
        if self.resume_path.exists():
            with contextlib.suppress(ValueError):
                have = Bitfield(self.n_pieces, self.resume_path.read_bytes())
        self.resume_path.write_bytes(have.bits)
        return have

    def save_have(self, index: int) -> None:
        """
        Save that we have a piece to the resume file, made by :meth:`.load_have`.
        Reads and rewrites the byte holding the piece's bit, so call it from one thread at a time.
        """
        fd = os.open(self.resume_path, os.O_RDWR)
        try:
            (byte,) = os.pread(fd, 1, index >> 3)
            os.pwrite(fd, bytes([byte | 0x80 >> (index & 7)]), index >> 3)
        finally:
            os.close(fd)

    def path(self, full_path: str) -> Path:
        """Where a file in the torrent is on disk"""
        if full_path not in self._paths:
            self._paths[full_path] = safe_join(self.root, full_path)
        return self._paths[full_path]

    def write(self, piece: V1PieceRange | V2PieceRange, blocks: list[bytes]) -> None:
        """Write a verified piece into the files it covers"""
        data = memoryview(b"".join(blocks))
        if isinstance(piece, V2PieceRange):
            self._write(piece.full_path, piece.range_start, data)
        else:
            pos = 0
            for f in piece.ranges:
                n = f.range_end - f.range_start
                if not f.is_padfile:
                    self._write(f.full_path, f.range_start, data[pos : pos + n])
                pos += n

    def _write(self, full_path: str, offset: int, data: memoryview) -> None:
        path = self.path(full_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o644)
        try:
            while data:
                n = os.pwrite(fd, data, offset)
                data, offset = data[n:], offset + n
        finally:
            os.close(fd)

    def read(self, full_path: str, start: int, end: int) -> bytes:
        fd = os.open(self.path(full_path), os.O_RDONLY)
        try:
            chunks = []
            while start < end:
                chunk = os.pread(fd, end - start, start)
                if not chunk:
                    raise EOFError(f"{full_path} ends before byte {end}")
                chunks.append(chunk)
                start += len(chunk)
            return b"".join(chunks)
        finally:
            os.close(fd)
