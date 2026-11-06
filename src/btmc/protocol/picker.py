"""The state of every piece of a torrent, and choosing which block to ask a peer for"""

from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from enum import Enum
from typing import Literal

import anyio
from torrent_models.types.v2 import BLOCK_SIZE

from btmc.protocol.messages import Bitfield

Block = tuple[int, int, int]
"""piece, offset, length"""

PickerEvent = Literal["want"]
"""
What callbacks can be added for, see :meth:`.PiecePicker.add_callback`

- ``"want"``: more pieces are wanted. Called with the ones just wanted, in order,
  leaving out any wanted already or that we have.
"""

DEFAULT_PRIORITY = 1


class PieceState(Enum):
    UNWANTED = "unwanted"
    """Not had, and nobody has asked for it"""
    OPEN = "open"
    """Wanted, and waiting to be started"""
    DOWNLOADING = "downloading"
    """Some of its blocks have been asked for"""
    CHECKING = "checking"
    """All of its blocks have arrived, and it's waiting for its hash check"""
    HAVE = "have"
    """Checked and written"""


@dataclass
class _WantedPiece:
    """
    A wanted piece we don't have yet.
    Private representation of a piece that does not store any piece data!
    """

    priority: int
    state: PieceState = PieceState.OPEN
    blocks: dict[int, int] = field(default_factory=dict)
    """offset -> length, once it's started"""
    unrequested: dict[int, None] = field(default_factory=dict)
    """Offsets of blocks nobody has been asked for, in order"""
    received: set[int] = field(default_factory=set)


class PiecePicker:
    """
    The state of every piece of a torrent.

    Liaises between the TorrentState and Swarm to coordinate which pieces to download.
    Operates only on pieces and blocks, does not do any I/O and stores no peer info.

    A piece that hasn't been requested is not represented (implicitly has `UNWANTED` state).
    A piece that has been requested is added to `_wanted` with a Wanted piece state
    When a piece is finished, it is removed from `_wanted` and put in `have`

    Once wanted, a piece stays wanted. Pieces are started in the order they were
    first wanted, and the ones started are finished before new ones are started.
    A piece that fails its hash check keeps its place.

    Each block is asked of one peer at a time, and goes back up for grabs
    when it's :meth:`.unrequest` ed, like when that peer chokes us or goes away.

    Args:
        n_pieces: the number of pieces in the torrent
        piece_size: the number of bytes in a piece, from its index
        have: the pieces we have already
    """

    def __init__(
        self, n_pieces: int, piece_size: Callable[[int], int], have: Bitfield | None = None
    ):
        self.n_pieces = n_pieces
        self.piece_size = piece_size
        self.have = Bitfield(n_pieces) if have is None else have
        """Pieces that passed their hash check and were written. Only the picker changes it."""
        self._wanted: dict[int, _WantedPiece] = {}
        """Wanted pieces we don't have yet, in the order they were first wanted"""
        self._callbacks: dict[PickerEvent, list[Callable[[list[int]], None]]] = {"want": []}
        self._done: dict[int, anyio.Event] = {}
        """Set when the piece passes, for anyone waiting for it"""
        self._closed = False

    @property
    def n_wanted(self) -> int:
        """The number of wanted pieces we don't have yet"""
        return len(self._wanted)

    def piece_state(self, index: int) -> PieceState:
        if index in self.have:
            return PieceState.HAVE
        if (piece := self._wanted.get(index)) is not None:
            return piece.state
        return PieceState.UNWANTED

    def add_callback(self, event: PickerEvent, callback: Callable[[list[int]], None]) -> None:
        """Call ``callback`` whenever ``event`` happens"""
        if event not in self._callbacks:
            raise ValueError(f"no such event: {event}")
        self._callbacks[event].append(callback)

    # ---- wanting and waiting ----

    def want(self, pieces: Iterable[int]) -> None:
        """
        Mark the pieces as wanted.

        Raises IndexError for any not in the torrent, without wanting any of them.
        """
        pieces = list(pieces)
        for index in pieces:
            if not 0 <= index < self.n_pieces:
                raise IndexError(f"piece {index} out of range, torrent has {self.n_pieces} pieces")
        new = []
        for index in pieces:
            if index not in self.have and index not in self._wanted:
                self._wanted[index] = _WantedPiece(DEFAULT_PRIORITY)
                new.append(index)
        if new:
            for callback in self._callbacks["want"]:
                callback(new)

    def is_wanted(self, index: int) -> bool:
        """Whether the piece is wanted and we don't have it yet"""
        return index in self._wanted

    def peer_has_wanted_pieces(self, has: Bitfield) -> bool:
        """Whether a peer with these pieces has any we want"""
        return any(index in has for index in self._wanted)

    async def wait(self, pieces: Iterable[int]) -> None:
        """
        Want the pieces, and return once we have all of them.
        Stopping waiting doesn't stop them being wanted.
        Raises RuntimeError if the picker is closed first.
        """
        pieces = list(pieces)
        self.want(pieces)
        for index in pieces:
            if index in self.have:
                continue
            if not self._closed:
                await self._done.setdefault(index, anyio.Event()).wait()
            if index not in self.have:
                raise RuntimeError("stopped before the pieces arrived")

    def close(self) -> None:
        """Stop, and fail everything waiting"""
        self._closed = True
        for event in self._done.values():
            event.set()
        self._done.clear()

    # ---- checking ----

    def passed(self, index: int) -> None:
        """A piece passed its hash check and was written"""
        self._checking(index)
        del self._wanted[index]
        self.have.add(index)
        if (event := self._done.pop(index, None)) is not None:
            event.set()

    def failed(self, index: int) -> None:
        """A piece failed its hash check, so fetch it again"""
        self._checking(index).state = PieceState.OPEN

    def _checking(self, index: int) -> _WantedPiece:
        piece = self._wanted.get(index)
        if piece is None or piece.state is not PieceState.CHECKING:
            raise ValueError(f"piece {index} isn't waiting for its hash check")
        return piece

    # ---- blocks ----

    def pick(self, has: Bitfield) -> Block | None:
        """The next block to ask a peer for, if it has any we want"""
        first_open = None
        for index, piece in self._wanted.items():
            if index not in has:
                continue
            if piece.state is PieceState.DOWNLOADING and piece.unrequested:
                return self._take(index, piece)
            if piece.state is PieceState.OPEN and first_open is None:
                first_open = index
        if first_open is None:
            return None
        piece = self._wanted[first_open]
        size = self.piece_size(first_open)
        piece.blocks = {b: min(BLOCK_SIZE, size - b) for b in range(0, size, BLOCK_SIZE)}
        piece.unrequested = dict.fromkeys(piece.blocks)
        piece.received = set()
        piece.state = PieceState.DOWNLOADING
        return self._take(first_open, piece)

    @staticmethod
    def _take(index: int, piece: _WantedPiece) -> Block:
        begin = next(iter(piece.unrequested))
        del piece.unrequested[begin]
        return index, begin, piece.blocks[begin]

    def received(self, index: int, begin: int, length: int) -> bool:
        """
        A block arrived. False if it isn't one we're waiting for: the wrong size,
        or here already. The piece is checking once its last block arrives.
        """
        piece = self._wanted.get(index)
        if (
            piece is None
            or piece.state is not PieceState.DOWNLOADING
            or begin in piece.received
            or piece.blocks.get(begin) != length
        ):
            return False
        piece.received.add(begin)
        piece.unrequested.pop(begin, None)
        if len(piece.received) == len(piece.blocks):
            piece.state = PieceState.CHECKING
        return True

    def release_blocks(self, requests: Iterable[tuple[int, int]]) -> None:
        """Put blocks, as (piece, offset), back up for grabs"""
        for index, begin in requests:
            piece = self._wanted.get(index)
            if (
                piece is not None
                and piece.state is PieceState.DOWNLOADING
                and begin not in piece.received
            ):
                piece.unrequested[begin] = None
