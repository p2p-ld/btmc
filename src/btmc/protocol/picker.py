"""The state of every piece of a torrent, and choosing which block to ask a peer for"""

from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

import anyio
from torrent_models.layout import FileLayout
from torrent_models.types.v2 import BLOCK_SIZE

from btmc.protocol.messages import Bitfield
from btmc.types import Block, BlockLength, BlockOffset, PieceIndex

type PieceCallback = Callable[[list[PieceIndex]], None]
"""Called with pieces that just entered a state, see :meth:`.PiecePicker.add_callback`"""

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
    blocks: dict[BlockOffset, BlockLength] = field(default_factory=dict)
    """Once it's started"""
    unrequested: dict[BlockOffset, None] = field(default_factory=dict)
    """Blocks nobody has been asked for, in order"""
    received: set[BlockOffset] = field(default_factory=set)


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

    Complete pieces are held in memory until their hash check is done, so when the checks
    fall behind, by ``max_checking`` pieces, no new piece is started until they catch up.
    Callers waiting for room hear about it when a piece is had, or opened again by failing.

    Each block is asked of one peer at a time, and goes back up for grabs
    when it's released with :meth:`.release_blocks`, like when that peer chokes us
    or goes away.

    Padfiles at the end of a piece are all zeros, so they're never asked for (BEP 47).

    Callbacks added with :meth:`.add_callback` hear about pieces entering a state.

    Args:
        layout: the torrent's pieces and files
        max_checking: the most pieces waiting for their hash check before no more are started
        have: the pieces we have already
    """

    def __init__(self, layout: FileLayout[Any], max_checking: int, have: Bitfield | None = None):
        self.n_pieces = layout.n_pieces
        self._layout = layout
        self._max_checking = max_checking
        self.have = Bitfield(self.n_pieces) if have is None else have
        """Pieces that passed their hash check and were written. Only the picker changes it."""
        self._wanted: dict[PieceIndex, _WantedPiece] = {}
        """Wanted pieces we don't have yet, in the order they were first wanted"""
        self._callbacks: dict[PieceState, list[PieceCallback]] = {state: [] for state in PieceState}
        self._done: dict[PieceIndex, anyio.Event] = {}
        """Set when the piece passes, for anyone waiting for it"""
        self._has_wanted = anyio.Event()
        """Set while anything is wanted, and replaced with a new one when nothing is"""
        self._closed = False

    @property
    def n_wanted(self) -> int:
        """The number of wanted pieces we don't have yet"""
        return len(self._wanted)

    def piece_state(self, index: PieceIndex) -> PieceState:
        if index in self.have:
            return PieceState.HAVE
        if (piece := self._wanted.get(index)) is not None:
            return piece.state
        return PieceState.UNWANTED

    def add_callback(self, state: PieceState, callback: PieceCallback) -> None:
        """
        Call ``callback`` with pieces whenever they enter ``state``.
        Pieces wanted together come in one call, in the order they were wanted.
        """
        self._callbacks[state].append(callback)

    def remove_callback(self, state: PieceState, callback: PieceCallback) -> None:
        """Stop calling a callback, like when whatever added it goes away before the torrent"""
        self._callbacks[state].remove(callback)

    def _entered(self, state: PieceState, pieces: list[PieceIndex]) -> None:
        for callback in self._callbacks[state]:
            callback(pieces)

    # ---- wanting and waiting ----

    def want(self, pieces: Iterable[PieceIndex]) -> None:
        """
        Mark the pieces as wanted.

        Raises IndexError for any not in the torrent, without wanting any of them.
        """
        pieces = list(pieces)
        for index in pieces:
            if not 0 <= index < self.n_pieces:
                raise IndexError(f"piece {index} out of range, torrent has {self.n_pieces} pieces")
        new: list[PieceIndex] = []
        for index in pieces:
            if index not in self.have and index not in self._wanted:
                self._wanted[index] = _WantedPiece(DEFAULT_PRIORITY)
                new.append(index)
        if new:
            self._has_wanted.set()
            self._entered(PieceState.OPEN, new)

    async def wait_for_wanted(self) -> None:
        """Return once anything is wanted, right away if something is already"""
        await self._has_wanted.wait()

    def is_wanted(self, index: PieceIndex) -> bool:
        """Whether the piece is wanted and we don't have it yet"""
        return index in self._wanted

    def peer_has_wanted_pieces(self, has: Bitfield) -> bool:
        """Whether a peer with these pieces has any we want"""
        return any(index in has for index in self._wanted)

    async def wait(self, pieces: Iterable[PieceIndex]) -> None:
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

    def passed(self, index: PieceIndex) -> None:
        """A piece passed its hash check and was written"""
        self._checking(index)
        del self._wanted[index]
        if not self._wanted:
            self._has_wanted = anyio.Event()
        self.have.add(index)
        if (event := self._done.pop(index, None)) is not None:
            event.set()
        self._entered(PieceState.HAVE, [index])

    def found(self, pieces: list[PieceIndex]) -> None:
        """
        Pieces that passed their check without being fetched, because their data was
        already on disk. Wanted ones can't have been started yet.
        """
        new = [index for index in pieces if index not in self.have]
        had_wanted = bool(self._wanted)
        for index in new:
            piece = self._wanted.pop(index, None)
            if piece is not None and piece.state is not PieceState.OPEN:
                raise ValueError(f"piece {index} was started, so it can't be found on disk")
            self.have.add(index)
            if (event := self._done.pop(index, None)) is not None:
                event.set()
        # only replaced once it's been set, or whoever's waiting for it would miss the next want
        if had_wanted and not self._wanted:
            self._has_wanted = anyio.Event()
        if new:
            self._entered(PieceState.HAVE, new)

    def failed(self, index: PieceIndex) -> None:
        """A piece failed its hash check, so fetch it again"""
        self._checking(index).state = PieceState.OPEN
        self._entered(PieceState.OPEN, [index])

    def _checking(self, index: PieceIndex) -> _WantedPiece:
        piece = self._wanted.get(index)
        if piece is None or piece.state is not PieceState.CHECKING:
            raise ValueError(f"piece {index} isn't waiting for its hash check")
        return piece

    # ---- blocks ----

    def pick(self, has: Bitfield) -> Block | None:
        """
        The next block to ask a peer for, if it has any we want

        Limit yielding pieces according to the _max_checking value -
        applies backpressure to fetching by not requesting more blocks
        while our hashing queue is full.
        """
        first_open: tuple[PieceIndex, _WantedPiece] | None = None
        checking = 0
        for index, piece in self._wanted.items():
            match piece.state:
                case PieceState.CHECKING:
                    checking += 1
                case PieceState.DOWNLOADING if index in has and piece.unrequested:
                    return self._take(index, piece)
                case PieceState.OPEN if first_open is None and index in has:
                    first_open = (index, piece)
        if first_open is None or checking >= self._max_checking:
            return None
        index, piece = first_open
        if not self._start(index, piece):
            # it was all padding, so it's checking already, and another piece might have blocks
            return self.pick(has)
        return self._take(index, piece)

    def _start(self, index: PieceIndex, piece: _WantedPiece) -> bool:
        """
        Split a piece into the blocks to ask for, leaving out the padfiles at its end.
        False if it's all padding, so there's nothing to ask for and it's checking already.
        """
        piece_range = self._layout.piece_range(index)
        size = piece_range.size - piece_range.padding
        piece.blocks = {b: min(BLOCK_SIZE, size - b) for b in range(0, size, BLOCK_SIZE)}
        piece.unrequested = dict.fromkeys(piece.blocks)
        piece.received = set()
        piece.state = PieceState.DOWNLOADING if piece.blocks else PieceState.CHECKING
        self._entered(piece.state, [index])
        return bool(piece.blocks)

    @staticmethod
    def _take(index: PieceIndex, piece: _WantedPiece) -> Block:
        begin = next(iter(piece.unrequested))
        del piece.unrequested[begin]
        return Block(index, begin, piece.blocks[begin])

    def received(self, block: Block) -> bool:
        """
        A block arrived. False if it isn't one we're waiting for: the wrong size,
        or here already. The piece is checking once its last block arrives.
        """
        piece = self._wanted.get(block.piece)
        if (
            piece is None
            or piece.state is not PieceState.DOWNLOADING
            or block.begin in piece.received
            or piece.blocks.get(block.begin) != block.length
        ):
            return False
        piece.received.add(block.begin)
        piece.unrequested.pop(block.begin, None)
        if len(piece.received) == len(piece.blocks):
            piece.state = PieceState.CHECKING
            self._entered(PieceState.CHECKING, [block.piece])
        return True

    def release_blocks(self, blocks: Iterable[Block]) -> None:
        """Put blocks back up for grabs"""
        for block in blocks:
            piece = self._wanted.get(block.piece)
            if (
                piece is not None
                and piece.state is PieceState.DOWNLOADING
                and block.begin not in piece.received
            ):
                piece.unrequested[block.begin] = None
