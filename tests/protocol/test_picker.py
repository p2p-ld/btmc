import anyio
import pytest
from torrent_models import KiB, Torrent
from torrent_models.hashing.hybrid import add_padfiles
from torrent_models.types import FileItem

from btmc.protocol.messages import Bitfield
from btmc.protocol.picker import PiecePicker, PieceState
from btmc.types import Block

PIECE_LENGTH = 64 * KiB
PIECE = 40000
"""The data in each piece before its padding: three blocks, two of 16384 bytes and one of 7232"""


def torrent(files: list[FileItem]) -> Torrent:
    """A torrent of the files, each padded out to the end of its last piece"""
    files = add_padfiles(files, PIECE_LENGTH)
    n_pieces = sum(f.length for f in files) // PIECE_LENGTH
    info = {"name": "t", "piece length": PIECE_LENGTH, "pieces": bytes(20 * n_pieces)}
    return Torrent.model_validate({"info": {**info, "files": files}})


TEN_PIECES = torrent([FileItem(path=[str(i)], length=PIECE) for i in range(10)])


@pytest.fixture
def picker() -> PiecePicker:
    """A picker for :data:`.TEN_PIECES`, with room for 10 pieces waiting for their check"""
    return PiecePicker(TEN_PIECES.layout, 10)


def test_padding_isnt_asked_for():
    """
    The padding at the end of a piece isn't asked for,
    and a piece that's all padding goes straight to its check
    """
    a, b = FileItem(path=["a"], length=PIECE), FileItem(path=["b"], length=PIECE)
    picker = PiecePicker(torrent([a, FileItem.padfile(PIECE_LENGTH), b]).layout, 10)
    checking = []
    picker.add_callback(PieceState.CHECKING, checking.extend)
    picker.want([1, 2])
    picked = [picker.pick(Bitfield.from_pieces(3, [1, 2])) for _ in range(4)]
    assert picked == [
        Block(2, 0, 16384),
        Block(2, 16384, 16384),
        Block(2, 32768, 7232),
        None,
    ]
    assert checking == [1]


@pytest.mark.limits
def test_no_new_piece_while_checks_are_behind():
    """
    While the most pieces allowed are waiting for their check, no new piece is started,
    though ones already started are finished, and one passing makes room for another
    """
    picker = PiecePicker(TEN_PIECES.layout, 1)
    picker.want([1, 2, 3])
    assert picker.pick(Bitfield.from_pieces(10, [2])) == Block(2, 0, 16384)
    while (block := picker.pick(Bitfield.from_pieces(10, [1]))) is not None:
        picker.received(block)
    assert picker.piece_state(1) is PieceState.CHECKING
    assert picker.pick(Bitfield.from_pieces(10, [2])) == Block(2, 16384, 16384)
    assert picker.pick(Bitfield.from_pieces(10, [3])) is None
    picker.passed(1)
    assert picker.pick(Bitfield.from_pieces(10, [3])) == Block(3, 0, 16384)


def test_pieces_in_the_order_first_wanted(picker):
    """
    Pieces are started in the order they're first wanted, and finished before starting another
    """
    wanted = []
    picker.add_callback(PieceState.OPEN, wanted.append)
    picker.want([5, 6])
    picker.want([1, 5])
    picker.want([6])
    assert wanted == [[5, 6], [1]]
    picked = [picker.pick(Bitfield.from_pieces(10, [1, 5, 6])) for _ in range(7)]
    assert picked == [
        Block(5, 0, 16384),
        Block(5, 16384, 16384),
        Block(5, 32768, 7232),
        Block(6, 0, 16384),
        Block(6, 16384, 16384),
        Block(6, 32768, 7232),
        Block(1, 0, 16384),
    ]


def test_out_of_range(picker):
    with pytest.raises(IndexError):
        picker.want([5, 10])
    assert not picker.is_wanted(5)


def test_only_what_the_peer_has(picker):
    picker.want([5, 6])
    assert picker.peer_has_wanted_pieces(Bitfield.from_pieces(10, [6]))
    assert not picker.peer_has_wanted_pieces(Bitfield.from_pieces(10, [1]))
    assert picker.pick(Bitfield.from_pieces(10, [6])) == Block(6, 0, 16384)
    assert picker.pick(Bitfield.from_pieces(10, [1])) is None


def test_pieces_we_have_arent_wanted():
    picker = PiecePicker(TEN_PIECES.layout, 10, Bitfield.from_pieces(10, [5]))
    wanted = []
    picker.add_callback(PieceState.OPEN, wanted.append)
    picker.want([5])
    assert wanted == []
    assert picker.piece_state(5) is PieceState.HAVE
    assert picker.n_wanted == 0
    assert picker.pick(Bitfield.from_pieces(10, [5])) is None


def test_unrequested_blocks_are_picked_again(picker):
    """Blocks asked of a peer that choked us or went away are asked of the next peer"""
    picker.want([5, 6])
    has = Bitfield.from_pieces(10, [5, 6])
    assert picker.pick(has) == Block(5, 0, 16384)
    picker.release_blocks([Block(5, 0, 16384)])
    picked = [picker.pick(has) for _ in range(4)]
    assert picked == [
        Block(5, 16384, 16384),
        Block(5, 32768, 7232),
        Block(5, 0, 16384),
        Block(6, 0, 16384),
    ]


def test_states(picker):
    has = Bitfield.from_pieces(10, [5])
    assert picker.piece_state(5) is PieceState.UNWANTED
    picker.want([5])
    assert picker.piece_state(5) is PieceState.OPEN
    picker.pick(has)
    picker.received(Block(5, 0, 16384))
    assert picker.piece_state(5) is PieceState.DOWNLOADING
    for block in [Block(5, 16384, 16384), Block(5, 32768, 7232)]:
        assert picker.pick(has) == block
        picker.received(block)
    assert picker.piece_state(5) is PieceState.CHECKING
    picker.passed(5)
    assert picker.piece_state(5) is PieceState.HAVE
    assert 5 in picker.have
    assert not picker.is_wanted(5)
    assert picker.n_wanted == 0


def test_callbacks_hear_each_state_entered(picker):
    """Including a failed piece opening again"""
    entered: list[tuple[PieceState, list[int]]] = []
    for state in PieceState:
        picker.add_callback(state, lambda pieces, state=state: entered.append((state, pieces)))
    has = Bitfield.from_pieces(10, [5])
    picker.want([5])
    assert entered == [
        (PieceState.OPEN, [5]),
    ]

    while (block := picker.pick(has)) is not None:
        picker.received(block)
    assert entered == [
        (PieceState.OPEN, [5]),
        (PieceState.DOWNLOADING, [5]),
        (PieceState.CHECKING, [5]),
    ]

    picker.failed(5)
    assert entered == [
        (PieceState.OPEN, [5]),
        (PieceState.DOWNLOADING, [5]),
        (PieceState.CHECKING, [5]),
        (PieceState.OPEN, [5]),
    ]

    while (block := picker.pick(has)) is not None:
        picker.received(block)
    assert entered == [
        (PieceState.OPEN, [5]),
        (PieceState.DOWNLOADING, [5]),
        (PieceState.CHECKING, [5]),
        (PieceState.OPEN, [5]),
        (PieceState.DOWNLOADING, [5]),
        (PieceState.CHECKING, [5]),
    ]

    picker.passed(5)
    assert entered == [
        (PieceState.OPEN, [5]),
        (PieceState.DOWNLOADING, [5]),
        (PieceState.CHECKING, [5]),
        (PieceState.OPEN, [5]),
        (PieceState.DOWNLOADING, [5]),
        (PieceState.CHECKING, [5]),
        (PieceState.HAVE, [5]),
    ]


@pytest.mark.validation
def test_wrong_size_and_duplicate_blocks_arent_received(picker):
    picker.want([5])
    picker.pick(Bitfield.from_pieces(10, [5]))
    assert not picker.received(Block(5, 0, 5))
    assert picker.received(Block(5, 0, 16384))
    assert not picker.received(Block(5, 0, 16384))
    assert not picker.received(Block(6, 0, 16384))


def test_failed_piece_keeps_its_place(picker):
    """It's fetched again, after the pieces that were wanted before it"""
    picker.want([5, 6])
    while (block := picker.pick(Bitfield.from_pieces(10, [6]))) is not None:
        picker.received(block)
    picker.failed(6)
    assert picker.piece_state(6) is PieceState.OPEN
    assert picker.is_wanted(6)
    assert picker.pick(Bitfield.from_pieces(10, [5, 6])) == Block(5, 0, 16384)


def test_only_checking_pieces_pass_or_fail(picker):
    picker.want([5])
    with pytest.raises(ValueError):
        picker.passed(5)
    picker.pick(Bitfield.from_pieces(10, [5]))
    with pytest.raises(ValueError):
        picker.failed(5)


async def test_found_on_disk(picker):
    """
    Pieces found on disk are had, wanted or not, and whoever's waiting for them is done.
    A piece that's been started can't be found.
    """
    had = []
    picker.add_callback(PieceState.HAVE, had.extend)
    async with anyio.create_task_group() as tg:
        tg.start_soon(picker.wait, [5])
        await anyio.wait_all_tasks_blocked()
        picker.found([5, 8])
    assert list(picker.have) == [5, 8]
    assert had == [5, 8]
    assert picker.n_wanted == 0
    picker.want([6])
    picker.pick(Bitfield.from_pieces(10, [6]))
    with pytest.raises(ValueError):
        picker.found([6])


async def test_found_doesnt_strand_waiters_for_wanted(picker):
    """Finding pieces when nothing is wanted doesn't lose whoever's waiting for a want"""
    with anyio.fail_after(1):
        async with anyio.create_task_group() as tg:
            tg.start_soon(picker.wait_for_wanted)
            await anyio.wait_all_tasks_blocked()
            picker.found([5])
            picker.want([6])


async def test_wait(picker):
    """Waiters for overlapping pieces are each done once all of theirs are"""
    done = []

    async def wait(name: str, pieces: list[int]) -> None:
        await picker.wait(pieces)
        done.append(name)

    async with anyio.create_task_group() as tg:
        tg.start_soon(wait, "a", [5, 6])
        tg.start_soon(wait, "b", [6, 7])
        await anyio.wait_all_tasks_blocked()
        assert picker.n_wanted == 3
        for index in (6, 5):
            while (block := picker.pick(Bitfield.from_pieces(10, [index]))) is not None:
                picker.received(block)
            picker.passed(index)
            await anyio.wait_all_tasks_blocked()
        assert done == ["a"]
        while (block := picker.pick(Bitfield.from_pieces(10, [7]))) is not None:
            picker.received(block)
        picker.passed(7)
    assert done == ["a", "b"]


async def test_waiting_wants(picker):
    """Waiting for pieces wants them, and they stay wanted after the waiter is cancelled"""
    with anyio.move_on_after(0):
        await picker.wait([5, 6])
    assert picker.is_wanted(5) and picker.is_wanted(6)
    with pytest.raises(IndexError):
        await picker.wait([10])


async def test_wait_for_wanted(picker):
    """Waiting for anything to be wanted returns once something is, until we have all of it"""
    woke = []

    async def wait() -> None:
        await picker.wait_for_wanted()
        woke.append(True)

    async with anyio.create_task_group() as tg:
        tg.start_soon(wait)
        await anyio.wait_all_tasks_blocked()
        assert not woke
        picker.want([5])
    assert woke
    await picker.wait_for_wanted()

    while (block := picker.pick(Bitfield.from_pieces(10, [5]))) is not None:
        picker.received(block)
    picker.passed(5)
    async with anyio.create_task_group() as tg:
        tg.start_soon(wait)
        await anyio.wait_all_tasks_blocked()
        assert len(woke) == 1
        tg.cancel_scope.cancel()


async def test_close_fails_waiters(picker):
    failed = []

    async def wait() -> None:
        with pytest.raises(RuntimeError):
            await picker.wait([5])
        failed.append(True)

    async with anyio.create_task_group() as tg:
        tg.start_soon(wait)
        await anyio.wait_all_tasks_blocked()
        picker.close()
    assert failed
    with pytest.raises(RuntimeError):
        await picker.wait([5])
