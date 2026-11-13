"""The torrent's own work: asking for pieces, and checking and storing them"""

import contextlib
import hashlib
import os
import shutil
from collections.abc import AsyncGenerator, Callable, Iterator, Sequence
from pathlib import Path

import anyio
import pytest
from torrent_models import Torrent
from torrent_models.types import FileItem
from torrent_models.types.v2 import V2PieceRange

from btmc import Config, TorrentState
from btmc.protocol.messages import Bitfield
from btmc.protocol.picker import PieceState
from btmc.protocol.shared import Shared
from btmc.protocol.torrent import ReceivedPiece, SmartBan
from btmc.types import Addr, Block
from btmc.utils import torrent_name

pytestmark = pytest.mark.libtorrent

OUR_ID = b"A" * 20
PEER = Addr("10.0.0.1", 1)
OTHER = Addr("10.0.0.2", 1)
B = 16384
"""Block size. The test torrents' pieces are two blocks."""


type MakeState = Callable[..., TorrentState]


@pytest.fixture
def make_state(tmp_path: Path) -> Iterator[MakeState]:
    """
    Makes torrents in ``tmp_path``. Running one closes its check queue when it stops,
    and the ones that never ran are closed after the test.
    """
    made: list[TorrentState] = []

    def make(torrent: Torrent, config: Config | None = None) -> TorrentState:
        shared = Shared(config or Config(), OUR_ID)
        t = TorrentState(torrent, tmp_path / torrent_name(torrent), shared, trackers=[])
        made.append(t)
        return t

    yield make
    for t in made:
        t._to_check.close()
        t._checks.close()


@contextlib.asynccontextmanager
async def running(t: TorrentState) -> AsyncGenerator[None]:
    """Run the torrent for the body of the block, then stop it"""
    with anyio.fail_after(5):
        async with anyio.create_task_group() as tg:
            tg.start_soon(t.run)
            yield
            await t.stop()


def arrive(t: TorrentState, index: int, data: bytes, senders: Sequence[Addr] = (PEER,)) -> None:
    """The blocks of a piece arrive, as if ``data`` were sent by ``senders``, a block each"""
    has = Bitfield.full(t.n_pieces)
    for i in range(len(data) // B):
        block = t.picker.pick(has)
        assert block is not None and block.piece == index
        sender = senders[i % len(senders)]
        t._block_arrived(block, data[block.begin : block.begin + block.length], sender)
    assert t.picker.piece_state(index) is PieceState.CHECKING


def picked_pieces(t: TorrentState, n: int) -> list[int]:
    has = Bitfield.full(t.n_pieces)
    return [block.piece for block in (t.picker.pick(has) for _ in range(n)) if block]


async def test_wanted_in_the_order_asked(make_state, torrent):
    """
    Pieces are fetched in the order they were first asked for,
    and stay wanted when the callers waiting for them are cancelled
    """
    t = make_state(torrent)
    async with anyio.create_task_group() as tg:
        tg.start_soon(t.fetch, [5, 6])
        tg.start_soon(t.fetch, [2, 1, 5])
        await anyio.wait_all_tasks_blocked()
        tg.cancel_scope.cancel()
    assert t.picker.n_wanted == 4
    assert picked_pieces(t, 8) == [5, 5, 6, 6, 2, 2, 1, 1]


async def test_want_everything_by_default(make_state, torrent):
    """Wanting without saying which pieces wants all of them, and an empty list wants none"""
    t = make_state(torrent)
    t.want([])
    assert t.picker.n_wanted == 0
    t.want()
    assert t.picker.n_wanted == t.n_pieces


async def test_good_piece(make_state, torrent, test_data):
    """A complete piece is checked and written, and whoever was waiting for it is done"""
    t = make_state(torrent)
    async with running(t):
        t.want([5])
        arrive(t, 5, torrent.layout.piece_range(5).read(test_data))
        await t.fetch([5])
    assert list(t.picker.have) == [5]
    assert t.downloaded == 2 * B
    assert t.picker.n_wanted == 0
    assert list(make_state(torrent).picker.have) == [5]


async def test_checks_wait_for_a_hashing_thread(make_state, torrent, test_data):
    """
    Checks take one of the hashing threads shared by every torrent,
    and pieces whose bits share a byte of the resume file are both saved
    """
    t = make_state(torrent, Config(hash_threads=1))
    async with running(t):
        t.want([5, 6])
        async with t.shared.hashing:
            arrive(t, 5, torrent.layout.piece_range(5).read(test_data))
            arrive(t, 6, torrent.layout.piece_range(6).read(test_data))
            await anyio.wait_all_tasks_blocked()
            assert t.picker.piece_state(5) is PieceState.CHECKING
            assert t.picker.piece_state(6) is PieceState.CHECKING
        await t.fetch([5, 6])
    assert list(make_state(torrent).picker.have) == [5, 6]


async def test_data_on_disk_is_checked(make_state, torrent, test_data):
    """
    Without a resume file, the data already on disk is checked, like files from another
    client, so the good pieces aren't fetched, and they start the resume file
    """
    t = make_state(torrent)
    shutil.copytree(test_data, t.store.root, dirs_exist_ok=True)
    piece = t.layout.piece_range(5)
    first = piece if isinstance(piece, V2PieceRange) else piece.ranges[0]
    with t.store.path(first.full_path).open("r+b") as f:
        f.seek(first.range_start)
        f.write(b"x")
    await t._check_disk()
    assert t.have.count() == t.n_pieces - 1
    assert t.picker.n_wanted == 0
    t.want()
    assert t.picker.n_wanted == 1
    assert 5 not in t.have
    assert t.downloaded == 0
    assert t.store.load_have() == t.have


async def test_resume_file_is_trusted(make_state, torrent, test_data):
    """With a resume file, what's on disk isn't checked"""
    t = make_state(torrent)
    t.store.create_resume(Bitfield(t.n_pieces))
    shutil.copytree(test_data, t.store.root, dirs_exist_ok=True)
    t = make_state(torrent)
    async with running(t):
        await anyio.wait_all_tasks_blocked()
    assert t.have.count() == 0


async def test_padding_isnt_fetched(make_state):
    """
    Padfiles at the end of a piece aren't asked for, and the piece still passes its check,
    without the padding being written. A piece that's all padding passes with nothing fetched.
    """
    a, b = os.urandom(B + 100), os.urandom(B)
    pad = 2 * B - len(a)
    data = a + bytes(pad) + bytes(2 * B) + b
    files = [
        FileItem(path=["a"], length=len(a)),
        FileItem.padfile(pad),
        FileItem.padfile(2 * B),
        FileItem(path=["b"], length=len(b)),
    ]
    hashes = [hashlib.sha1(data[i : i + 2 * B]).digest() for i in range(0, len(data), 2 * B)]
    info = {"name": "t", "piece length": 2 * B, "pieces": b"".join(hashes), "files": files}
    t = make_state(Torrent.model_validate({"info": info}))
    async with running(t):
        t.want([0, 1])
        has = Bitfield.full(t.n_pieces)
        blocks = [Block(0, 0, B), Block(0, B, 100)]
        assert [t.picker.pick(has) for _ in range(3)] == [*blocks, None]
        for block in blocks:
            t._block_arrived(block, data[block.begin : block.begin + block.length], PEER)
        await t.fetch([0, 1])
    assert t.downloaded == B + 100
    assert t.store.path("t/a").read_bytes() == a
    assert not t.store.path("t/.pad").exists()


async def test_bad_piece_from_one_peer(make_state, torrent):
    """The peer is banned for every torrent, and the piece is open to fetch again"""
    t = make_state(torrent)
    t.want([5])
    arrive(t, 5, bytes(2 * B))
    await t._check(ReceivedPiece(5, t._blocks.pop(5)))
    assert t.hash_failures == 1
    assert t.shared.is_banned(PEER)
    assert t.picker.piece_state(5) is PieceState.OPEN


async def test_bad_block_found_when_the_piece_passes(make_state, torrent, test_data):
    """
    When a piece from two peers fails, nobody is banned yet. Once it passes,
    the peer whose block was different is.
    """
    good = torrent.layout.piece_range(5).read(test_data)
    t = make_state(torrent)
    # running would make it, and passing pieces are saved in it
    t.store.create_resume(t.have)
    t.want([5])
    arrive(t, 5, bytes(B) + good[B:], senders=[PEER, OTHER])
    await t._check(ReceivedPiece(5, t._blocks.pop(5)))
    assert t.hash_failures == 1
    assert not t.shared.is_banned(PEER) and not t.shared.is_banned(OTHER)
    arrive(t, 5, good, senders=[OTHER])
    await t._check(ReceivedPiece(5, t._blocks.pop(5)))
    assert 5 in t.have
    assert t.shared.is_banned(PEER)
    assert not t.shared.is_banned(OTHER)


# ---- smart ban ----

GOOD = b"g" * B
BAD = b"x" * B


def received(*blocks: tuple[bytes, Addr]) -> ReceivedPiece:
    """Piece 5, made of these blocks, in order"""
    return ReceivedPiece(5, {i * B: block for i, block in enumerate(blocks)})


def test_smart_ban_one_sender():
    ban = SmartBan()
    assert ban.checked(received((BAD, PEER), (GOOD, PEER)), ok=False) == {PEER}


def test_smart_ban_on_pass():
    """A peer whose block differs from the one in the piece that passed is to blame"""
    ban = SmartBan()
    assert ban.checked(received((BAD, PEER), (GOOD, OTHER)), ok=False) == set()
    assert ban.checked(received((GOOD, OTHER), (GOOD, OTHER)), ok=True) == {PEER}
    # and it's forgotten once the piece passes
    assert ban.checked(received((GOOD, OTHER), (GOOD, OTHER)), ok=True) == set()


def test_smart_ban_same_block_changed():
    """A peer that sends the same block with different data is to blame"""
    ban = SmartBan()
    assert ban.checked(received((BAD, PEER), (GOOD, OTHER)), ok=False) == set()
    assert ban.checked(received((b"y" * B, PEER), (GOOD, OTHER)), ok=False) == {PEER}
