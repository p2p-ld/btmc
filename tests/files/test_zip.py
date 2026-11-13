import zipfile
from types import SimpleNamespace

import anyio
import pytest

from btmc import TorrentZip

from ..fixtures import ZIP_MEMBERS

pytestmark = pytest.mark.libtorrent


async def test_zip_members(client, seeded, test_data):
    """We can list and read the members of a zip in a torrent"""
    tzip = TorrentZip(client, seeded, "archive.zip")
    assert sorted(await tzip.namelist()) == sorted(ZIP_MEMBERS)
    with zipfile.ZipFile(test_data / "testdata" / "archive.zip") as source:
        for name in ZIP_MEMBERS:
            assert await tzip.read(name) == source.read(name), name


async def test_zip_lines(client, seeded, test_data):
    """We can read the lines of a member of a zip in a torrent in batches"""
    tzip = TorrentZip(client, seeded, "archive.zip")
    batches = [batch async for batch in tzip.lines("deflated.txt", 7)]
    with zipfile.ZipFile(test_data / "testdata" / "archive.zip") as source:
        expected = source.read("deflated.txt").splitlines(keepends=True)
    assert all(len(batch) == 7 for batch in batches[:-1])
    assert [line for batch in batches for line in batch] == expected


async def test_zip_cancel_while_fetching(client, one_torrent):
    """Cancelling a read that's waiting in a worker thread for pieces that never come stops it"""
    tzip = TorrentZip(client, one_torrent, "archive.zip")
    with anyio.fail_after(5):
        with anyio.move_on_after(0.2) as scope:
            await tzip.namelist()
    assert scope.cancelled_caught
    assert one_torrent.have.count() == 0


async def test_zip_fetches_only_what_it_needs(client, seeded):
    """Reading a small member only fetches the zip's index and the member's pieces"""
    tzip = TorrentZip(client, seeded, "archive.zip")
    await tzip.read("dir/small.txt")
    length = seeded.layout.file("archive.zip").length
    zip_pieces = seeded.layout.byte_range_pieces("archive.zip", 0, length)
    # 64 KiB of zip tail is 3-4 pieces, and the member is 1-2
    assert seeded.have.count() <= 6
    # the stored member alone is 40 pieces
    assert len(zip_pieces) > 40


class _HeldClient:
    """Reads a zip from memory, holding back reads that touch one byte range until let go"""

    def __init__(self, data: bytes, held: tuple[int, int]):
        self.data = data
        self.held = held
        self.holding = anyio.Event()
        self.let_go = anyio.Event()

    async def read(self, t, path: str, start: int, end: int) -> bytes:
        if start < self.held[1] and end > self.held[0]:
            self.holding.set()
            await self.let_go.wait()
        return self.data[start:end]


async def test_zip_members_read_independently(torrent, test_data):
    """Reading one member doesn't wait while another member's read waits for its pieces"""
    path = test_data / "testdata" / "archive.zip"
    with zipfile.ZipFile(path) as source:
        stored = source.getinfo("stored.bin")
        expected = source.read("dir/small.txt")
    held = (stored.header_offset + 100, stored.header_offset + 200)
    client = _HeldClient(path.read_bytes(), held)
    t = SimpleNamespace(layout=torrent.layout)
    tzip = TorrentZip(client, t, "archive.zip")  # type: ignore[arg-type]

    waiting = await tzip.open_member("stored.bin")
    other = await tzip.open_member("dir/small.txt")
    try:
        async with anyio.create_task_group() as tg:
            tg.start_soon(waiting.read, 0, 1000)
            await client.holding.wait()
            with anyio.fail_after(2):
                assert await other.read(0, 1000) == expected
            client.let_go.set()
    finally:
        waiting.close()
        other.close()
