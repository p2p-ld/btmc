import io
from types import SimpleNamespace

import pytest
from rich.console import Console

from btmc.cli.progress import TorrentProgress, _BinarySpeedColumn
from btmc.protocol.picker import PieceState

from ..fixtures import PIECE_LENGTH

pytestmark = pytest.mark.libtorrent


async def test_progress(client, seeded):
    """
    A torrent's bar counts the pieces asked for, as they arrive, and its peers,
    with sizes in base 2 units
    """
    console = Console(file=io.StringIO(), record=True, width=200)
    progress = TorrentProgress(console)
    added_callbacks = list(client._callbacks["added"])
    with progress:
        progress.attach(client)
        await client.fetch(seeded, [0, 1])
        [task] = progress._progress.tasks
        assert task.fields["pieces"] == "2/2 pieces"
        assert task.completed == task.total == 2 * PIECE_LENGTH
        assert task.fields["peers"] == len(seeded.swarm.peers) > 0
        console.print(progress._progress.make_tasks_table(progress._progress.tasks))
    kib = 2 * PIECE_LENGTH // 1024
    assert f"{kib}.0/{kib}.0 KiB" in console.export_text()
    # detached, leaving the bar
    assert progress._progress.tasks == [task]
    assert client._callbacks["added"] == added_callbacks
    assert not seeded.swarm._callbacks["connected"]
    assert seeded.picker._callbacks[PieceState.HAVE] == [seeded.swarm._had]


def test_speed_in_base_2_units():
    column = _BinarySpeedColumn()

    def speed(bytes_per_second: float | None) -> str:
        task = SimpleNamespace(finished_speed=None, speed=bytes_per_second)
        return column.render(task).plain  # type: ignore[arg-type]

    assert speed(None) == "?"
    assert speed(512) == "512 bytes/s"
    assert speed(1536) == "1.5 KiB/s"
    assert speed(3 * 1024**2) == "3.0 MiB/s"


async def test_removed_torrents_lose_their_bar(client, one_torrent):
    progress = TorrentProgress(Console(file=io.StringIO()))
    with progress:
        progress.attach(client)
        assert len(progress._progress.tasks) == 1
        await client.remove(one_torrent.name)
        assert progress._progress.tasks == []
