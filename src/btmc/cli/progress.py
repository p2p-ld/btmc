"""Progress bars for the torrents a client is fetching"""

import sys
from typing import Self

from rich import filesize
from rich.console import Console
from rich.progress import (
    BarColumn,
    DownloadColumn,
    Progress,
    ProgressColumn,
    Task,
    TaskID,
    TaskProgressColumn,
    TextColumn,
    TimeRemainingColumn,
)
from rich.text import Text

from btmc.protocol.client import Client
from btmc.protocol.peer import Peer
from btmc.protocol.picker import PieceState
from btmc.protocol.torrent import TorrentState
from btmc.types import PieceIndex

NAME_WIDTH = 30
BINARY_UNITS = ["bytes", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB", "ZiB", "YiB"]
"""Base 2 sizes, like :class:`.DownloadColumn` does"""


class _BinarySpeedColumn(ProgressColumn):
    """Download speed in base 2 units, to match the sizes"""

    def render(self, task: Task) -> Text:
        speed = task.finished_speed or task.speed
        if speed is None:
            return Text("?", style="progress.data.speed")
        unit, suffix = filesize.pick_unit_and_suffix(int(speed), BINARY_UNITS, 1024)
        precision = 0 if unit == 1 else 1
        return Text(f"{speed / unit:,.{precision}f} {suffix}/s", style="progress.data.speed")


class TorrentProgress:
    """
    A progress bar for each torrent in a client: how many of the pieces asked for have
    arrived, and how many bytes, how fast they're arriving, how long until the rest do,
    and how many peers the torrent is connected to.

    Show the bars with ``with``, and :meth:`.attach` them to a client to follow its torrents
    as they're added and removed. Only pieces asked for after attaching are counted.
    Leaving the ``with`` block detaches, and leaves the bars on screen as they were.

    Args:
        console: where to draw the bars
    """

    def __init__(self, console: Console | None = None):
        self._progress = Progress(
            TextColumn("{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            TextColumn("{task.fields[pieces]}"),
            DownloadColumn(binary_units=True),
            _BinarySpeedColumn(),
            TextColumn("{task.fields[peers]} peers"),
            TimeRemainingColumn(),
            console=console,
            # shown above the bars when it's on the same terminal, and left alone when piped
            redirect_stdout=sys.stdout.isatty(),
        )
        self._client: Client | None = None
        self._bars: dict[TorrentState, _TorrentBar] = {}

    def __enter__(self) -> Self:
        self._progress.start()
        return self

    def __exit__(self, *exc: object) -> None:
        self.detach()
        self._progress.stop()

    def attach(self, client: Client) -> None:
        """
        Show a bar for each of the client's torrents, now and as they're added.
        Raises RuntimeError if it's attached to a client already.
        """
        if self._client is not None:
            raise RuntimeError("already attached to a client")
        self._client = client
        client.add_callback("added", self._added)
        client.add_callback("removed", self._removed)
        for t in client.torrents:
            self._added(t)

    def detach(self) -> None:
        """Stop following the client's torrents, leaving their bars as they are"""
        if self._client is None:
            return
        self._client.remove_callback("added", self._added)
        self._client.remove_callback("removed", self._removed)
        for bar in self._bars.values():
            bar.close()
        self._bars.clear()
        self._client = None

    def _added(self, t: TorrentState) -> None:
        self._bars[t] = _TorrentBar(self._progress, t)

    def _removed(self, t: TorrentState) -> None:
        bar = self._bars.pop(t)
        bar.close()
        self._progress.remove_task(bar.task)


class _TorrentBar:
    """One torrent's bar, kept up to date by the torrent's events until it's closed"""

    def __init__(self, progress: Progress, t: TorrentState):
        self._progress = progress
        self._t = t
        self._requested: set[PieceIndex] = set()
        self._requested_bytes = 0
        self._arrived = 0
        self._arrived_bytes = 0
        self._peers = 0
        name = t.torrent.info.name or t.name
        if len(name) > NAME_WIDTH:
            name = name[: NAME_WIDTH - 1] + "…"
        self.task: TaskID = progress.add_task(name, total=None, pieces=self._pieces(), peers=0)
        t.picker.add_callback(PieceState.OPEN, self._opened)
        t.picker.add_callback(PieceState.HAVE, self._had)
        t.swarm.add_callback("connected", self._connected)
        t.swarm.add_callback("disconnected", self._disconnected)

    def close(self) -> None:
        """Stop following the torrent"""
        self._t.picker.remove_callback(PieceState.OPEN, self._opened)
        self._t.picker.remove_callback(PieceState.HAVE, self._had)
        self._t.swarm.remove_callback("connected", self._connected)
        self._t.swarm.remove_callback("disconnected", self._disconnected)

    def _opened(self, pieces: list[PieceIndex]) -> None:
        for index in pieces:
            self._request(index)
        self._update()

    def _had(self, pieces: list[PieceIndex]) -> None:
        for index in pieces:
            # asked for before we were attached, or found on disk
            self._request(index)
            self._arrived += 1
            self._arrived_bytes += self._size(index)
        self._update()

    def _request(self, index: PieceIndex) -> None:
        # pieces that fail their check open again, and are only counted once
        if index not in self._requested:
            self._requested.add(index)
            self._requested_bytes += self._size(index)

    def _size(self, index: PieceIndex) -> int:
        """Bytes of the piece that are downloaded, which leaves out padding"""
        piece = self._t.layout.piece_range(index)
        return piece.size - piece.padding

    def _connected(self, peer: Peer) -> None:
        self._peers += 1
        self._progress.update(self.task, peers=self._peers)

    def _disconnected(self, peer: Peer) -> None:
        self._peers -= 1
        self._progress.update(self.task, peers=self._peers)

    def _pieces(self) -> str:
        return f"{self._arrived}/{len(self._requested)} pieces"

    def _update(self) -> None:
        self._progress.update(
            self.task,
            total=self._requested_bytes,
            completed=self._arrived_bytes,
            pieces=self._pieces(),
        )
