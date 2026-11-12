"""``btmc fetch``: fetching pieces of a torrent"""

from pathlib import Path
from typing import Any

import anyio
import click
from rich.console import Console
from torrent_models import Torrent

from btmc.cli.common import running_client, torrent_options
from btmc.cli.progress import TorrentProgress
from btmc.types import Addr


@click.command("fetch")
@torrent_options
@click.argument("pieces", type=int, nargs=-1)
@click.pass_obj
def fetch(
    console: Console,
    torrent: Torrent[Any],
    dest: Path,
    peer: list[Addr],
    listen: int | None,
    no_trackers: bool,
    pieces: tuple[int, ...],
) -> None:
    """Fetch pieces of TORRENT into DEST, by their index, or all of them without PIECES"""
    n_pieces = torrent.layout.n_pieces
    for index in pieces:
        if not 0 <= index < n_pieces:
            raise click.BadParameter(
                f"there's no piece {index}, the torrent has {n_pieces}", param_hint="PIECES"
            )

    async def run() -> None:
        # around the whole client, so the bars stay below what's logged while it starts and stops
        with TorrentProgress(console) as progress:
            async with running_client(torrent, dest, peer, listen, no_trackers) as (client, t):
                progress.attach(client)
                await client.fetch(t, pieces or None)
                # stopping removes the torrent, which would take its bar away
                progress.detach()
        console.print(f"downloaded {t.downloaded:,} bytes, {t.hash_failures} hash failures")

    anyio.run(run)
