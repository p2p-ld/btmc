"""``btmc zip``: listing and extracting members of a zip in a torrent"""

from pathlib import Path
from typing import Any

import anyio
import click
from rich.console import Console
from torrent_models import Torrent, safe_join

from btmc.cli.common import echo, running_client, torrent_options
from btmc.cli.progress import TorrentProgress
from btmc.files.zip import TorrentZip
from btmc.types import AnyAddr


@click.command("zip")
@torrent_options
@click.option(
    "-f",
    "--file",
    "path",
    help="Path of the zip file within the torrent. Needed if the torrent has more than one file.",
)
@click.option(
    "-x",
    "--extract",
    type=click.Path(file_okay=False, path_type=Path),
    help="Directory to extract MEMBERS into",
)
@click.argument("members", nargs=-1)
@click.pass_obj
def zip_(
    console: Console,
    torrent: Torrent[Any],
    dest: Path,
    peer: list[AnyAddr],
    listen: int | None,
    no_trackers: bool,
    path: str | None,
    extract: Path | None,
    members: tuple[str, ...],
) -> None:
    """
    List the members of a zip file in TORRENT, or extract some of them,
    fetching only the pieces they're in. Pieces are stored in DEST.
    """
    zip_path = path if path is not None else _only_file(torrent)

    async def run() -> None:
        # around the whole client, so the bars stay below what's logged while it starts and stops
        with TorrentProgress(console) as progress:
            async with running_client(torrent, dest, peer, listen, no_trackers) as (client, t):
                progress.attach(client)
                tzip = TorrentZip(client, t, zip_path)
                zf = await tzip.open()
                if not members:
                    for info in zf.infolist():
                        echo(f"{info.file_size:>14,}  {info.filename}")
                for name in members:
                    data = await tzip.read(name)
                    if extract is None:
                        echo(f"{len(data):>14,}  {name}")
                    else:
                        out = safe_join(extract, name)
                        out.parent.mkdir(parents=True, exist_ok=True)
                        out.write_bytes(data)
                        console.print(f"extracted {name} to {out}", markup=False)
                # stopping removes the torrent, which would take its bar away
                progress.detach()
        console.print(
            f"fetched {t.have.count()} of {t.n_pieces} pieces, {t.downloaded:,} bytes",
            markup=False,
        )

    anyio.run(run)


def _only_file(torrent: Torrent[Any]) -> str:
    """The torrent's file, when it only has one, for when the zip isn't named"""
    files = torrent.files
    if len(files) != 1:
        raise click.UsageError(f"the torrent has {len(files)} files, so pick one with --file")
    return files[0].path
