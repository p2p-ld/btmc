"""``btmc mount``: a torrent as a directory"""

import dataclasses
import glob
import shutil
from collections.abc import Callable
from pathlib import Path
from typing import Any

import click
from rich.console import Console
from torrent_models import Torrent

from btmc.cli.common import client_config, client_options, torrent_argument
from btmc.cli.progress import TorrentProgress
from btmc.types import AnyAddr
from btmc.utils import torrent_name


@click.command("mount")
@torrent_argument
@click.argument("mountpoint", type=click.Path(exists=True, file_okay=False, path_type=Path))
@click.option(
    "--mount-zip",
    "zips",
    multiple=True,
    metavar="PATH",
    help="A file in the torrent to show as a zip, like a .wacz. Can be used multiple times.",
)
@click.option(
    "--download-dir",
    type=click.Path(file_okay=False, path_type=Path),
    help=(
        "Where to keep fetched pieces, in a directory for each torrent. "
        "Defaults to download_dir in btmc.toml, or the user cache directory."
    ),
)
@click.option(
    "--fetch",
    # FetchMode's values, which can't be imported without the mount extra
    type=click.Choice(["as-read", "whole-file"]),
    default="as-read",
    show_default=True,
    help="Fetch an opened file's pieces as they're read, or all of them as soon as it's opened",
)
@click.option(
    "--clear-cache",
    is_flag=True,
    help="Delete the pieces already fetched for this torrent, and start from nothing",
)
@client_options
@click.pass_obj
def mount(
    console: Console,
    torrent: Torrent[Any],
    mountpoint: Path,
    zips: tuple[str, ...],
    download_dir: Path | None,
    fetch: str,
    clear_cache: bool,
    peer: list[AnyAddr],
    listen: int | None,
    no_trackers: bool,
) -> None:
    """
    Mount TORRENT as a read-only directory at MOUNTPOINT, until Ctrl-C.

    Files are fetched a piece at a time as they're read, or with --fetch whole-file,
    all at once as soon as they're opened.
    Zip files are also shown as a directory next to them, named with .d on the end,
    with what's in them in it. Files ending in .zip are shown like this,
    and so are any given with --mount-zip.

    On macOS, a file's Get Info in Finder has a tag for whether it's on disk.

    Needs the mount extra and FUSE: on macOS, macFUSE 5.2 or newer, with its file system
    extension switched on in System Settings, under General > Login Items & Extensions.
    """
    try:
        from fsspec.asyn import sync

        from btmc.fsspec.filesystem import FetchMode, TorrentFileSystem
        from btmc.fsspec.fuse import mount as fuse_mount
    except (ImportError, OSError) as e:
        raise click.ClickException(f"can't mount without FUSE and the mount extra: {e}") from e
    if missing := set(zips) - {f.path for f in torrent.files}:
        raise click.BadParameter(
            f"not files in the torrent: {sorted(missing)}", param_hint="--mount-zip"
        )

    config = client_config(listen)
    if download_dir is not None:
        config = dataclasses.replace(config, download_dir=download_dir.expanduser())
    dest = config.download_dir / torrent_name(torrent)
    if clear_cache:
        # the torrent's resume files too, including ones for other directories,
        # which only means those get checked again
        for path in (dest, config.state_dir / "resume" / torrent_name(torrent)):
            if path.exists():
                shutil.rmtree(path)
                console.print(f"cleared {path}", markup=False)
    fs = TorrentFileSystem(
        torrents=[torrent],
        zips=["*.zip", *(glob.escape(path) for path in zips)],
        trackers=[] if no_trackers else None,
        peers=peer,
        config=config,
        # this command closes it, so it can't be one fsspec handed out before
        skip_instance_cache=True,
    )
    # around the whole client, so the bars stay below what's logged while it starts and stops
    with TorrentProgress(console) as progress:
        try:
            client = sync(fs.loop, fs.get_client)
            sync(fs.loop, _on_loop, progress.attach, client)
            # so it starts finding peers before anything's read
            sync(fs.loop, fs.torrent_state, torrent)
            try:
                root = torrent_name(torrent)
                fuse_mount(fs, root, mountpoint, torrent.info.name, FetchMode(fetch))
            except RuntimeError as e:
                # FUSE prints why before failing
                raise click.ClickException(f"couldn't mount at {mountpoint}") from e
            finally:
                # stopping removes the torrent, which would take its bar away
                sync(fs.loop, _on_loop, progress.detach)
        finally:
            fs.close()


async def _on_loop(fn: Callable[..., None], *args: Any) -> None:
    """Call ``fn`` on the event loop's thread, through :func:`fsspec.asyn.sync`"""
    fn(*args)
