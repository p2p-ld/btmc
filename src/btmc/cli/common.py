"""What the commands that work on one torrent have in common"""

import contextlib
from collections.abc import AsyncGenerator, Callable
from pathlib import Path
from typing import Any

import click
from torrent_models import Torrent

from btmc.config import Config
from btmc.protocol.client import Client
from btmc.protocol.torrent import TorrentState
from btmc.types import Addr, parse_addr


def echo(line: str) -> None:
    """
    Print a line of a command's output to stdout. Not with ``click.echo``,
    which writes past the progress bars' redirect of stdout and ends up on top of them.
    """
    print(line)


def _read_torrent(ctx: click.Context, param: click.Parameter, value: Path) -> Torrent[Any]:
    try:
        return Torrent.read(value)
    except Exception as e:
        raise click.BadParameter(f"can't read {value}: {e}") from e


def _parse_addrs(ctx: click.Context, param: click.Parameter, values: tuple[str, ...]) -> list[Addr]:
    try:
        return [parse_addr(value) for value in values]
    except ValueError as e:
        raise click.BadParameter(str(e)) from e


torrent_argument = click.argument(
    "torrent", type=click.Path(exists=True, dir_okay=False, path_type=Path), callback=_read_torrent
)
"""The .torrent file to fetch from, read into a :class:`.Torrent`"""


def torrent_options(fn: Callable[..., Any]) -> Callable[..., Any]:
    """The arguments and options for fetching from one torrent, see :func:`.running_client`"""
    fn = client_options(fn)
    fn = click.argument("dest", type=click.Path(file_okay=False, path_type=Path))(fn)
    return torrent_argument(fn)


def client_options(fn: Callable[..., Any]) -> Callable[..., Any]:
    """The options for how the client finds peers, see :func:`.running_client`"""
    options = [
        click.option(
            "--peer",
            multiple=True,
            callback=_parse_addrs,
            help="HOST:PORT of a peer to connect to. Can be used multiple times.",
        ),
        click.option("--listen", type=int, help="Port to take incoming connections on"),
        click.option(
            "--no-trackers", is_flag=True, help="Don't announce to the torrent's trackers"
        ),
    ]
    for option in reversed(options):
        fn = option(fn)
    return fn


@contextlib.asynccontextmanager
async def running_client(
    torrent: Torrent[Any], dest: Path, peer: list[Addr], listen: int | None, no_trackers: bool
) -> AsyncGenerator[tuple[Client, TorrentState]]:
    """A running client, with the torrent added, from :func:`.torrent_options`"""
    async with Client(config=client_config(listen)) as client:
        t = client.add(torrent, dest, known_peers=peer, trackers=[] if no_trackers else None)
        yield client, t


def client_config(listen: int | None) -> Config:
    """Settings from btmc.toml, listening on all interfaces on the ``--listen`` port if given"""
    if listen is None:
        return Config.from_toml()
    return Config.from_toml(listen_host="0.0.0.0", port=listen)
