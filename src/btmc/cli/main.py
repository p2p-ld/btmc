"""The ``btmc`` command, which every other command is under"""

import logging

import click
from rich.console import Console
from rich.logging import RichHandler

from btmc.cli.fetch import fetch
from btmc.cli.mount import mount
from btmc.cli.zip import zip_


@click.group("btmc")
@click.option(
    "-v",
    "--verbose",
    count=True,
    help="Log more: -v for HTTP tracker requests and the like, -vv for announce results and peers",
)
@click.pass_context
def main(ctx: click.Context, verbose: int) -> None:
    """
    btmc - bittorrent microclient

    Settings are read from btmc.toml in the current directory, if there is one.
    """
    # progress, logs and status go to stderr, so what commands print to stdout can be piped
    console = Console(stderr=True)
    levels = [logging.WARNING, logging.INFO, logging.DEBUG]
    logging.basicConfig(
        level=levels[min(verbose, len(levels) - 1)],
        format="%(message)s",
        handlers=[RichHandler(console=console, show_path=False)],
    )
    ctx.obj = console


main.add_command(fetch)
main.add_command(zip_)
main.add_command(mount)
