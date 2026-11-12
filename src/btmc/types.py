from collections.abc import Sequence
from typing import Any, NamedTuple

type PieceIndex = int
"""Which piece of a torrent, counting from 0"""

type BlockOffset = int
"""Where a block starts within its piece, in bytes. The protocol calls it ``begin``."""

type BlockLength = int
"""The number of bytes in a block"""


class Block(NamedTuple):
    """
    A block of a piece, the unit we ask peers for.
    The fields are in the same order as the ``request`` message's.
    """

    piece: PieceIndex
    begin: BlockOffset
    length: BlockLength


class Addr(NamedTuple):
    host: str
    port: int

    def __str__(self) -> str:
        """``host:port``, or ``[host]:port`` for IPv6, as :func:`.parse_addr` reads it"""
        return f"[{self.host}]:{self.port}" if ":" in self.host else f"{self.host}:{self.port}"


def parse_addr(value: str | Sequence[Any]) -> Addr:
    """An address from ``"host:port"`` (``"[::1]:port"`` for IPv6) or a ``(host, port)`` pair"""
    if isinstance(value, str):
        host, _, port = value.rpartition(":")
        host = host.strip("[]")
    else:
        host, port = (str(part) for part in value)
    if not host or not port.isdigit():
        raise ValueError(f"{value} isn't a host and port")
    return Addr(host, int(port))
