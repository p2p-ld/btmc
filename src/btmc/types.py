from collections.abc import Sequence
from typing import Any, NamedTuple

type PieceIndex = int
"""Which piece of a torrent, counting from 0"""

type BlockOffset = int
"""Where a block starts within its piece, in bytes. The protocol calls it ``begin``."""

type BlockLength = int
"""The number of bytes in a block"""


class Addr(NamedTuple):
    host: str
    port: int


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
