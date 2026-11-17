import contextlib
import ipaddress
import re
import socket
from collections.abc import Sequence
from dataclasses import dataclass
from ipaddress import IPv4Address, IPv6Address
from typing import Any, NamedTuple

import idna

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


_NUMERIC_IPV4 = re.compile(r"[0-9a-fA-FxX.]+")
"""The characters of IPv4 addresses in the older ways of writing them, like ``127.1``"""


@dataclass(frozen=True, slots=True, order=True)
class Addr:
    """
    A host and port, checked and written one way when it's made,
    so that two ways of writing the same address are equal. Raises ValueError if it's invalid.

    - The port is 1 to 65535.
    - IP addresses are normalized to the common ipv4/v6 notation
    - Host names have to be valid. They're lowercased and lose any dot at the end,
      and international ones are written in their ASCII form (IDNA), like ``xn--trackr-fva.com``.
    """

    host: str
    port: int

    def __post_init__(self) -> None:
        if not 0 < self.port < 65536:
            raise ValueError(f"port {self.port} is out of range")
        object.__setattr__(self, "host", _normal_host(self.host))

    def __str__(self) -> str:
        """``host:port``, or ``[host]:port`` for IPv6, as :func:`.parse_addr` reads it"""
        return f"[{self.host}]:{self.port}" if ":" in self.host else f"{self.host}:{self.port}"


def _normal_host(host: str) -> str:
    """See :class:`.Addr`"""
    ip: IPv4Address | IPv6Address | None = None
    with contextlib.suppress(ValueError):
        ip = ipaddress.ip_address(host)
    # older ways of writing IPv4 that ip_address refuses but the system reads, like 127.1.
    # inet_aton ignores anything after a space, so only those characters go to it
    if ip is None and _NUMERIC_IPV4.fullmatch(host):
        with contextlib.suppress(OSError):
            ip = IPv4Address(socket.inet_aton(host))
    # a peer that reached a dual-stack socket over IPv4
    if isinstance(ip, IPv6Address) and ip.ipv4_mapped is not None:
        ip = ip.ipv4_mapped
    if ip is not None:
        return str(ip)
    try:
        return idna.encode(host.removesuffix("."), uts46=True).decode()
    except idna.IDNAError as e:
        raise ValueError(f"{host!r} isn't an IP address or host name: {e}") from None


def parse_addr(value: Addr | str | Sequence[Any]) -> Addr:
    """
    An address from ``"host:port"`` (``"[::1]:port"`` for IPv6) or a ``(host, port)`` pair.
    An :class:`.Addr` is returned as it is.
    """
    if isinstance(value, Addr):
        return value
    if isinstance(value, str):
        host, _, port = value.rpartition(":")
        host = host.strip("[]")
    else:
        host, port = (str(part) for part in value)
    if not host or not port.isdigit():
        raise ValueError(f"{value} isn't a host and port")
    return Addr(host, int(port))
