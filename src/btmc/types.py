import contextlib
import ipaddress
import re
import socket
from collections.abc import Sequence
from dataclasses import dataclass
from ipaddress import IPv4Address, IPv6Address
from typing import Any, NamedTuple, Self

import anyio
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


type IP = IPv4Address | IPv6Address


@dataclass(frozen=True, slots=True)
class Addr:
    """
    An IP address and port, where a peer can be reached. Raises ValueError if it's invalid.

    - The port is 1 to 65535.
    - IPv4 mapped into IPv6 is kept as IPv4, so the same peer has one address.

    Make one from text with :meth:`.parse`, or from a host name with :meth:`.DomainAddr.resolve`.
    """

    ip: IP
    port: int

    def __post_init__(self) -> None:
        if not isinstance(self.ip, IPv4Address | IPv6Address):
            raise TypeError(f"{self.ip!r} isn't an IP address, use Addr.parse for text")
        _check_port(self.port)
        # a peer that reached a dual-stack socket over IPv4
        if isinstance(self.ip, IPv6Address) and self.ip.ipv4_mapped is not None:
            object.__setattr__(self, "ip", self.ip.ipv4_mapped)

    @classmethod
    def parse(cls, host: str, port: int) -> Self:
        """
        An address from an IP address written as text, including the older ways of writing
        IPv4 that the system reads, like ``127.1``. Raises ValueError if it isn't an IP address.
        """
        ip = _parse_ip(host)
        if ip is None:
            raise ValueError(f"{host!r} isn't an IP address")
        return cls(ip, port)

    def __str__(self) -> str:
        """``ip:port``, or ``[ip]:port`` for IPv6, as :func:`.parse_addr` reads it"""
        return f"[{self.ip}]:{self.port}" if self.ip.version == 6 else f"{self.ip}:{self.port}"


@dataclass(frozen=True, slots=True)
class DomainAddr:
    """
    A host name and port, which has to be looked up with :meth:`.resolve` to connect to it.
    Raises ValueError if it's invalid, or if the name is an IP address, which is an :class:`.Addr`.

    - The port is 1 to 65535.
    - Names are lowercased and lose any dot at the end,
      and international ones are written in their ASCII form (IDNA), like ``xn--trackr-fva.com``.
    """

    name: str
    port: int

    def __post_init__(self) -> None:
        _check_port(self.port)
        if _parse_ip(self.name) is not None:
            raise ValueError(f"{self.name!r} is an IP address, not a host name")
        try:
            name = idna.encode(self.name.removesuffix("."), uts46=True).decode()
        except idna.IDNAError as e:
            raise ValueError(f"{self.name!r} isn't a host name: {e}") from None
        object.__setattr__(self, "name", name)

    async def resolve(self) -> Addr:
        """
        The address the name points to: the first one the system gives, like libtorrent.
        Raises OSError if there isn't one.
        """
        # getaddrinfo raises rather than finding nothing
        found = await anyio.getaddrinfo(self.name, self.port, type=socket.SOCK_STREAM)
        _, _, _, _, sockaddr = found[0]
        return Addr(ipaddress.ip_address(sockaddr[0]), self.port)

    def __str__(self) -> str:
        """``name:port``, as :func:`.parse_addr` reads it"""
        return f"{self.name}:{self.port}"


type AnyAddr = Addr | DomainAddr
"""An address as it was given: an IP address, or a host name to look up"""


def _check_port(port: int) -> None:
    if not 0 < port < 65536:
        raise ValueError(f"port {port} is out of range")


def _parse_ip(host: str) -> IP | None:
    """See :meth:`.Addr.parse`. None if it isn't an IP address."""
    with contextlib.suppress(ValueError):
        return ipaddress.ip_address(host)
    # older ways of writing IPv4 that ip_address refuses but the system reads, like 127.1.
    # inet_aton ignores anything after a space, so only those characters go to it
    if _NUMERIC_IPV4.fullmatch(host):
        with contextlib.suppress(OSError):
            return IPv4Address(socket.inet_aton(host))
    return None


def parse_addr(value: AnyAddr | str | Sequence[Any]) -> AnyAddr:
    """
    An address from ``"host:port"`` (``"[::1]:port"`` for IPv6) or a ``(host, port)`` pair:
    an :class:`.Addr` if the host is an IP address, and a :class:`.DomainAddr` if it's a name.
    An address that's already made is returned as it is.
    """
    if isinstance(value, Addr | DomainAddr):
        return value
    if isinstance(value, str):
        host, _, port = value.rpartition(":")
        host = host.strip("[]")
    else:
        host, port = (str(part) for part in value)
    if not host or not port.isdigit():
        raise ValueError(f"{value} isn't a host and port")
    if (ip := _parse_ip(host)) is not None:
        return Addr(ip, int(port))
    return DomainAddr(host, int(port))
