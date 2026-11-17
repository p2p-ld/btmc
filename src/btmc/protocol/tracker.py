"""Announcing to trackers, one call at a time"""

import contextlib
import secrets
import socket
from urllib.parse import urlsplit

import anyio
import httpx
from anyio.abc import ConnectedUDPSocket, SocketAttribute

from btmc.config import Config
from btmc.exceptions import TrackerError, TrackerRefused
from btmc.protocol.messages import (
    AnnounceRequest,
    AnnounceResponse,
    Connect,
    Connected,
    TrackerFailure,
    UDPResponse,
    decode_http,
    decode_udp,
)
from btmc.types import parse_addr

_DEFAULT_PORTS = {"http": 80, "https": 443}
"""Ports of trackers whose url doesn't say. UDP tracker urls have to."""


def check_url(url: str) -> None:
    """
    Raise TrackerError if a tracker url is one we can't announce to:
    not HTTP(S) or UDP, with spaces or control characters, or without a valid host and port.
    Like libtorrent's ``is_valid_tracker_url``.
    """
    if any(c <= " " or c == "\x7f" for c in url):
        raise TrackerError("url has spaces or control characters")
    try:
        parts = urlsplit(url)
        if parts.scheme not in ("http", "https", "udp"):
            raise TrackerError("only HTTP and UDP trackers are supported")
        port = _DEFAULT_PORTS.get(parts.scheme, 0) if parts.port is None else parts.port
        parse_addr((parts.hostname or "", port))
    except ValueError as e:
        raise TrackerError(f"bad tracker url: {e}") from e


async def announce(
    url: str,
    request: AnnounceRequest,
    *,
    config: Config,
    http: httpx.AsyncClient | None = None,
) -> AnnounceResponse:
    """
    Announce to an HTTP(S) or UDP tracker.

    Each request is sent once. Trying again is up to the caller, which knows how the tracker
    has been doing. Anything that goes wrong, including the tracker not answering within
    :attr:`.Config.announce_timeout`, raises a :class:`.TrackerError`,
    or a :class:`.TrackerRefused` if the tracker answered but refused the announce.

    Args:
        url: the tracker's announce url
        request: what to announce
        config: the timeout
        http: an HTTP client to reuse between announces. Without one, each announce makes its own.
    """
    check_url(url)
    try:
        with anyio.fail_after(config.announce_timeout):
            if url.startswith("udp://"):
                return await _udp_announce(url, request)
            return await _http_announce(url, request, http)
    except (httpx.HTTPError, OSError) as e:
        raise TrackerError(f"announce to {url} failed: {e!r}") from e


async def _http_announce(
    url: str, request: AnnounceRequest, http: httpx.AsyncClient | None
) -> AnnounceResponse:
    sep = "&" if "?" in url else "?"
    async with contextlib.AsyncExitStack() as stack:
        if http is None:
            http = await stack.enter_async_context(httpx.AsyncClient())
        # the whole announce has one timeout, in announce
        response = await http.get(f"{url}{sep}{request.to_http()}", timeout=None)
    # like libtorrent, nothing but a 200 counts as an answer
    if response.status_code != 200:
        raise TrackerError(f"tracker answered with HTTP {response.status_code}")
    match decode_http(response.content):
        case TrackerFailure(reason=reason):
            raise TrackerRefused(reason)
        case AnnounceResponse() as announced:
            return announced


async def _udp_announce(url: str, request: AnnounceRequest) -> AnnounceResponse:
    """BEP 15, for a url :func:`.check_url` passed, so it has a host and port"""
    parts = urlsplit(url)
    assert parts.hostname is not None and parts.port is not None
    async with await anyio.create_connected_udp_socket(parts.hostname, parts.port) as sock:
        ipv6 = sock.extra(SocketAttribute.family) == socket.AF_INET6
        txid = secrets.randbits(32)
        connected = await _udp_call(sock, txid, Connect().to_udp(txid), Connected, ipv6)
        txid = secrets.randbits(32)
        packet = request.to_udp(connected.connection_id, txid)
        return await _udp_call(sock, txid, packet, AnnounceResponse, ipv6)


async def _udp_call[R: UDPResponse](
    sock: ConnectedUDPSocket, transaction_id: int, packet: bytes, expect: type[R], ipv6: bool
) -> R:
    """
    Send a request once, and wait for its answer. Packets that are malformed,
    for other transactions, or of the wrong kind are skipped, since anyone can send
    a UDP packet that looks like it's from the tracker.
    FIXME: Waits forever if no answer comes, so it must be called under a timeout,
    like the one around the whole announce in :func:`.announce`.
    """
    await sock.send(packet)
    while True:
        try:
            answered, response = decode_udp(await sock.receive(), ipv6)
        except TrackerError:
            continue
        if answered != transaction_id:
            continue
        if isinstance(response, TrackerFailure):
            raise TrackerRefused(response.reason)
        if isinstance(response, expect):
            return response
