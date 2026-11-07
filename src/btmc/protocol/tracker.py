"""Announcing to trackers, one call at a time"""

import contextlib
import random
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


async def announce(
    url: str,
    request: AnnounceRequest,
    *,
    http: httpx.AsyncClient | None = None,
    config: Config | None = None,
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
        http: an HTTP client to reuse between announces. Without one, each announce makes its own.
        config: the timeout
    """
    config = config or Config()
    try:
        with anyio.fail_after(config.announce_timeout):
            if url.startswith("udp://"):
                return await _udp_announce(url, request)
            if url.startswith(("http://", "https://")):
                return await _http_announce(url, request, http)
    except (httpx.HTTPError, OSError) as e:
        raise TrackerError(f"announce to {url} failed: {e!r}") from e
    raise TrackerError(f"can't announce to {url}, only HTTP and UDP trackers are supported")


async def _http_announce(
    url: str, request: AnnounceRequest, http: httpx.AsyncClient | None
) -> AnnounceResponse:
    sep = "&" if "?" in url else "?"
    async with contextlib.AsyncExitStack() as stack:
        if http is None:
            http = await stack.enter_async_context(httpx.AsyncClient())
        # the whole announce has one timeout, in announce
        response = await http.get(f"{url}{sep}{request.to_http()}", timeout=None)
        response.raise_for_status()
    match decode_http(response.content):
        case TrackerFailure(reason=reason):
            raise TrackerRefused(reason)
        case AnnounceResponse() as announced:
            return announced


async def _udp_announce(url: str, request: AnnounceRequest) -> AnnounceResponse:
    """BEP 15"""
    parts = urlsplit(url)
    if not parts.hostname or not parts.port:
        raise TrackerError(f"bad UDP tracker url {url}")
    async with await anyio.create_connected_udp_socket(parts.hostname, parts.port) as sock:
        ipv6 = sock.extra(SocketAttribute.family) == socket.AF_INET6
        txid = random.getrandbits(32)
        connected = await _udp_call(sock, txid, Connect().to_udp(txid), Connected, ipv6)
        txid = random.getrandbits(32)
        packet = request.to_udp(connected.connection_id, txid)
        return await _udp_call(sock, txid, packet, AnnounceResponse, ipv6)


async def _udp_call[R: UDPResponse](
    sock: ConnectedUDPSocket, transaction_id: int, packet: bytes, expect: type[R], ipv6: bool
) -> R:
    """
    Send a request once, and wait for its answer, skipping packets for other transactions.
    FIXME: Waits forever if no answer comes, so it must be called under a timeout,
    like the one around the whole announce in :func:`.announce`.
    """
    await sock.send(packet)
    while True:
        answered, response = decode_udp(await sock.receive(), ipv6)
        if answered != transaction_id:
            continue
        if isinstance(response, TrackerFailure):
            raise TrackerRefused(response.reason)
        if not isinstance(response, expect):
            raise TrackerError(f"expected {expect.__name__}, got {type(response).__name__}")
        return response
