import asyncio
import os
import socket
import struct
from collections import defaultdict
from collections.abc import AsyncIterator, Callable, Iterator
from typing import NamedTuple
from urllib.parse import parse_qsl

import bencode_rs
import pytest

from btmc.types import Addr

from .net import free_port

__all__ = ["MiniTracker", "TrackerUrls", "reset_tracker", "tracker", "tracker_urls"]


def compact(addrs: list[Addr]) -> bytes:
    return b"".join(a.ip.packed + a.port.to_bytes(2) for a in addrs)


class TrackerUrls(NamedTuple):
    """The announce urls of the :func:`.tracker`, from the ports it listens on"""

    http_port: int
    udp_port: int

    @property
    def http(self) -> str:
        return f"http://127.0.0.1:{self.http_port}/announce"

    @property
    def udp(self) -> str:
        return f"udp://127.0.0.1:{self.udp_port}/announce"


class MiniTracker:
    """
    HTTP and UDP tracker in one, sharing a peer list.

    ``only[info_hash] = {ports}`` limits which peers it hands out for an info hash,
    to split a hybrid torrent's swarms. Announces over HTTP for info hashes in ``refuse``
    are refused. With ``unresponsive``, announces over HTTP are taken, but never answered.
    ``interval`` is how long it tells clients to wait between announces.
    """

    def __init__(self, urls: TrackerUrls):
        self.urls = urls
        self._kept: dict[bytes, dict[bytes, Addr]] = {}
        self.reset()

    def keep(self) -> None:
        """Keep the peers in the swarms now through every :meth:`.reset`"""
        self._kept = {info_hash: dict(swarm) for info_hash, swarm in self.swarms.items()}

    def reset(self) -> None:
        """Undo every announce and setting, apart from the peers it was told to :meth:`.keep`"""
        self.swarms: dict[bytes, dict[bytes, Addr]] = defaultdict(
            dict, {info_hash: dict(swarm) for info_hash, swarm in self._kept.items()}
        )
        self.events: dict[bytes, asyncio.Queue[tuple[bytes, str]]] = defaultdict(asyncio.Queue)
        """
        peer id -> the info hash and event of each announce it made with an event,
        that :meth:`.next_event` hasn't taken yet
        """
        self.only: dict[bytes, set[int]] = {}
        self.refuse: set[bytes] = set()
        self.unresponsive = False
        self.interval = 60

    async def start(self) -> None:
        self._http = await asyncio.start_server(self._serve_http, "127.0.0.1", self.urls.http_port)
        loop = asyncio.get_running_loop()
        self._udp, _ = await loop.create_datagram_endpoint(
            lambda: _DatagramServer(self._answer_udp), local_addr=("127.0.0.1", self.urls.udp_port)
        )

    async def stop(self) -> None:
        self._udp.close()
        self._http.close()

    def announce(
        self, info_hash: bytes, peer_id: bytes, ip: str, port: int, event: str
    ) -> list[Addr]:
        if event:
            self.events[peer_id].put_nowait((info_hash, event))
        swarm = self.swarms[info_hash]
        if event == "stopped":
            swarm.pop(peer_id, None)
        else:
            swarm[peer_id] = Addr.parse(ip, port)
        peers = [addr for pid, addr in swarm.items() if pid != peer_id]
        if info_hash in self.only:
            peers = [a for a in peers if a.port in self.only[info_hash]]
        return peers

    async def next_event(self, peer_id: bytes) -> tuple[bytes, str]:
        """
        The info hash and event of the next announce a peer makes with an event,
        waiting up to 5 seconds for it
        """
        async with asyncio.timeout(5):
            return await self.events[peer_id].get()

    def swarms_of(self, peer_id: bytes) -> set[bytes]:
        """The info hashes whose swarms a peer is in: it announced, and hasn't said it stopped"""
        return {info_hash for info_hash, swarm in self.swarms.items() if peer_id in swarm}

    async def _serve_http(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            try:
                request = await reader.readuntil(b"\r\n\r\n")
            except asyncio.IncompleteReadError:
                # the client gave up before sending its request, like after a short timeout
                return
            target = request.split(b" ")[1]
            params = dict(parse_qsl(target.split(b"?", 1)[1]))
            if params[b"info_hash"] in self.refuse:
                body = bencode_rs.bencode({"failure reason": "unregistered torrent"})
            else:
                peers = self.announce(
                    params[b"info_hash"],
                    params[b"peer_id"],
                    writer.get_extra_info("peername")[0],
                    int(params[b"port"]),
                    params.get(b"event", b"").decode(),
                )
                body = bencode_rs.bencode({"interval": self.interval, "peers": compact(peers)})
            if self.unresponsive:
                # until the client gives up
                await reader.read()
                return
            writer.write(
                b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nConnection: close\r\n"
                b"Content-Length: %d\r\n\r\n" % len(body) + body
            )
            await writer.drain()
        finally:
            writer.close()

    def _answer_udp(self, data: bytes, addr: tuple[str, int]) -> bytes | None:
        action, txid = struct.unpack(">II", data[8:16])
        if action == 0:
            return struct.pack(">II", 0, txid) + os.urandom(8)
        if action == 1:
            fields = struct.unpack(">8sII20s20sQQQIIIiH", data[:98])
            info_hash, peer_id, event, port = fields[3], fields[4], fields[8], fields[12]
            event_name = ["", "completed", "started", "stopped"][event]
            peers = self.announce(info_hash, peer_id, addr[0], port, event_name)
            header = struct.pack(">IIIII", 1, txid, self.interval, 0, len(peers))
            return header + compact(peers)
        return None


class _DatagramServer(asyncio.DatagramProtocol):
    """Answers each datagram with what ``answer`` returns for it, if anything"""

    def __init__(self, answer: Callable[[bytes, tuple[str, int]], bytes | None]):
        self._answer = answer

    def connection_made(self, transport: asyncio.DatagramTransport) -> None:  # type: ignore[override]
        self._transport = transport

    def datagram_received(self, data: bytes, addr: tuple[str, int]) -> None:
        if (reply := self._answer(data, addr)) is not None:
            self._transport.sendto(reply, addr)


@pytest.fixture(scope="session")
def tracker_urls() -> TrackerUrls:
    """
    Where the :func:`.tracker` listens, picked without starting it,
    for torrents that only need to name it
    """
    return TrackerUrls(http_port=free_port(), udp_port=free_port(socket.SOCK_DGRAM))


@pytest.fixture(scope="session")
async def tracker(anyio_backend: str, tracker_urls: TrackerUrls) -> AsyncIterator[MiniTracker]:
    """
    The tracker shared by the whole test session, reset after each test that uses it.
    Asks for ``anyio_backend`` so sync tests can use it too.
    """
    tracker = MiniTracker(tracker_urls)
    await tracker.start()
    yield tracker
    await tracker.stop()


@pytest.fixture(autouse=True)
def reset_tracker(request: pytest.FixtureRequest) -> Iterator[None]:
    """After each test that used the tracker, undo what it did to it"""
    yield
    if "tracker" in request.fixturenames:
        request.getfixturevalue("tracker").reset()
