import asyncio
import os
from collections.abc import AsyncIterator, Awaitable, Callable
from pathlib import Path

import pytest
from torrent_models import Torrent, TorrentVersion

from btmc.protocol.messages import (
    Bitfield,
    BitfieldMessage,
    Handshake,
    PeerMessage,
    Piece,
    Request,
    Unchoke,
)
from btmc.types import Addr
from btmc.utils import info_hashes

from .net import free_port
from .tracker import MiniTracker

try:
    import libtorrent as lt
except ImportError:
    lt = None

__all__ = [
    "FakeSeeder",
    "LtSeeder",
    "fake_seeder",
    "seeders",
]


class LtSeeder:
    """A libtorrent session that seeds from localhost, with only TCP and trackers"""

    def __init__(self):
        self.port = free_port()
        self.addr = Addr("127.0.0.1", self.port)
        self.ses = lt.session(
            {
                "listen_interfaces": f"127.0.0.1:{self.port}",
                "outgoing_interfaces": "127.0.0.1",
                "enable_dht": False,
                "enable_lsd": False,
                "enable_upnp": False,
                "enable_natpmp": False,
                "enable_incoming_utp": False,
                "enable_outgoing_utp": False,
                "allow_multiple_connections_per_ip": True,
                "announce_to_all_tiers": True,
                "announce_to_all_trackers": True,
            }
        )
        self.handles: dict[TorrentVersion, lt.torrent_handle] = {}

    def add(self, version: TorrentVersion, torrent_path: Path, save_path: Path) -> None:
        atp = lt.add_torrent_params()
        atp.ti = lt.torrent_info(str(torrent_path))
        atp.save_path = str(save_path)
        self.handles[version] = self.ses.add_torrent(atp)

    async def wait_seeding(self) -> None:
        while not all(h.status().is_seeding for h in self.handles.values()):
            await asyncio.sleep(0.1)

    def close(self) -> None:
        for h in self.handles.values():
            self.ses.remove_torrent(h)
        self.ses.pause()


class FakeSeeder:
    """
    A seeder that only answers to some info hashes, like a v1-only or v2-only client would
    in a hybrid torrent's swarm, and can send junk instead of data.
    """

    def __init__(
        self, torrent: Torrent, source_dir: Path, accept: set[bytes], corrupt: bool = False
    ):
        self.layout = torrent.layout
        self.source_dir = source_dir
        self.accept = accept
        self.corrupt = corrupt
        self.peer_id = b"-FAKE00-" + os.urandom(12)
        self.refused = 0

    async def start(self) -> None:
        self._server = await asyncio.start_server(self._serve, "127.0.0.1", 0)
        self.port = self._server.sockets[0].getsockname()[1]
        self.addr = Addr("127.0.0.1", self.port)

    async def stop(self) -> None:
        self._server.close()

    async def _serve(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            handshake = Handshake.decode(await reader.readexactly(Handshake.LENGTH))
            if handshake.info_hash not in self.accept:
                self.refused += 1
                return
            writer.write(Handshake(handshake.info_hash, self.peer_id).encode())
            writer.write(BitfieldMessage(Bitfield.full(self.layout.n_pieces)).encode())
            writer.write(Unchoke().encode())
            while True:
                length = int.from_bytes(await reader.readexactly(4))
                body = await reader.readexactly(length) if length else b""
                msg = PeerMessage.decode(length.to_bytes(4) + body)
                if isinstance(msg, Request):
                    data = self.layout.piece_range(msg.index).read(self.source_dir)
                    block = data[msg.begin : msg.begin + msg.length]
                    if self.corrupt:
                        block = os.urandom(len(block))
                    writer.write(Piece(msg.index, msg.begin, block).encode())
                    await writer.drain()
        except (asyncio.IncompleteReadError, ConnectionError):
            pass
        finally:
            writer.close()


@pytest.fixture(scope="session")
async def seeders(
    torrent_files: dict[TorrentVersion, Path],
    torrents: dict[TorrentVersion, Torrent],
    test_data: Path,
    tracker_server: MiniTracker,
) -> AsyncIterator[list[LtSeeder]]:
    """
    Two libtorrent seeders, seeding all the :func:`.torrents`, and in the tracker's swarms.

    Shared by the whole session, because they take a few seconds to start announcing.
    """
    seeders = [LtSeeder(), LtSeeder()]
    for seeder in seeders:
        for version, path in torrent_files.items():
            seeder.add(version, path, test_data)
    await asyncio.gather(*(seeder.wait_seeding() for seeder in seeders))
    hashes = [h for t in torrents.values() for h in info_hashes(t)]
    while not all(len(tracker_server.swarms[h]) >= 2 for h in hashes):
        await asyncio.sleep(0.1)
    yield seeders
    for seeder in seeders:
        seeder.close()


@pytest.fixture
async def fake_seeder(
    test_data: Path, tracker: MiniTracker
) -> AsyncIterator[Callable[..., Awaitable[FakeSeeder]]]:
    """
    Start :class:`.FakeSeeder` s, which are stopped and taken out of the tracker's swarms
    after the test
    """
    started = []

    async def start(torrent: Torrent, accept: set[bytes], corrupt: bool = False) -> FakeSeeder:
        seeder = FakeSeeder(torrent, test_data, accept=accept, corrupt=corrupt)
        await seeder.start()
        started.append(seeder)
        return seeder

    yield start
    for seeder in started:
        await seeder.stop()
        for swarm in tracker.swarms.values():
            swarm.pop(seeder.peer_id, None)
