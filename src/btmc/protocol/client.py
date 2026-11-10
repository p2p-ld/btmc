"""Coordinating many torrents"""

import contextlib
import os
from collections.abc import Iterable, Sequence
from pathlib import Path
from typing import Any, cast

import anyio
import httpx
from anyio.abc import SocketAttribute, SocketStream, TaskGroup
from anyio.streams.buffered import BufferedByteStream
from torrent_models import Torrent

from btmc.config import Config
from btmc.protocol.messages import Bitfield, Handshake
from btmc.protocol.peer import PEER_ERRORS
from btmc.protocol.shared import Shared
from btmc.protocol.swarm import wire_hash
from btmc.protocol.torrent import TorrentState, torrent_name
from btmc.types import Addr, PieceIndex

PEER_ID_PREFIX = b"-SC0001-"
"""The start of our peer id, which tells peers what client we are"""


class Client:
    """
    Fetches pieces of many torrents, each a :class:`.TorrentState`.

    The client only coordinates: it keeps track of the torrents, gives them
    what they have in common in a :class:`.Shared` (who we are, the HTTP client,
    limits, banned peers and tracker backoffs), saves and loads what should outlast it,
    and hands each incoming connection to its torrent.

    Use it with ``async with``, or call :meth:`.start` and then :meth:`.stop`
    from the same task. A client can only be started once.
    Add torrents with :meth:`.add`, then :meth:`.fetch` pieces or :meth:`.read` bytes of files
    from them, as many times as needed, from as many tasks as needed.
    Adding a torrent that's already there returns the same one,
    so different jobs can share a client without coordinating.

    Args:
        download_dir: where torrents go, each in a directory named by its info hash,
            unless :meth:`.add` is given a directory for it
        config: settings, see :meth:`.Config.from_toml`
    """

    def __init__(self, download_dir: Path | None = None, *, config: Config | None = None):
        self.download_dir = download_dir
        self.config = config or Config()
        self.peer_id = PEER_ID_PREFIX + os.urandom(20 - len(PEER_ID_PREFIX))
        self._torrents: set[TorrentState] = set()
        self._by_hash: dict[bytes, TorrentState] = {}
        """info hash, see :func:`.wire_hash` -> torrent. Hybrids are in here twice."""
        self._stack = contextlib.AsyncExitStack()
        self._shared: Shared | None = None
        self._tg: TaskGroup | None = None
        self._started = anyio.Event()
        self._stopped = anyio.Event()

    async def __aenter__(self) -> "Client":
        await self.start()
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.stop()

    async def start(self) -> None:
        """
        Open the HTTP client, start listening if we do, and load saved state.
        Raises RuntimeError if the client was started before.
        """
        if self._started.is_set():
            raise RuntimeError("a client can only be started once")
        self._started.set()
        try:
            http = await self._stack.enter_async_context(httpx.AsyncClient())
            port = self.config.port
            listener = None
            if self.config.listen_host is not None:
                listener = await anyio.create_tcp_listener(
                    local_host=self.config.listen_host, local_port=port
                )
                self._stack.push_async_callback(listener.aclose)
                port = listener.extra(SocketAttribute.local_port)
            self._shared = Shared(self.config, self.peer_id, port=port, http=http)
            state_file = self.config.state_file
            if state_file is not None:
                await anyio.to_thread.run_sync(self._shared.load, state_file)
                self._stack.push_async_callback(self._save_state, state_file)
            self._tg = await self._stack.enter_async_context(anyio.create_task_group())
            if listener is not None:
                self._tg.start_soon(listener.serve, self._incoming)
        except BaseException:
            await self.stop()
            raise

    async def stop(self) -> None:
        """
        Stop every torrent, save state, and close everything :meth:`.start` opened.
        Does nothing if the client isn't running.
        """
        if not self._started.is_set() or self._stopped.is_set():
            return
        self._stopped.set()
        with anyio.CancelScope(shield=True):
            async with anyio.create_task_group() as tg:
                for t in list(self._torrents):
                    tg.start_soon(t.stop)
        if self._tg is not None:
            self._tg.cancel_scope.cancel()
            self._tg = None
        await self._stack.aclose()

    async def _save_state(self, path: Path) -> None:
        assert self._shared is not None
        # even if we're closing because the caller was cancelled
        with anyio.CancelScope(shield=True):
            await anyio.to_thread.run_sync(self._shared.save, path)

    @property
    def port(self) -> int:
        """The port we announce, which is the one we listen on, if we do"""
        assert self._shared is not None
        return self._shared.port

    @property
    def peak_connections(self) -> int:
        return self._shared.peak_connections if self._shared is not None else 0

    # ---- torrents ----

    @property
    def torrents(self) -> list[TorrentState]:
        return list(self._torrents)

    def add(
        self,
        torrent: Torrent[Any],
        root: Path | None = None,
        *,
        trackers: list[list[str]] | None = None,
        known_peers: Sequence[Addr] = (),
    ) -> TorrentState:
        """
        Add a torrent and start fetching from it, or get it if it's already been added.

        Args:
            torrent: the torrent
            root: download directory, defaults to a directory named by its info hash
                in the client's download directory
            trackers: tracker tiers to use instead of the torrent's own
            known_peers: peers to connect to without asking a tracker
        """
        if self._tg is None or self._shared is None:
            raise RuntimeError("the client isn't running")
        existing = self.get(torrent_name(torrent))
        if existing is not None:
            return existing
        if root is None:
            if self.download_dir is None:
                raise ValueError("pass a root for the torrent, or give the client a download_dir")
            root = self.download_dir / torrent_name(torrent)
        t = TorrentState(torrent, root, self._shared, trackers=trackers, known_peers=known_peers)
        self._torrents.add(t)
        for info_hash in t.info_hashes:
            self._by_hash[info_hash] = t
        self._tg.start_soon(self._run, t)
        return t

    def get(self, info_hash: str | bytes) -> TorrentState | None:
        """A torrent by any of its info hashes, see :func:`.wire_hash`"""
        return self._by_hash.get(wire_hash(info_hash))

    async def remove(self, info_hash: str | bytes) -> None:
        """Stop fetching a torrent and forget it. What was fetched stays on disk."""
        t = self.get(info_hash)
        if t is not None:
            await t.stop()
            self._forget(t)

    async def _run(self, t: TorrentState) -> None:
        try:
            await t.run()
        finally:
            self._forget(t)

    def _forget(self, t: TorrentState) -> None:
        self._torrents.discard(t)
        for info_hash in t.info_hashes:
            if self._by_hash.get(info_hash) is t:
                del self._by_hash[info_hash]

    # ---- fetching ----

    def want(self, t: TorrentState, pieces: Iterable[PieceIndex]) -> None:
        """See :meth:`.TorrentState.want`"""
        t.want(pieces)

    def have(self, t: TorrentState) -> Bitfield:
        """See :attr:`.TorrentState.have`"""
        return t.have

    async def fetch(self, t: TorrentState, pieces: Iterable[PieceIndex]) -> None:
        """See :meth:`.TorrentState.fetch`"""
        await t.fetch(pieces)

    async def read(self, t: TorrentState, path: str, start: int, end: int) -> bytes:
        """See :meth:`.TorrentState.read`"""
        return await t.read(path, start, end)

    # ---- incoming connections ----

    async def _incoming(self, stream: SocketStream) -> None:
        """Read an incoming connection's handshake, and hand it to its torrent"""
        connection = BufferedByteStream(stream)
        try:
            with anyio.fail_after(self.config.handshake_timeout):
                handshake = Handshake.decode(await connection.receive_exactly(Handshake.LENGTH))
            t = self.get(handshake.info_hash)
            addr = cast(Addr, stream.extra(SocketAttribute.remote_address)[:2])
            if t is not None and t.accept_connection(connection, addr, handshake):
                return
        except PEER_ERRORS:
            pass
        await anyio.aclose_forcefully(connection)
