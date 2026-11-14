"""Coordinating many torrents"""

import asyncio
import contextlib
import os
from collections.abc import Callable, Iterable, Sequence
from pathlib import Path
from typing import Any, Literal, cast

import anyio
import httpx
from anyio.abc import SocketAttribute, SocketStream, TaskGroup
from anyio.streams.buffered import BufferedByteStream
from torrent_models import Torrent

from btmc.config import Config
from btmc.protocol.messages import Bitfield, Handshake
from btmc.protocol.peer import PEER_ERRORS
from btmc.protocol.shared import Shared
from btmc.protocol.torrent import TorrentState
from btmc.types import Addr, PieceIndex
from btmc.utils import torrent_name, wire_hash

PEER_ID_PREFIX = b"-SC0001-"
"""The start of our peer id, which tells peers what client we are"""

type ClientEvent = Literal["added", "removed"]
"""
What callbacks can be added for, see :meth:`.Client.add_callback`

- ``"added"``: a torrent was added. Called with the torrent.
- ``"removed"``: a torrent was removed or stopped running. Called with the torrent.
"""

type TorrentCallback = Callable[[TorrentState], None]


class Client:
    """
    Fetches pieces of many torrents, each a :class:`.TorrentState`.

    The client only coordinates: it keeps track of the torrents, gives them
    what they have in common in a :class:`.Shared` (who we are, the HTTP client,
    limits, banned peers and tracker backoffs), saves and loads what should outlast it,
    and hands each incoming connection to its torrent.

    Use it with ``async with``, or call :meth:`.start` and later :meth:`.stop`,
    from any task. A client can only be started once.
    Add torrents with :meth:`.add`, then :meth:`.fetch` pieces or :meth:`.read` bytes of files
    from them, as many times as needed, from as many tasks as needed.
    Adding a torrent that's already there returns the same one,
    so different jobs can share a client without coordinating.

    Args:
        config: settings, including where torrents go, :attr:`.Config.download_dir`.
            See :meth:`.Config.from_toml`
    """

    def __init__(self, *, config: Config | None = None):
        self.config = config or Config()
        self.peer_id = PEER_ID_PREFIX + os.urandom(20 - len(PEER_ID_PREFIX))
        self._torrents: set[TorrentState] = set()
        self._by_hash: dict[bytes, TorrentState] = {}
        """info hash, see :func:`.wire_hash` -> torrent. Hybrids are in here twice."""
        self._shared: Shared | None = None
        self._tg: TaskGroup | None = None
        self._task: asyncio.Task[None] | None = None
        """The task the client runs in, see :meth:`.start`"""
        self._running = asyncio.Event()
        """Set while the client runs, from when it's started until it starts stopping"""
        self._stopping = asyncio.Event()
        self._callbacks: dict[ClientEvent, list[TorrentCallback]] = {"added": [], "removed": []}

    async def __aenter__(self) -> "Client":
        await self.start()
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.stop()

    async def start(self) -> None:
        """
        Open the HTTP client, start listening if we do, and load saved state,
        returning once the client is running. Raises RuntimeError if it was started before.
        """
        if self._task is not None:
            raise RuntimeError("a client can only be started once")
        # what start opens has to be closed in the same task, so the client runs in a task
        # of its own, and start and stop can be called from any task
        # resolved by _run once the client is running, or failed with what stopped it starting
        ready: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        self._task = asyncio.create_task(self._run(ready))
        try:
            await ready
        except BaseException:
            self._stopping.set()
            raise

    async def stop(self) -> None:
        """
        Stop every torrent, save state, and close everything :meth:`.start` opened.
        Raises whatever the client stopped running with, if anything.
        Does nothing if the client was never started.
        """
        if self._task is None:
            return
        self._stopping.set()
        # so a caller that stops waiting doesn't cancel the client partway through stopping
        await asyncio.shield(self._task)

    async def _run(self, ready: asyncio.Future) -> None:
        try:
            async with contextlib.AsyncExitStack() as stack:
                # start listeners
                http = await stack.enter_async_context(httpx.AsyncClient())
                port = self.config.port
                listener = None
                if self.config.listen_host is not None:
                    listener = await anyio.create_tcp_listener(
                        local_host=self.config.listen_host, local_port=port
                    )
                    stack.push_async_callback(listener.aclose)
                    port = listener.extra(SocketAttribute.local_port)

                # create shared state
                self._shared = Shared(self.config, self.peer_id, port=port, http=http)
                state_file = self.config.state_file
                if state_file is not None:
                    await anyio.to_thread.run_sync(self._shared.load, state_file)
                    stack.push_async_callback(self._save_state, state_file)

                # run them torrents
                async with anyio.create_task_group() as tg:
                    self._tg = tg
                    if listener is not None:
                        tg.start_soon(listener.serve, self._incoming)
                    self._running.set()
                    # done already if start's caller was cancelled
                    if not ready.done():
                        ready.set_result(None)
                    await self._stopping.wait()

                    # closing...
                    self._running.clear()
                    with anyio.CancelScope(shield=True):
                        async with anyio.create_task_group() as stopping:
                            for t in list(self._torrents):
                                stopping.start_soon(t.stop)
                    tg.cancel_scope.cancel()
        except BaseException as e:
            if ready.done():
                raise
            # a failure to start goes to start's caller, and nobody waits on this task after that,
            # but cancellations still have to end it
            ready.set_exception(e)
            if not isinstance(e, Exception):
                raise
        finally:
            self._tg = None
            self._running.clear()

    @property
    def running(self) -> bool:
        """Whether the client has started, and isn't stopping or stopped"""
        return self._running.is_set()

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

    def add_callback(self, event: ClientEvent, callback: TorrentCallback) -> None:
        """Call ``callback`` whenever ``event`` happens"""
        self._callbacks[event].append(callback)

    def remove_callback(self, event: ClientEvent, callback: TorrentCallback) -> None:
        """Stop calling a callback, like when whatever added it goes away before the client"""
        self._callbacks[event].remove(callback)

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
                in the config's :attr:`.Config.download_dir`
            trackers: tracker tiers to use instead of the torrent's own
            known_peers: peers to connect to without asking a tracker
        """
        if not self.running:
            raise RuntimeError("the client isn't running")
        assert self._tg is not None and self._shared is not None
        existing = self.get(torrent_name(torrent))
        if existing is not None:
            return existing
        if root is None:
            root = self.config.download_dir / torrent_name(torrent)
        t = TorrentState(torrent, root, self._shared, trackers=trackers, known_peers=known_peers)
        self._torrents.add(t)
        for info_hash in t.info_hashes:
            self._by_hash[info_hash] = t
        self._tg.start_soon(self._run_torrent, t)
        for callback in self._callbacks["added"]:
            callback(t)
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

    async def _run_torrent(self, t: TorrentState) -> None:
        try:
            await t.run()
        finally:
            self._forget(t)

    def _forget(self, t: TorrentState) -> None:
        # removing a torrent forgets it, and so does it stopping running afterwards
        if t not in self._torrents:
            return
        self._torrents.discard(t)
        for info_hash in t.info_hashes:
            if self._by_hash.get(info_hash) is t:
                del self._by_hash[info_hash]
        for callback in self._callbacks["removed"]:
            callback(t)

    # ---- fetching ----

    def want(self, t: TorrentState, pieces: Iterable[PieceIndex] | None = None) -> None:
        """See :meth:`.TorrentState.want`"""
        t.want(pieces)

    def have(self, t: TorrentState) -> Bitfield:
        """See :attr:`.TorrentState.have`"""
        return t.have

    async def fetch(
        self,
        t: TorrentState,
        pieces: Iterable[PieceIndex] | None = None,
        *,
        timeout: float | None = None,
    ) -> None:
        """See :meth:`.TorrentState.fetch`"""
        await t.fetch(pieces, timeout=timeout)

    async def read(self, t: TorrentState, path: str, start: int, end: int) -> bytes:
        """See :meth:`.TorrentState.read`"""
        return await t.read(path, start, end)

    async def fetch_file(self, t: TorrentState, path: str) -> Path:
        """See :meth:`.TorrentState.fetch_file`"""
        return await t.fetch_file(path)

    # ---- incoming connections ----

    async def _incoming(self, stream: SocketStream) -> None:
        """Read an incoming connection's handshake, and hand it to its torrent"""
        connection = BufferedByteStream(stream)
        accepted = False
        try:
            with anyio.fail_after(self.config.handshake_timeout):
                handshake = Handshake.decode(await connection.receive_exactly(Handshake.LENGTH))
            t = self.get(handshake.info_hash)
            addr = cast(Addr, stream.extra(SocketAttribute.remote_address)[:2])
            accepted = t is not None and t.accept_connection(connection, addr, handshake)
        except PEER_ERRORS:
            pass
        finally:
            # including when the client stops while we wait for the handshake
            if not accepted:
                await anyio.aclose_forcefully(connection)
