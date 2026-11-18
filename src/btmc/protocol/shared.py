"""What a client's torrents share"""

import json
import logging
import os
from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Literal

import anyio
import httpx

from btmc.config import Config
from btmc.types import Addr

log = logging.getLogger(__name__)


class BanReason(StrEnum):
    """Why a peer was banned"""

    BAD_DATA = "sent bad data"
    MALFORMED_MESSAGE = "sent a malformed message"


type SharedEvent = Literal["ban"]
"""
What callbacks can be added for, see :meth:`.Shared.add_callback`

- ``"ban"``: a peer was banned. Called with its address.
"""

type BanCallback = Callable[[Addr], None]


@dataclass
class _TrackerBackoff:
    """A tracker that isn't answering"""

    wait: float
    """How long we waited after the last failure"""
    retry_at: float


class Shared:
    """
    What a client's torrents share, and the only way they share anything:
    who we are, limits on connections, announces and hashing, banned and blocked peers,
    and trackers that aren't answering.

    Args:
        config: settings
        peer_id: our peer id
        port: the port we announce. Defaults to the configured one.
        http: an HTTP client for announcing. Without one, each announce makes its own.
    """

    def __init__(
        self,
        config: Config,
        peer_id: bytes,
        port: int | None = None,
        http: httpx.AsyncClient | None = None,
    ):
        self.config = config
        self.peer_id = peer_id
        self.port = config.port if port is None else port
        self.http = http
        self.connections = anyio.CapacityLimiter(config.max_connections)
        """Peer connections across all torrents"""
        self.announces = anyio.CapacityLimiter(config.announce_concurrency)
        """Announces in progress across all torrents"""
        self.hashing = anyio.CapacityLimiter(config.hash_threads)
        """
        Worker threads hash checking pieces, across all torrents.
        Pass it as the ``limiter`` of ``anyio.to_thread.run_sync``.
        """
        self._banned: set[Addr] = set()
        self._tracker_backoffs: dict[str, _TrackerBackoff] = {}
        self._callbacks: dict[SharedEvent, list[BanCallback]] = {"ban": []}

    def add_callback(self, event: SharedEvent, callback: BanCallback) -> None:
        """Call ``callback`` whenever ``event`` happens"""
        self._callbacks[event].append(callback)

    def remove_callback(self, event: SharedEvent, callback: BanCallback) -> None:
        """Stop calling a callback, like when a torrent that added it is removed"""
        self._callbacks[event].remove(callback)

    def ban(self, addr: Addr, reason: BanReason) -> None:
        """Stop every torrent from connecting to a peer"""
        log.warning("banning %s: %s", addr, reason)
        self._banned.add(addr)
        for callback in self._callbacks["ban"]:
            callback(addr)

    def is_banned(self, addr: Addr) -> bool:
        return addr in self._banned

    def is_blocked(self, addr: Addr) -> bool:
        """Whether an address is in :attr:`.Config.ip_blocklist`, so no torrent connects to it"""
        return any(addr.ip in net for net in self.config.ip_blocklist)

    def tracker_ready_at(self, url: str) -> float:
        """When torrents can announce to a tracker, which is in the past unless it's backed off"""
        backoff = self._tracker_backoffs.get(url)
        return 0 if backoff is None else backoff.retry_at

    def announced(self, url: str, now: float, *, answered: bool = True) -> None:
        """
        How an announce to a tracker went. A tracker that answers is ready again.
        One that doesn't is backed off for every torrent,
        twice as long for each failure in a row.
        """
        if answered:
            self._tracker_backoffs.pop(url, None)
            return
        backoff = self._tracker_backoffs.get(url)
        if backoff is None:
            self._tracker_backoffs[url] = _TrackerBackoff(
                self.config.tracker_backoff, now + self.config.tracker_backoff
            )
        # other torrents' announces that were already on their way when it went down
        # fail at about the same time, and only the first counts
        elif now >= backoff.retry_at:
            backoff.wait = min(backoff.wait * 2, self.config.max_tracker_backoff)
            backoff.retry_at = now + backoff.wait

    def save(self, path: Path) -> None:
        """
        Write what should outlast the client, like banned peers.
        Blocks, so run it in a worker thread.
        """
        tmp = path.with_name(path.name + ".tmp")
        banned = sorted([str(addr.ip), addr.port] for addr in self._banned)
        tmp.write_text(json.dumps({"banned": banned}))
        os.replace(tmp, path)

    def load(self, path: Path) -> None:
        """
        Read what :meth:`.save` wrote, if there's anything there.
        Blocks, so run it in a worker thread.
        """
        if path.exists():
            state = json.loads(path.read_text())
            self._banned |= {Addr.parse(ip, port) for ip, port in state["banned"]}
