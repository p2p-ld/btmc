import dataclasses
import os
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Self

import platformdirs

CONFIG_FILE = "btmc.toml"
"""Where :meth:`.Config.from_toml` looks by default, in the current directory"""


@dataclass
class Config:
    """
    Settings for a :class:`.Client`. Times are in seconds.

    Load them from a TOML file of ``name = value`` pairs with :meth:`.from_toml`.
    """

    # ---- listening ----

    listen_host: str | None = None
    """Address to take incoming connections on. ``None`` doesn't listen."""
    port: int = 6881
    """Port to listen on, or 0 for any free port. It's also the port we announce."""

    # ---- limits ----

    max_connections: int = 200
    """Peer connections across all torrents"""
    max_peers: int = 30
    """Peer connections per torrent"""
    pipeline_depth: int = 32
    """Requests in flight per peer, in 16 KiB blocks"""
    announce_concurrency: int = 16
    """Announces in progress at once, across all torrents"""
    hash_threads: int = dataclasses.field(default_factory=lambda: os.cpu_count() or 1)
    """Pieces being hash checked at once, across all torrents. Defaults to the number of CPUs."""
    max_pieces_checking: int = 16
    """
    Pieces per torrent that are complete and waiting for their hash check, held in memory.
    While this many are, no new piece is started, until the checks catch up.
    """

    # ---- peers ----

    connect_timeout: float = 10
    handshake_timeout: float = 10
    request_timeout: float = 60
    """How long pending requests can go without a block arriving, before giving up on the peer"""
    peer_idle_timeout: float = 240
    """How long a peer can go without sending anything, when we're not waiting on it"""
    timeout_check_interval: float = 5
    """How often each peer checks the timeouts above, so how late they can be"""
    keepalive_interval: float = 60
    connect_interval: float = 5
    """How often to look for peers to connect to, when nothing else prompts it"""
    peer_retry: float = 30
    """How long to wait before reconnecting to a peer that sent us data and then disconnected"""
    peer_backoff: float = 15
    """How long to wait before retrying a peer that failed, doubled for each failure in a row"""
    max_peer_failures: int = 5
    """Failures in a row before giving up on a peer"""

    # ---- trackers ----

    announce_timeout: float = 15
    """
    How long one announce can take, from looking up the tracker to its answer.
    Each request is sent once: a tracker that doesn't answer in time is backed off.
    """
    announce_retry: float = 300
    """How long to wait before announcing again when a tracker refused, or didn't say"""
    tracker_backoff: float = 15
    """
    How long every torrent waits before announcing to a tracker that didn't answer,
    doubled for each failure in a row
    """
    max_tracker_backoff: float = 3600
    min_announce_interval: float = 60
    """The shortest wait between announces. A tracker that asks for a longer one gets it."""
    max_announce_interval: float = 86400
    """
    The longest wait between announces, so a tracker that asks for a huge one
    doesn't stop us announcing to it
    """
    stopped_announce_timeout: float = 5
    """How long to wait for trackers when announcing that we stopped"""
    numwant: int = 50
    """Peers to ask trackers for"""

    # ---- state ----

    cache_dir: Path = dataclasses.field(
        default_factory=lambda: platformdirs.user_cache_path("btmc")
    )
    """
    Where to keep fetched pieces when nothing says where to download to,
    in a directory for each torrent named by its info hash.
    Defaults to the user cache directory, like ``~/Library/Caches/btmc`` on macOS
    and ``~/.cache/btmc`` on Linux.
    """
    state_file: Path | None = None
    """Where to keep state between runs, like banned peers. ``None`` keeps nothing."""

    def __post_init__(self) -> None:
        # paths come from TOML as strings
        if isinstance(self.cache_dir, str):
            self.cache_dir = Path(self.cache_dir).expanduser()
        if isinstance(self.state_file, str):
            self.state_file = Path(self.state_file)

    @classmethod
    def from_toml(cls, path: Path | str | None = None, **settings: Any) -> Self:
        """
        Load settings from a TOML file, with defaults for anything it leaves out.

        Args:
            path: the file. By default, ``btmc.toml`` in the current directory if there is one,
                and all defaults if there isn't.
            settings: settings to use instead of the file's
        """
        if path is None:
            path = Path(CONFIG_FILE)
            if not path.exists():
                return cls(**settings)
        with open(path, "rb") as f:
            values = tomllib.load(f)
        unknown = values.keys() - {f.name for f in dataclasses.fields(cls)}
        if unknown:
            raise ValueError(f"unknown settings in {path}: {', '.join(sorted(unknown))}")
        return cls(**{**values, **settings})
