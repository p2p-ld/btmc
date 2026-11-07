import dataclasses
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Self

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
    announce_retry: float = 300
    """How long to wait before announcing again after every tracker in a tier failed"""
    min_announce_interval: float = 60
    max_announce_interval: float = 3600
    stopped_announce_timeout: float = 5
    """How long to wait for trackers when announcing that we stopped"""
    numwant: int = 50
    """Peers to ask trackers for"""
    udp_tries: int = 3
    udp_timeout: float = 3
    """How long to wait for a UDP tracker's first answer, doubled for each try"""

    # ---- state ----

    state_file: Path | None = None
    """Where to keep state between runs, like banned peers. ``None`` keeps nothing."""

    def __post_init__(self) -> None:
        # paths come from TOML as strings
        if isinstance(self.state_file, str):
            self.state_file = Path(self.state_file)

    @classmethod
    def from_toml(cls, path: Path | str | None = None) -> Self:
        """
        Load settings from a TOML file, with defaults for anything it leaves out.

        Args:
            path: the file. By default, ``btmc.toml`` in the current directory if there is one,
                and all defaults if there isn't.
        """
        if path is None:
            path = Path(CONFIG_FILE)
            if not path.exists():
                return cls()
        with open(path, "rb") as f:
            values = tomllib.load(f)
        unknown = values.keys() - {f.name for f in dataclasses.fields(cls)}
        if unknown:
            raise ValueError(f"unknown settings in {path}: {', '.join(sorted(unknown))}")
        return cls(**values)
