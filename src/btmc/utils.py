"""Telling torrents apart by their info hashes"""

from typing import Any, cast

from torrent_models import Torrent
from torrent_models.types import TorrentVersion


def wire_hash(info_hash: str | bytes) -> bytes:
    """
    Any info hash, as bytes or hex, v1 or v2, whole or truncated,
    as the 20 bytes used on the wire
    """
    if isinstance(info_hash, str):
        info_hash = bytes.fromhex(info_hash)
    return info_hash[:20]


def info_hashes(torrent: Torrent[Any]) -> list[bytes]:
    """
    The info hashes of the torrent's swarms, as used in handshakes and announces,
    see :func:`.wire_hash`.

    v2 first, so peers that turn up in both of a hybrid's swarms get the v2 hash.
    """
    hashes = []
    if torrent.torrent_version != TorrentVersion.v1:
        hashes.append(wire_hash(cast(str, torrent.v2_infohash)))
    if torrent.torrent_version != TorrentVersion.v2:
        hashes.append(wire_hash(cast(str, torrent.v1_infohash)))
    return hashes


def torrent_name(torrent: Torrent[Any]) -> str:
    """The v2 info hash if there is one, else the v1 info hash, in hex, like sciop names torrents"""
    return cast(str, torrent.v2_infohash or torrent.v1_infohash)
