"""Needs the ``fsspec`` extra"""

from btmc.fsspec.filesystem import TorrentFileSystem
from btmc.fsspec.registry import TorrentRegistry

__all__ = ["TorrentFileSystem", "TorrentRegistry"]
