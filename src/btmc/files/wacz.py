"""
WACZ files inside torrents.

A WACZ is a zip of web archive data, with lists of the archived pages
and an index of every archived URL. See https://specs.webrecorder.net/wacz/latest/
"""

import json
from collections.abc import AsyncIterator
from enum import IntEnum
from typing import NotRequired, TypedDict

import anyio
import anyio.to_thread

from btmc.files.zip import TorrentZip

PAGES = "pages/pages.jsonl"
EXTRA_PAGES = "pages/extraPages.jsonl"


class LoadState(IntEnum):
    """How far Browsertrix got loading a page"""

    FAILED = 0
    CONTENT_LOADED = 1
    FULL_PAGE_LOADED = 2
    EXTRACTION_DONE = 3
    BEHAVIORS_DONE = 4


class Page(TypedDict):
    """
    One archived page.

    The WACZ spec only requires ``url`` and ``ts``. These are the fields Browsertrix writes,
    and we assume crawls come from Browsertrix.
    """

    id: str
    url: str
    ts: str
    """When the page was captured, as an RFC 3339 datetime"""
    loadState: LoadState
    title: NotRequired[str]
    mime: NotRequired[str]
    status: NotRequired[int]
    """
    The HTTP status, or Browsertrix's own codes for failures with no response:
    999 couldn't connect, 899 couldn't resolve the host.
    Missing if there's no status at all.
    """
    seed: NotRequired[bool]
    """Only there, and true, for seed pages"""
    depth: NotRequired[int]
    favIconUrl: NotRequired[str]
    text: NotRequired[str]
    """Text extracted from the page, if the crawl was asked to save it"""


class TorrentWACZ(TorrentZip):
    """A WACZ file inside a torrent"""

    async def pages(self, extra: bool = False, batch_size: int = 1000) -> AsyncIterator[list[Page]]:
        """
        The archived pages from ``pages/pages.jsonl``, ``batch_size`` at a time.
        If ``extra`` is true, then the ones from ``pages/extraPages.jsonl`` after them.

        Skips page files the WACZ doesn't have.
        """
        names = await self.namelist()
        for name in (PAGES, EXTRA_PAGES) if extra else (PAGES,):
            if name not in names:
                continue
            first = True
            async for lines in self.lines(name, batch_size):
                pages = await anyio.to_thread.run_sync(_parse_pages, lines, first)
                first = False
                if pages:
                    yield pages


def _parse_pages(lines: list[bytes], first: bool) -> list[Page]:
    records = [json.loads(line) for line in lines if line.strip()]
    # the first line of a page file is a header like {"format": "json-pages-1.0", ...}
    if first and records and "format" in records[0]:
        records = records[1:]
    for record in records:
        record["loadState"] = LoadState(record["loadState"])
    return records
