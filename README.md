# `btmc` - bittorrent microclient

~ alpha software primarily intended for use with [sciop](https://codeberg.org/Safeguarding/sciop), use at your own risk! ~

A small asyncio bittorrent client for fetching specific pieces of torrents,
and the files inside zip files inside torrents (like WACZ web archives),
without downloading the whole thing.

- Pure python on asyncio (through anyio), no native client library
- v1, v2, and hybrid torrents. Hybrids are fetched from both their v1 and v2 swarms,
  since a hybrid torrent's swarm can have v1-only and v2-only clients in it
- HTTP and UDP trackers, known peers, and incoming connections
- Fetch pieces by index, byte ranges of files, or members of zip files
- One client for many torrents, shared between jobs, with connection limits
- An fsspec filesystem: `torrent://{infohash}/{path}`, which can show zip files as directories
- Mount a torrent as a directory with FUSE, with what's in its zip files as directories too

It doesn't do DHT, PEX, magnet links, uTP, private torrents, or uploading (yet).
It's not intended to be a full-on, real-deal bittorrent client.
It's a small library to fill our specific needs where we couldn't find an existing package for it:
being able to pull slices of torrents, like files within zips within torrents,
and experimental support for fsspec over bittorrent (which will likely get split into a separate package).

It is leech-only, at least for now, so not recommended for general use!

## Usage

```python
from pathlib import Path

from torrent_models import Torrent
from btmc import Client, Config, TorrentWACZ

async with Client(config=Config(download_dir=Path("downloads"))) as client:
    t = client.add(Torrent.read("crawl.torrent"))
    # pieces by index
    await client.fetch(t, [0, 1, 2])
    # bytes of a file in the torrent
    header = await client.read(t, "crawl.wacz", 0, 1024)
    # the pages archived in a WACZ in the torrent, a batch at a time
    wacz = TorrentWACZ(client, t, "crawl.wacz")
    async for pages in wacz.pages(extra=True):
        ...
```

Through fsspec, with the `fsspec` extra:

```python
import fsspec

with fsspec.open(
    "zip://pages/pages.jsonl::torrent:///path/to/crawl.torrent/crawl.wacz",
    torrent={"download_dir": "downloads"},
) as f:
    pages = f.read()
```

From the command line, with the `cli` extra:

```shell
btmc fetch crawl.torrent downloads 0 1 2
btmc zip crawl.torrent downloads --file crawl.wacz
btmc zip crawl.torrent downloads --file crawl.wacz -x out pages/pages.jsonl
```

And mount a torrent as a directory, with the `mount` extra and FUSE
(see the docs for installing macFUSE):

```shell
btmc mount crawl.torrent ./crawl --mount-zip crawl.wacz
```
