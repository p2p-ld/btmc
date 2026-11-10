# `btmc` - bittorrent microclient

~ alpha software primarily intended for use with [sciop](https://codeberg.org/Safeguarding/sciop) ~

A small asyncio bittorrent client for fetching specific pieces of torrents,
and the files inside zip files inside torrents (like WACZ web archives),
without downloading the whole thing.

- Pure python on asyncio (through anyio), no native client library
- v1, v2, and hybrid torrents. Hybrids are fetched from both their v1 and v2 swarms,
  since a hybrid torrent's swarm can have v1-only and v2-only clients in it
- HTTP and UDP trackers, known peers, and incoming connections
- Fetch pieces by index, byte ranges of files, or members of zip files
- One client for many torrents, shared between jobs, with connection limits
- An fsspec filesystem: `torrent://{infohash}/{path}`

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
from btmc import Client, TorrentWACZ

async with Client(Path("downloads")) as client:
    t = client.add(Torrent.read("crawl.torrent"))
    # pieces by index
    await client.fetch(t, [0, 1, 2])
    # bytes of a file in the torrent
    header = await client.read(t, "crawl.wacz", 0, 1024)
    # files inside a zip in the torrent
    wacz = TorrentWACZ(client, t, "crawl.wacz")
    pages = await wacz.pages()
```

Through fsspec, with the `fsspec` extra:

```python
import fsspec

with fsspec.open(
    "zip://pages/pages.jsonl::torrent:///path/to/crawl.torrent/crawl.wacz",
    torrent={"cache_dir": "downloads"},
) as f:
    pages = f.read()
```

From the command line, with the `cli` extra:

```shell
btmc fetch crawl.torrent downloads 0 1 2
btmc zip crawl.torrent downloads --file crawl.wacz
btmc zip crawl.torrent downloads --file crawl.wacz -x out pages/pages.jsonl
```
