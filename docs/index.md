# `btmc` - bittorrent microclient

A small asyncio bittorrent client that can be used as an application 
but also embedded as a library within other projects!

This package was created for, and used by [sciop <3](https://sciop.net/)!!!
We wrote it for fetching specific pieces of torrents,
and the files inside zip files inside torrents (like WACZ web archives),
without downloading the whole thing.

We looked around and couldn't find other clients that could do these things:

- Pure python on asyncio (through anyio), no native client library - libtorrent loves to segfault!
- Support for v1, v2, and hybrid torrents.
- Fetch pieces by index, byte ranges of files, or members of zip files
- Simple connection and resource limits through the `Client`

And while we were at it we added these fun little extras:

- An [fsspec filesystem](./filesystem.md): `torrent://{infohash}/{path}`,
  which can show zip files as directories of what's in them
- [Mount a torrent](./filesystem.md#mounting) as a directory with FUSE,
  with what's in its zip files as directories too

It doesn't do DHT, PEX, magnet links, uTP, private torrents, or uploading (yet).
See [BEP support](./beps.md) for the details.

## Example

Add torrents to a {class}`.Client`, then fetch from them as many times as you need.
The client keeps peer connections open between fetches,
and tears everything down at the end.

```python
from pathlib import Path

from torrent_models import Torrent
from btmc import Client, Config, TorrentWACZ, TorrentZip

# torrents go in downloads/{info hash}, unless add() is given a directory
async with Client(config=Config(download_dir=Path("downloads"))) as client:
    t = client.add(Torrent.read("crawl.torrent"))

    # pieces by index
    await client.fetch(t, [0, 1, 2])

    # bytes of a file in the torrent
    header = await client.read(t, "crawl.wacz", 0, 1024)

    # members of a zip in the torrent
    tzip = TorrentZip(client, t, "crawl.wacz")
    names = await tzip.namelist()
    datapackage = await tzip.read("datapackage.json")

    # lines of a big member, a batch at a time
    async for lines in tzip.lines("logs/crawl.log", 1000):
        ...

    # iterate batches of pages from a wacz file
    wacz = TorrentWACZ(client, t, "crawl.wacz")
    async for pages in wacz.pages(extra=True):
        ...
```

### Many torrents

One client fetches any number of torrents at once, with one peer id, HTTP client,
listening port, and connection limit.

```python
async with Client() as client:
    a = client.add(Torrent.read("a.torrent"))
    b = client.add(Torrent.read("b.torrent"))
    await asyncio.gather(client.fetch(a, [0]), client.read(b, "b.wacz", 0, 1024))
```

A torrent only announces and connects to new peers while something is wanted from it,
so its trackers only send peers our way while we'd take their connections,
and it keeps its connections until it's removed.
This lets us properly handle connections from NAT-ted peers,
who only try to connect in once we announce.
When all the connections are in use, a torrent that wants pieces takes connections
from torrents that don't.

### Configuration

Limits, timeouts, listening, and where torrents go are set with a {class}`.Config`.
{meth}`.Config.from_toml` reads them from a TOML file, `btmc.toml` in the current directory
by default, with defaults for anything it leaves out.
The CLI and the fsspec filesystem read `btmc.toml` if there is one.

```toml
max_connections = 50
max_peers = 10
listen_host = "0.0.0.0"
port = 6881
# where torrents go, unless they're given a directory,
# by default the user cache directory
download_dir = "~/btmc"
# where to keep which pieces we have, by default the user state directory
state_dir = "~/.btmc-state"
# keep banned peers between runs
state_file = "btmc-state.json"
```

```python
from btmc import Client, Config

async with Client(config=Config.from_toml()) as client:
    ...
```

## Installation

From pypi

```shell
python -m pip install btmc
```

From git

```shell
git clone https://github.com/p2p-ld/btmc
cd btmc
python -m venv .venv
source .venv/bin/activate
python -m pip install -e .
```

### Extras

Extras add the [CLI](./cli.md) (`cli`), the [fsspec filesystem](./filesystem.md) (`fsspec`),
and [mounting](./filesystem.md#mounting) (`mount`, which also needs FUSE):

```shell
python -m pip install -e '.[cli,mount]'
```

- `cli`: use btmc from the command line
- `fsspec`: use a torrent as an fsspec filesystem
- `mount`: mount a torrent to a local filesystem

```{toctree}
:caption: Usage:
:hidden:

cli
filesystem
```

```{toctree}
:caption: API:
:hidden:

api/index
```

```{toctree}
:maxdepth: 2
:caption: Reference:
:hidden:

beps
changelog
```
