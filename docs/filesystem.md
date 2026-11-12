# fsspec filesystem

With the `fsspec` extra, the files in torrents are an [fsspec](https://filesystem-spec.readthedocs.io)
filesystem, so anything that reads from fsspec can read from torrents,
fetching only the pieces it reads.

```shell
python -m pip install 'btmc[fsspec]'
```

## Paths

- `torrent://{infohash}/{path}` - a file in the torrent with that info hash (v1, v2, or v2
  truncated to 20 bytes), whose .torrent file is found by a {class}`.TorrentRegistry`
  in the `torrent_dirs` or `torrents` options
- `torrent://{path to a .torrent file}/{path}` - a file in the torrent described by that .torrent file.
  The path to the .torrent file ends at the first part that ends with `.torrent`.

Magnet links (`magnet://`) aren't supported yet.

The root, `torrent://`, lists every torrent the registry can find,
named by their v2 info hash if they have one, else their v1 info hash, like sciop does.

## Example

```python
import fsspec

fs = fsspec.filesystem(
    "torrent",
    # where fetched pieces go, by default the config's download_dir
    download_dir="~/btmc",
    # find .torrent files by info hash, e.g. sciop's {infohash}/{name}.torrent layout
    torrent_dirs=["/srv/sciop/torrents"],
)

# listing a torrent reads the .torrent file, and doesn't need any peers
fs.ls("torrent://8a19577fb5f690970ca43a57ff1011ae202244b8c1d83a3d0545ab2a0df7e6a0")

# files are fetched a piece at a time as they're read
with fs.open("torrent:///srv/sciop/torrents/.../crawl.torrent/crawl.wacz") as f:
    header = f.read(1024)
```

fsspec's zip filesystem can read members of a zip in a torrent,
like the page list in a WACZ web archive,
fetching the zip's index and then just the pieces the member is in:

```python
import fsspec

with fsspec.open(
    "zip://pages/pages.jsonl::torrent://8a19577f.../crawl.wacz",
    torrent={"torrent_dirs": ["/srv/sciop/torrents"]},
) as f:
    pages = f.read()
```

For example, reading the zip index, page list, CDX index, and `datapackage.json`
of an 8 GB WACZ this way fetches 3 of its 7638 pieces.

### Zips as directories

With `zips`, zip files in torrents are also shown as a directory of what's in them,
next to the zip with `.d` on the end of its name.
`zips` are patterns of paths within torrents, like `"*.zip"` or `"crawl.wacz"`:

```python
fs = fsspec.filesystem("torrent", torrent_dirs=["/srv/sciop/torrents"], zips=["*.wacz"])
fs.ls("torrent://8a19577f.../crawl.wacz.d/pages")
pages = fs.cat_file("torrent://8a19577f.../crawl.wacz.d/pages/pages.jsonl")
```

Unlike `zip://`, each open file in a zip reads through a file object of its own,
so reading one never waits on another.
A zip's index is read the first time its directory is listed or looked in.

### Async files and status

{meth}`.TorrentFileSystem.open_async` opens a file on the event loop.
Its `fetch` says when the file's pieces are fetched: as they're read, by default,
or all of them as soon as it's opened. {meth}`.AsyncTorrentFile.read_at` reads at an offset
without moving the file's position, so several reads can run at once.
{meth}`.TorrentFileSystem.status` says how much of a file has been fetched.

## How it works

{class}`.TorrentFileSystem` is an fsspec `AsyncFileSystem`.
fsspec runs it on its own event loop, in a background thread,
and makes the synchronous methods like `ls`, `cat_file`, and `open` from the async ones.

The first time it needs a peer, it starts a {class}`.Client` on that loop,
which adds each torrent the first time it's read, and keeps it until the filesystem
is closed.
Fetched pieces are kept in `download_dir`, or by default the config's
{attr}`.Config.download_dir`, the user cache directory, so anything that's been read once is read from disk after that.

Open files are fsspec buffered files without an in-memory cache by default,
since every piece that's fetched is already kept on disk:
each read fetches just the pieces under it. Files from
{meth}`.TorrentFileSystem.open_async` don't buffer either.

## Mounting

With the `mount` extra and FUSE, `btmc mount` mounts a torrent as a read-only directory,
fetching files a piece at a time as they're read:

```shell
python -m pip install 'btmc[cli,mount]'
btmc mount crawl.torrent ./crawl --mount-zip crawl.wacz
```

A zip file is there as a file, and next to it is a directory named like the zip
with `.d` on the end, like `crawl.wacz.d/`, with what's in the zip in it.
Files ending in `.zip` are shown this way, and so are any given with `--mount-zip`.
What's in a zip is read from its index the first time its directory is opened,
so browsing to a zip only fetches the pieces at the end of it.

By default, reading a file fetches just the pieces under what's read, when it's read.
With `--fetch whole-file`, opening a file asks for all of its pieces at once,
so they download in parallel, and they keep downloading after it's closed.
Finder's previews open files just from browsing to them,
so with `whole-file`, looking in a folder of big files starts downloading them.

On macOS, each file has a tag for how much of it has been fetched,
"On disk", "Partly fetched", or "Not fetched", shown in Finder's Get Info.
Finder doesn't draw tags as dots in its windows on FUSE volumes and we're not exactly sure why - 
sometimes it does, sometimes it doesn't, who knows.

Pieces are kept in `--download-dir`, in a directory for each torrent named by its info hash.
By default that's `download_dir` in `btmc.toml`, or the user cache directory,
like `~/Library/Caches/btmc` on macOS and `~/.cache/btmc` on Linux.
`--clear-cache` deletes that directory first, to start from nothing.

Ctrl-C unmounts. On macOS it unmounts even while an app has a file in the mount open,
since FSKit volumes can't refuse.

### Installing FUSE

- **macOS**: [macFUSE](https://macfuse.github.io) 5.2 or newer. btmc mounts with its FSKit backend,
  so the kernel extension the installer asks about isn't needed. Instead, switch on macFUSE in
  System Settings, under General > Login Items & Extensions > File System Extensions.
  If macFUSE isn't listed there, register it with
  `pluginkit -a /Library/Filesystems/macfuse.fs/Contents/Resources/macfuse.app/Contents/Extensions/io.macfuse.app.fsmodule.macfuse.appex`.
- **Linux**: libfuse 2 or 3, from your package manager.

### How mounting works

The mount is the torrent's directory in a {class}`.TorrentFileSystem`, with `"*.zip"`
and any `--mount-zip` paths as its `zips`. FUSE calls the operations in {mod}`btmc.fsspec.fuse`
from threads of its own, which run the filesystem's async methods on its event loop
with fsspec's `sync`. Open files are {meth}`.TorrentFileSystem.open_async` files.
Members of zips are read with {meth}`.TorrentZip.open_member`,
each through a file object of its own, so reading one never waits on another:
uncompressed ones, like the WARCs in a WACZ, seek straight to what's read,
and compressed ones are unzipped up to it.

## API

See {mod}`btmc.fsspec.filesystem`, {mod}`btmc.fsspec.registry` and {mod}`btmc.fsspec.fuse`.
