# Changelog

## v0.0.*

### v0.0.1 - Start of package

- Fetch chosen pieces of v1, v2, and hybrid torrents, from HTTP and UDP trackers,
  known peers, and incoming connections.
- Read byte ranges of files in torrents, and members of zip and WACZ files in torrents,
  fetching only the pieces that hold them.
- {class}`.Client` fetches many torrents at once, with limits on connections and announces.
- {class}`.TorrentRegistry` finds .torrent files by info hash.
- {class}`.TorrentFileSystem`, an fsspec filesystem: `torrent://{infohash}/{path}`
  and `torrent://{path to .torrent}/{path}`. It can show zip files as directories of
  what's in them, and has async files and how much of each file has been fetched.
- `btmc mount` mounts a torrent as a directory with FUSE, with zip files in it as directories too,
  and a tag in macOS Finder's Get Info showing what's on disk.
- {attr}`.Config.download_dir`, where torrents go unless they're given a directory,
  defaults to the user cache directory.
