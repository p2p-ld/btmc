# BEP support

How much of each [BitTorrent Enhancement Proposal](https://www.bittorrent.org/beps/bep_0000.html)
btmc supports. btmc only downloads (for now), so ✅ means everything a downloading client needs

✅ yes · ☑️ mostly · 🟡 partly · ❌ no · ➖ doesn't apply

## General

| BEP | Title | BEP status | Support | Notes |
|----:|-------|------------|---------|-------|
| [3](https://www.bittorrent.org/beps/bep_0003.html) | The BitTorrent Protocol Specification | Final | ☑️ | Reads .torrent files, talks to HTTP trackers and to peers, and checks every piece. It never uploads, so it never unchokes peers or tells them which pieces it has. It doesn't send `completed`: it fetches parts of torrents, so it says `stopped` when nothing more is wanted and `started` when something is again. |
| [4](https://www.bittorrent.org/beps/bep_0004.html) | Assigned Numbers | Active | ✅ | Uses the core message ids 0–8 as assigned, and sets none of the reserved handshake bits. |
| [20](https://www.bittorrent.org/beps/bep_0020.html) | Peer ID Conventions | Active | ✅ | Peer ids are `-SC0001-` and 12 random bytes. |
| [40](https://www.bittorrent.org/beps/bep_0040.html) | Canonical Peer Priority | Draft | ❌ | Peers with the fewest failed connections are tried first. |

## DHT

| BEP | Title | BEP status | Support | Notes |
|----:|-------|------------|---------|-------|
| [5](https://www.bittorrent.org/beps/bep_0005.html) | DHT Protocol | Accepted | ❌ | Peers come only from trackers, incoming connections, and peers you give it. |
| [32](https://www.bittorrent.org/beps/bep_0032.html) | BitTorrent DHT Extensions for IPv6 | Draft | ❌ | No DHT. |
| [33](https://www.bittorrent.org/beps/bep_0033.html) | DHT Scrapes | Draft | ❌ | No DHT. |
| [42](https://www.bittorrent.org/beps/bep_0042.html) | DHT Security extension | Draft | ❌ | No DHT. |
| [43](https://www.bittorrent.org/beps/bep_0043.html) | Read-only DHT Nodes | Draft | ❌ | No DHT. |
| [44](https://www.bittorrent.org/beps/bep_0044.html) | Storing arbitrary data in the DHT | Draft | ❌ | No DHT. |
| [45](https://www.bittorrent.org/beps/bep_0045.html) | Multiple-address operation for the BitTorrent DHT | Draft | ❌ | No DHT. |
| [46](https://www.bittorrent.org/beps/bep_0046.html) | Updating Torrents Via DHT Mutable Items | Draft | ❌ | No DHT. |
| [49](https://www.bittorrent.org/beps/bep_0049.html) | Distributed Torrent Feeds | Draft | ❌ | No DHT. |
| [50](https://www.bittorrent.org/beps/bep_0050.html) | Publish/Subscribe Protocol | Draft | ❌ | No DHT. |
| [51](https://www.bittorrent.org/beps/bep_0051.html) | DHT Infohash Indexing | Draft | ❌ | No DHT. |

## Trackers

| BEP | Title | BEP status | Support | Notes |
|----:|-------|------------|---------|-------|
| [7](https://www.bittorrent.org/beps/bep_0007.html) | IPv6 Tracker Extension | Draft | 🟡 | Reads IPv6 peers from HTTP (`peers6`) and UDP trackers. Announces once per tracker, over whichever address family connects, rather than once per local IPv4 and IPv6 address, and doesn't send a `key`. |
| [8](https://www.bittorrent.org/beps/bep_0008.html) | Tracker Peer Obfuscation | Deferred | ❌ | |
| [12](https://www.bittorrent.org/beps/bep_0012.html) | Multitracker Metadata Extension | Accepted | 🟡 | Uses every tracker in `announce` and `announce-list`, but ignores the order of processing: it announces to every tracker in every tier at once, like qBittorrent, rather than trying tiers in order, shuffling them, and moving working trackers to the front. A tracker that doesn't answer is backed off for every torrent. |
| [15](https://www.bittorrent.org/beps/bep_0015.html) | UDP Tracker Protocol for BitTorrent | Accepted | ☑️ | Announces over IPv4 and IPv6, and reads tracker errors. Doesn't scrape, and gets a new connection id for every announce. Rather than resending a request on the BEP's schedule, it sends each one once, and backs off a tracker that doesn't answer, for every torrent, waiting twice as long after each failure in a row. |
| [22](https://www.bittorrent.org/beps/bep_0022.html) | BitTorrent Local Tracker Discovery Protocol | Deferred | ❌ | |
| [23](https://www.bittorrent.org/beps/bep_0023.html) | Tracker Returns Compact Peer Lists | Accepted | ✅ | Asks for compact lists, and reads both compact lists and lists of dictionaries. |
| [24](https://www.bittorrent.org/beps/bep_0024.html) | Tracker Returns External IP | Draft | ❌ | `external ip` is ignored. |
| [31](https://www.bittorrent.org/beps/bep_0031.html) | Failure Retry Extension | Draft | ❌ | `retry in` is ignored. A tracker that refuses a torrent is asked again after 5 minutes. |
| [34](https://www.bittorrent.org/beps/bep_0034.html) | DNS Tracker Preferences | Draft | ❌ | |
| [41](https://www.bittorrent.org/beps/bep_0041.html) | UDP Tracker Protocol Extensions | Draft | ❌ | The path and query of a `udp://` tracker url aren't sent. |
| [48](https://www.bittorrent.org/beps/bep_0048.html) | Tracker Protocol Extension: Scrape | Draft | ❌ | |

## Peer extensions

| BEP | Title | BEP status | Support | Notes |
|----:|-------|------------|---------|-------|
| [6](https://www.bittorrent.org/beps/bep_0006.html) | Fast Extension | Accepted | ❌ | Not offered. If a peer sends its messages anyway, they're ignored rather than closing the connection. |
| [9](https://www.bittorrent.org/beps/bep_0009.html) | Extension for Peers to Send Metadata Files | Accepted | ❌ | Needs a .torrent file. No magnet links. |
| [10](https://www.bittorrent.org/beps/bep_0010.html) | Extension Protocol | Accepted | ❌ | Not offered, and extension messages from peers are ignored. |
| [11](https://www.bittorrent.org/beps/bep_0011.html) | Peer Exchange (PEX) | Accepted | ❌ | |
| [16](https://www.bittorrent.org/beps/bep_0016.html) | Superseeding | Draft | ➖ | Only about seeding. |
| [21](https://www.bittorrent.org/beps/bep_0021.html) | Extension for partial seeds | Draft | ❌ | btmc never uploads, so it's never a partial seed. |
| [28](https://www.bittorrent.org/beps/bep_0028.html) | Tracker exchange extension | Deferred | ❌ | |
| [54](https://www.bittorrent.org/beps/bep_0054.html) | The lt_donthave extension | Draft | ❌ | |
| [55](https://www.bittorrent.org/beps/bep_0055.html) | Holepunch extension | Accepted | ❌ | Needs the extension protocol and uTP. |

## Torrent file extensions and magnet links

| BEP | Title | BEP status | Support | Notes |
|----:|-------|------------|---------|-------|
| [27](https://www.bittorrent.org/beps/bep_0027.html) | Private Torrents | Accepted | ❌ | Private torrents aren't supported yet. The private flag isn't read, and btmc announces to all of a torrent's trackers at once, where a private torrent should use one at a time. |
| [30](https://www.bittorrent.org/beps/bep_0030.html) | Merkle hash torrent extension | Draft | ❌ | This older format is unrelated to v2 torrents' hash trees, which are supported (BEP 52). |
| [35](https://www.bittorrent.org/beps/bep_0035.html) | Torrent Signing | Draft | ❌ | |
| [38](https://www.bittorrent.org/beps/bep_0038.html) | Finding Local Data Via Torrent File Hints | Draft | ❌ | |
| [39](https://www.bittorrent.org/beps/bep_0039.html) | Updating Torrents Via Feed URL | Draft | ❌ | |
| [47](https://www.bittorrent.org/beps/bep_0047.html) | Padding files and extended file attributes | Draft | 🟡 | Padding files are left out of file listings, aren't downloaded, and aren't written to disk. The executable, hidden and symlink attributes are ignored. |
| [52](https://www.bittorrent.org/beps/bep_0052.html) | The BitTorrent Protocol Specification v2 | Draft | 🟡 | Downloads v2 and hybrid torrents, checking each piece against the hashes in the .torrent file. Doesn't ask peers for hashes or answer their requests for them, and doesn't use the handshake bit that moves a hybrid's connection to v2: hybrids are fetched from both swarms instead. Hybrid pieces are only checked against their v2 hashes, and when a torrent is read, `meta version` isn't checked and piece layers aren't checked against their files' `pieces root`. |
| [53](https://www.bittorrent.org/beps/bep_0053.html) | Magnet URI extension - Select specific file indices for download | Draft | ❌ | No magnet links, though files and byte ranges can be fetched from a .torrent. |

## Local peer discovery

| BEP | Title | BEP status | Support | Notes |
|----:|-------|------------|---------|-------|
| [14](https://www.bittorrent.org/beps/bep_0014.html) | Local Service Discovery | Accepted | ❌ | |
| [25](https://www.bittorrent.org/beps/bep_0025.html) | An Alternate BitTorrent Cache Discovery Protocol | Draft | ❌ | |
| [26](https://www.bittorrent.org/beps/bep_0026.html) | Zeroconf Peer Advertising and Discovery | Deferred | ❌ | |

## Web seeds

| BEP | Title | BEP status | Support | Notes |
|----:|-------|------------|---------|-------|
| [17](https://www.bittorrent.org/beps/bep_0017.html) | HTTP Seeding | Draft | ❌ | |
| [19](https://www.bittorrent.org/beps/bep_0019.html) | WebSeed - HTTP/FTP Seeding (GetRight style) | Accepted | ❌ | Web seeds in the .torrent file are ignored. |

## Transport

| BEP | Title | BEP status | Support | Notes |
|----:|-------|------------|---------|-------|
| [29](https://www.bittorrent.org/beps/bep_0029.html) | uTorrent transport protocol | Accepted | ❌ | TCP only. |
| [37](https://www.bittorrent.org/beps/bep_0037.html) | Anonymous BitTorrent over proxies | Draft | ❌ | No proxy support. |

## Other

| BEP | Title | BEP status | Support | Notes |
|----:|-------|------------|---------|-------|
| [18](https://www.bittorrent.org/beps/bep_0018.html) | Search Engine Specification | Deferred | ❌ | |
| [36](https://www.bittorrent.org/beps/bep_0036.html) | Torrent RSS feeds | Draft | ❌ | |
