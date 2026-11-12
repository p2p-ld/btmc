# API

- {class}`.Client`: coordinates torrents. It keeps track of them, and hands each incoming
  connection to its torrent.
- {class}`.Shared`: the only way torrents share anything: who we are,
  limits on connections, announces and hashing, peers banned for sending bad data,
  and trackers that aren't answering.
- {class}`.TorrentState`: one torrent. It tells its picker which pieces are wanted,
  keeps the blocks its swarm gets until their piece is complete, checks and stores the
  piece, and works out who to ban when a piece fails.
- {class}`.PiecePicker`: the state of every piece: whether it's wanted, and whether it's
  open, downloading, checking, or had, and who to tell when that changes. It picks which
  block to ask a peer for next, and lets callers wait for pieces. No I/O, and it knows
  nothing about peers.
- {class}`.Swarm`: the torrent's peers: finding them through trackers, connecting to them,
  asking them for the blocks the picker picks, passing the blocks on,
  and closing connections to peers that get banned.
- {class}`.Peer`: one connection: opening it, sending and receiving messages, noticing when
  the peer has gone quiet for too long, and closing it.
- {mod}`.messages`: every peer and tracker message, encoded and decoded.
- {func}`.announce`: one announce to a tracker.

```{toctree}
:maxdepth: 2

protocol/index
files/index
fsspec/index
config
storage
types
exceptions
utils
```
