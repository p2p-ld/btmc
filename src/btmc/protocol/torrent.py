"""One torrent: asking for pieces, and checking and storing pieces as they arrive"""
from __future__ import annotations

import hashlib
import logging
import math
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import anyio
from torrent_models import Torrent

from btmc.protocol.picker import PiecePicker, PieceState
from btmc.protocol.shared import Shared
from btmc.protocol.swarm import Swarm
from btmc.storage import PieceStore
from btmc.types import Addr, Block, BlockOffset, PieceIndex

log = logging.getLogger(__name__)


class TorrentState:
    """
    One torrent being fetched.

    Its :class:`.PiecePicker` keeps the state of every piece: which are wanted, and which
    we have. Its :class:`.Swarm` fetches the wanted pieces from peers. The torrent keeps
    the blocks that arrive until their piece is complete, checks the piece against its hash,
    and writes the good ones to its :class:`.PieceStore`. When a piece fails,
    its :class:`.SmartBan` works out who sent bad data, and they're banned.

    Args:
        torrent: the torrent
        root: download directory. Files go where a regular client would put them.
        shared: what this torrent shares with the client's other torrents
        trackers: tracker tiers to use instead of the torrent's own
        known_peers: peers to connect to without asking a tracker
    """

    def __init__(
        self,
        torrent: Torrent[Any],
        root: Path,
        shared: Shared,
        *,
        trackers: list[list[str]] | None = None,
        known_peers: Sequence[Addr] = (),
    ):
        self.torrent = torrent
        self.layout = torrent.layout
        self.n_pieces = self.layout.n_pieces
        self.name = torrent_name(torrent)
        self.shared = shared
        self.store = PieceStore(root, self.n_pieces, self.name)
        self.smart_ban = SmartBan()
        self._blocks: dict[PieceIndex, dict[BlockOffset, tuple[bytes, Addr]]] = {}
        """The blocks of pieces being downloaded or waiting to be checked, and who sent them"""
        self._to_check, self._checks = anyio.create_memory_object_stream[PieceIndex](math.inf)
        self._have_lock = anyio.Lock()
        """Held while saving that we have a piece, see :meth:`._check`"""

        self.picker = PiecePicker(
            self.layout, shared.config.max_pieces_checking, self.store.load_have()
        )
        self.picker.add_callback(PieceState.CHECKING, self._queue_check)
        self.swarm = Swarm(torrent, self.picker, shared, trackers=trackers, known_peers=known_peers)
        self.swarm.add_callback("block", self._block_arrived)
        self.info_hashes = self.swarm.info_hashes

        self.downloaded = 0
        self.hash_failures = 0

    # ---- fetching ----

    def want(self, pieces: Iterable[PieceIndex]) -> None:
        """
        Fetch pieces, in the order given, after the ones wanted already.
        Raises IndexError for any not in the torrent.
        """
        self.picker.want(pieces)

    async def fetch(self, pieces: Iterable[PieceIndex]) -> None:
        """
        Fetch pieces, returning once all of them are checked and written.
        A caller that stops waiting doesn't stop them being fetched.
        """
        await self.picker.wait(pieces)

    # ---- running ----

    async def run(self) -> None:
        """Fetch what's wanted, until stopped"""
        try:
            async with anyio.create_task_group() as tg:
                tg.start_soon(self._check_loop)
                await self.swarm.run()
                tg.cancel_scope.cancel()
        finally:
            self.picker.close()

    async def stop(self) -> None:
        """Stop fetching, and fail anything still waiting"""
        self.picker.close()
        await self.swarm.stop()

    # ---- checking pieces ----

    def _block_arrived(self, block: Block, data: bytes, sender: Addr) -> None:
        if self.picker.received(block):
            self._blocks.setdefault(block.piece, {})[block.begin] = (data, sender)

    def _queue_check(self, pieces: list[PieceIndex]) -> None:
        # this runs before the piece's last block is stored,
        # but the check loop only gets to it afterwards, in its own task
        for index in pieces:
            self._to_check.send_nowait(index)

    async def _check_loop(self) -> None:
        """Check complete pieces as they come, as many at once as the client's hashing allows"""
        async with anyio.create_task_group() as tg:
            async for index in self._checks:
                tg.start_soon(self._check, ReceivedPiece(index, self._blocks.pop(index, {})))

    async def _check(self, piece: ReceivedPiece) -> None:
        """Check a complete piece against its hash, write it if it's good, and record which"""
        piece_range = self.layout.piece_range(piece.index)
        blocks = piece.data()
        # the picker doesn't ask for the padfiles at the end
        if piece_range.padding:
            blocks.append(bytes(piece_range.padding))
        ok = await anyio.to_thread.run_sync(
            piece_range.validate_data, blocks, limiter=self.shared.hashing
        )
        if ok:
            # pieces never overlap, so they can be written in any order, at the same time
            await anyio.to_thread.run_sync(self.store.write, piece_range, blocks)
            # saved before the piece passes, so whoever's waiting for it can rely on it
            async with self._have_lock:
                await anyio.to_thread.run_sync(self.store.save_have, piece.index)
        self._verified(piece, ok)

    def _verified(self, piece: ReceivedPiece, ok: bool) -> None:
        """
        Record whether a piece passed its check, banning whoever sent bad data
        before a failed piece is fetched again
        """
        for addr in self.smart_ban.checked(piece, ok):
            self.shared.ban(addr)
        if ok:
            self.downloaded += sum(len(block) for block in piece.data())
            self.picker.passed(piece.index)
        else:
            self.hash_failures += 1
            log.warning("piece %d failed its hash check, from %s", piece.index, piece.senders)
            self.picker.failed(piece.index)

@dataclass
class ReceivedPiece:
    """A piece whose blocks have all arrived, to be checked"""

    index: PieceIndex
    blocks: dict[BlockOffset, tuple[bytes, Addr]]
    """Each block, and who sent it"""

    def data(self) -> list[bytes]:
        """The blocks, in order"""
        return [self.blocks[begin][0] for begin in sorted(self.blocks)]

    @property
    def senders(self) -> set[Addr]:
        return {sender for _, sender in self.blocks.values()}


class SmartBan:
    """
    Works out who sent bad data, from the pieces that fail their hash check,
    like libtorrent's smart ban plugin.

    A failed piece with only one sender is that peer's fault. When a failed piece has several,
    we remember a hash of each block and who sent it. A peer is to blame if it sends
    the same block again with different data, or if the piece passes later
    and the block it sent before doesn't match.
    """

    def __init__(self) -> None:
        self._failed: dict[Block, dict[Addr, bytes]] = {}
        """Block -> who sent it in a failed piece -> the hash of what they sent"""

    def checked(self, piece: ReceivedPiece, ok: bool) -> set[Addr]:
        """Record a piece's hash check, returning the peers to blame"""
        if not ok and len(piece.senders) == 1:
            return piece.senders
        blamed = set()
        for begin, (block, sender) in piece.blocks.items():
            key = Block(piece.index, begin, len(block))
            if ok:
                if sent := self._failed.pop(key, None):
                    digest = hashlib.sha1(block).digest()
                    blamed |= {addr for addr, before in sent.items() if before != digest}
            else:
                digest = hashlib.sha1(block).digest()
                sent = self._failed.setdefault(key, {})
                if sent.get(sender, digest) != digest:
                    blamed.add(sender)
                sent[sender] = digest
        return blamed



def torrent_name(torrent: Torrent[Any]) -> str:
    """The v2 info hash if there is one, else the v1 info hash, in hex, like sciop names torrents"""
    return cast(str, torrent.v2_infohash or torrent.v1_infohash)
