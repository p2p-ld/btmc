"""One torrent: asking for pieces, and checking and storing pieces as they arrive"""
from __future__ import annotations

import hashlib
import logging
import math
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import anyio
from anyio.streams.buffered import BufferedByteStream
from torrent_models import Torrent

from btmc.protocol.messages import Bitfield, Handshake
from btmc.protocol.picker import PiecePicker, PieceState
from btmc.protocol.shared import Shared
from btmc.protocol.swarm import Swarm
from btmc.storage import PieceStore
from btmc.types import Addr, Block, BlockOffset, PieceIndex
from btmc.utils import torrent_name

log = logging.getLogger(__name__)


class TorrentState:
    """
    One torrent being fetched.

    Its :class:`.PiecePicker` keeps the state of every piece: which are wanted, and which
    we have. Its :class:`.Swarm` fetches the wanted pieces from peers. The torrent keeps
    the blocks that arrive until their piece is complete, checks the piece against its hash,
    and writes the good ones to its :class:`.PieceStore`. When a piece fails,
    its :class:`.SmartBan` works out who sent bad data, and they're banned.

    If there's no resume file saying which pieces we have, whatever is already on disk,
    like files from another client, is checked before anything is fetched.

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

        have = self.store.load_have()
        self._disk_check = anyio.CancelScope() if have is None else None
        """
        The check of what's on disk, when there's no resume file,
        so :meth:`.stop` can cut it short
        """
        self.picker = PiecePicker(self.layout, shared.config.max_pieces_checking, have)
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

    @property
    def have(self) -> Bitfield:
        """A copy of the pieces that are checked and written"""
        return Bitfield(self.n_pieces, bytes(self.picker.have.bits))

    async def fetch(self, pieces: Iterable[PieceIndex]) -> None:
        """
        Fetch pieces, returning once all of them are checked and written.
        A caller that stops waiting doesn't stop them being fetched.
        """
        await self.picker.wait(pieces)

    async def read(self, path: str, start: int, end: int) -> bytes:
        """Bytes [start, end) of a file in the torrent, fetching whatever pieces hold them"""
        file = self.layout.file(path)
        end = min(end, file.length)
        pieces = self.layout.byte_range_pieces(path, start, end)
        await self.fetch(piece.piece_idx for piece in pieces)
        return await anyio.to_thread.run_sync(self.store.read, file.download_path, start, end)

    async def fetch_file(self, path: str) -> Path:
        """
        Fetch every piece of a file in the torrent, and return where it is on disk.
        Empty files have no pieces, so they're never written, and might not be there.
        """
        file = self.layout.file(path)
        pieces = self.layout.byte_range_pieces(path, 0, file.length)
        await self.fetch(piece.piece_idx for piece in pieces)
        return self.store.path(file.download_path)

    # ---- running ----

    async def run(self) -> None:
        """Check what's on disk if there's no resume file, then fetch what's wanted until stopped"""
        try:
            if self._disk_check is not None:
                with self._disk_check:
                    await self._check_disk()
            async with anyio.create_task_group() as tg:
                tg.start_soon(self._check_loop)
                await self.swarm.run()
                tg.cancel_scope.cancel()
        finally:
            self.picker.close()
            self._to_check.close()
            self._checks.close()

    async def stop(self) -> None:
        """Stop fetching, and fail anything still waiting"""
        self.picker.close()
        if self._disk_check is not None:
            self._disk_check.cancel()
        await self.swarm.stop()

    def accept_connection(
        self, connection: BufferedByteStream, addr: Addr, handshake: Handshake
    ) -> bool:
        """See :meth:`.Swarm.accept_connection`"""
        return self.swarm.accept_connection(connection, addr, handshake)

    # ---- checking pieces ----

    async def _check_disk(self) -> None:
        """
        Check whatever data is on disk already, and start the resume file
        with the pieces that pass. Only written once every piece has been looked at,
        so a check that's cut short starts again next time.
        """
        paths = [f.download_path for f in self.torrent.files]
        if await anyio.to_thread.run_sync(self.store.any_exist, paths):
            log.info("no resume file for %s, checking the data already on disk", self.name)
            for index in range(self.n_pieces):
                if await anyio.to_thread.run_sync(
                    self._on_disk, index, limiter=self.shared.hashing
                ):
                    self.picker.found([index])
            log.info("found %d of %d pieces on disk", self.picker.have.count(), self.n_pieces)
        await anyio.to_thread.run_sync(self.store.create_resume, self.have)

    def _on_disk(self, index: PieceIndex) -> bool:
        """Whether a piece is on disk and passes its check. Blocks."""
        piece_range = self.layout.piece_range(index)
        blocks = self.store.read_piece(piece_range)
        return blocks is not None and piece_range.validate_data(blocks)

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
