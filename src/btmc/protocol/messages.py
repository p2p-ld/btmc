"""
The messages we send to and get from peers and trackers, with no I/O.

- Peer messages are :class:`.Message` s, which encode themselves and decode from their bytes.
  :meth:`.PeerMessage.decode` reads any of them.
- Tracker messages are :class:`.TrackerMessage` s. The same message can go over HTTP or UDP,
  so each has ``to_http``/``from_http`` and ``to_udp``/``from_udp`` for the ways it's sent.
  :func:`.decode_udp` and :func:`.decode_http` read whole UDP packets and HTTP responses,
  whatever message they turn out to be.

Decoding raises :class:`.PeerError` or :class:`.TrackerError` for bytes that aren't a message.
A tracker refusing a request is a :class:`.TrackerFailure` message, for the caller to deal with.
"""

from __future__ import annotations

import ipaddress
import struct
from abc import ABC, abstractmethod
from collections.abc import Iterator
from dataclasses import dataclass
from enum import IntEnum
from math import ceil
from typing import Any, ClassVar, Self
from urllib.parse import quote_from_bytes

import bencode_rs

from btmc.exceptions import PeerError, TrackerError
from btmc.types import Addr, BlockLength, BlockOffset, PieceIndex

PROTOCOL = b"BitTorrent protocol"
UDP_MAGIC = 0x41727101980
"""The connection id of a UDP tracker connect request (BEP 15)"""
MAX_MESSAGE = 1 << 21
"""Longest peer message we accept. Blocks are 16 KiB, but bitfields of big torrents are longer."""

type UDPRequest = Connect | AnnounceRequest
"""What we send to UDP trackers"""
type UDPResponse = Connected | AnnounceResponse | TrackerFailure
"""What UDP trackers send us"""
type HTTPResponse = AnnounceResponse | TrackerFailure
"""What HTTP trackers send us"""


class PeerMessageId(IntEnum):
    CHOKE = 0
    UNCHOKE = 1
    INTERESTED = 2
    NOT_INTERESTED = 3
    HAVE = 4
    BITFIELD = 5
    REQUEST = 6
    PIECE = 7
    CANCEL = 8


class TrackerAction(IntEnum):
    """What a UDP tracker packet is for (BEP 15)"""

    CONNECT = 0
    ANNOUNCE = 1
    ERROR = 3


class AnnounceEvent(IntEnum):
    """Announce events, numbered as in UDP announces"""

    NONE = 0
    COMPLETED = 1
    STARTED = 2
    STOPPED = 3


class Message(ABC):
    """Something we send to or get from a peer"""

    @abstractmethod
    def encode(self) -> bytes:
        """The bytes to send"""

    @classmethod
    @abstractmethod
    def decode(cls, data: bytes) -> Self:
        """
        A message from the bytes :meth:`.encode` makes.
        On the base class for a kind of message, reads any message of that kind.
        """


@dataclass(frozen=True)
class Handshake(Message):
    """The first thing each side sends on a peer connection, before any :class:`.PeerMessage`"""

    info_hash: bytes
    peer_id: bytes

    LENGTH: ClassVar[int] = 1 + len(PROTOCOL) + 8 + 20 + 20

    def encode(self) -> bytes:
        return bytes([len(PROTOCOL)]) + PROTOCOL + bytes(8) + self.info_hash + self.peer_id

    @classmethod
    def decode(cls, data: bytes) -> Self:
        if len(data) != cls.LENGTH or data[0] != len(PROTOCOL) or data[1:20] != PROTOCOL:
            raise PeerError("not a bittorrent handshake")
        return cls(info_hash=data[28:48], peer_id=data[48:68])


class PeerMessage(Message):
    """
    A message on a peer connection after the handshake: a 4-byte length,
    then a 1-byte id and a body. A keepalive is just a length of 0.

    Messages fill in their body with :meth:`._encode_body` and :meth:`._decode_body`.
    Messages without a body don't need to.
    """

    id: ClassVar[int | None]

    def encode(self) -> bytes:
        head = b"" if self.id is None else bytes([self.id])
        body = self._encode_body()
        return (len(head) + len(body)).to_bytes(4) + head + body

    @classmethod
    def decode(cls, data: bytes) -> Self:
        if len(data) < 4 or len(data) != 4 + int.from_bytes(data[:4]):
            raise PeerError("peer message's length doesn't match its length prefix")
        msg: PeerMessage
        if len(data) == 4:
            msg = KeepAlive()
        elif data[4] not in _PEER_MESSAGES:
            msg = Unknown(data[4], data[5:])
        else:
            kind = _PEER_MESSAGES[data[4]]
            try:
                msg = kind._decode_body(data[5:])
            except (struct.error, ValueError) as e:
                raise PeerError(f"malformed {kind.__name__} message") from e
        if not isinstance(msg, cls):
            raise PeerError(f"expected a {cls.__name__} message, got {type(msg).__name__}")
        return msg

    def _encode_body(self) -> bytes:
        return b""

    @classmethod
    def _decode_body(cls, body: bytes) -> PeerMessage:
        return cls()


@dataclass(frozen=True)
class KeepAlive(PeerMessage):
    id = None


@dataclass(frozen=True)
class Choke(PeerMessage):
    id = PeerMessageId.CHOKE


@dataclass(frozen=True)
class Unchoke(PeerMessage):
    id = PeerMessageId.UNCHOKE


@dataclass(frozen=True)
class Interested(PeerMessage):
    id = PeerMessageId.INTERESTED


@dataclass(frozen=True)
class NotInterested(PeerMessage):
    id = PeerMessageId.NOT_INTERESTED


@dataclass(frozen=True)
class Have(PeerMessage):
    index: PieceIndex

    id = PeerMessageId.HAVE

    def _encode_body(self) -> bytes:
        return struct.pack(">I", self.index)

    @classmethod
    def _decode_body(cls, body: bytes) -> Have:
        return cls(*struct.unpack(">I", body))


@dataclass(frozen=True)
class BitfieldMessage(PeerMessage):
    bits: Bitfield
    """
    When decoded, every bit of the message, since it doesn't say how many pieces there are.
    See :meth:`.Bitfield.for_pieces`
    """

    id = PeerMessageId.BITFIELD

    def _encode_body(self) -> bytes:
        return bytes(self.bits.bits)

    @classmethod
    def _decode_body(cls, body: bytes) -> BitfieldMessage:
        return cls(Bitfield(len(body) * 8, body))


@dataclass(frozen=True)
class Request(PeerMessage):
    index: PieceIndex
    begin: BlockOffset
    length: BlockLength

    id = PeerMessageId.REQUEST

    def _encode_body(self) -> bytes:
        return struct.pack(">III", self.index, self.begin, self.length)

    @classmethod
    def _decode_body(cls, body: bytes) -> Request:
        return cls(*struct.unpack(">III", body))


@dataclass(frozen=True)
class Cancel(PeerMessage):
    index: PieceIndex
    begin: BlockOffset
    length: BlockLength

    id = PeerMessageId.CANCEL

    def _encode_body(self) -> bytes:
        return struct.pack(">III", self.index, self.begin, self.length)

    @classmethod
    def _decode_body(cls, body: bytes) -> Cancel:
        return cls(*struct.unpack(">III", body))


@dataclass(frozen=True)
class Piece(PeerMessage):
    """A block of a piece"""

    index: PieceIndex
    begin: BlockOffset
    block: bytes

    id = PeerMessageId.PIECE

    def _encode_body(self) -> bytes:
        return struct.pack(">II", self.index, self.begin) + self.block

    @classmethod
    def _decode_body(cls, body: bytes) -> Piece:
        index, begin = struct.unpack(">II", body[:8])
        return cls(index, begin, body[8:])


@dataclass(frozen=True)
class Unknown(PeerMessage):
    """A message from an extension we don't speak"""

    message_id: int
    body: bytes

    id = None

    def encode(self) -> bytes:
        return (1 + len(self.body)).to_bytes(4) + bytes([self.message_id]) + self.body


_PEER_MESSAGES: dict[int, type[PeerMessage]] = {
    PeerMessageId.CHOKE: Choke,
    PeerMessageId.UNCHOKE: Unchoke,
    PeerMessageId.INTERESTED: Interested,
    PeerMessageId.NOT_INTERESTED: NotInterested,
    PeerMessageId.HAVE: Have,
    PeerMessageId.BITFIELD: BitfieldMessage,
    PeerMessageId.REQUEST: Request,
    PeerMessageId.PIECE: Piece,
    PeerMessageId.CANCEL: Cancel,
}


class Bitfield:
    """
    A set of piece indices, one bit per piece, most significant bit first,
    packed the way the bitfield message sends it.
    Spare bits after the last piece are ignored.
    """

    def __init__(self, n_pieces: int, data: bytes | None = None):
        self.n = n_pieces
        size = ceil(n_pieces / 8)
        if data is not None and len(data) != size:
            raise ValueError(
                f"bitfield for {n_pieces} pieces should be {size} bytes, got {len(data)}"
            )
        self.bits = bytearray(data) if data is not None else bytearray(size)

    def __eq__(self, other: object) -> bool:
        return isinstance(other, Bitfield) and (self.n, self.bits) == (other.n, other.bits)

    def __repr__(self) -> str:
        return f"Bitfield({self.n}, {bytes(self.bits)!r})"

    @classmethod
    def full(cls, n_pieces: int) -> Bitfield:
        """A bitfield with every piece, and the spare bits at the end left empty"""
        bits = cls(n_pieces, b"\xff" * ceil(n_pieces / 8))
        if n_pieces % 8:
            bits.bits[-1] = (0xFF << (8 - n_pieces % 8)) & 0xFF
        return bits

    def for_pieces(self, n_pieces: int) -> Bitfield:
        """
        A copy for a torrent with n pieces, raising ValueError if it's the wrong size for that.
        Bitfield messages are padded to whole bytes, so only the torrent knows how many pieces
        one is for.
        """
        return Bitfield(n_pieces, bytes(self.bits))

    def __contains__(self, i: PieceIndex) -> bool:
        return 0 <= i < self.n and bool(self.bits[i >> 3] & (0x80 >> (i & 7)))

    def add(self, i: PieceIndex) -> None:
        self.bits[i >> 3] |= 0x80 >> (i & 7)

    def discard(self, i: PieceIndex) -> None:
        self.bits[i >> 3] &= ~(0x80 >> (i & 7)) & 0xFF

    def intersects(self, other: Bitfield) -> bool:
        """Whether the two have any piece in common. They have to be for the same torrent."""
        if self.n != other.n:
            raise ValueError(f"bitfields for {self.n} and {other.n} pieces")
        # spare bits are never set in a bitfield we make, so they can't line up
        return bool(int.from_bytes(self.bits) & int.from_bytes(other.bits))

    def __iter__(self) -> Iterator[PieceIndex]:
        for byte_index, byte in enumerate(self.bits):
            if byte:
                base = byte_index << 3
                for bit in range(8):
                    if byte & (0x80 >> bit) and base + bit < self.n:
                        yield base + bit

    def count(self) -> int:
        # spare bits past the last piece don't count
        spare = self.bits[-1] & (0xFF >> (self.n % 8)) if self.n % 8 else 0
        return int.from_bytes(self.bits).bit_count() - spare.bit_count()


class TrackerMessage:
    """Something we send to or get from a tracker, over HTTP, UDP, or both"""

    action: ClassVar[TrackerAction]
    """What the message is for, in UDP packets"""


@dataclass(frozen=True)
class Connect(TrackerMessage):
    """Ask a UDP tracker for a connection id, which every other request needs"""

    action = TrackerAction.CONNECT

    def to_udp(self, transaction_id: int) -> bytes:
        # the header is the whole request, with the magic number where the connection id goes
        return _udp_header(UDP_MAGIC, self.action, transaction_id)


@dataclass(frozen=True)
class Connected(TrackerMessage):
    connection_id: int

    action = TrackerAction.CONNECT

    @classmethod
    def from_udp(cls, body: bytes, ipv6: bool) -> Self:
        (connection_id,) = struct.unpack(">Q", body[:8])
        return cls(connection_id)


@dataclass(frozen=True)
class AnnounceRequest(TrackerMessage):
    info_hash: bytes
    peer_id: bytes
    port: int
    downloaded: int = 0
    left: int = 0
    uploaded: int = 0
    event: AnnounceEvent = AnnounceEvent.NONE
    numwant: int = 50

    action = TrackerAction.ANNOUNCE

    def to_http(self) -> str:
        """The query string of the announce url"""
        params = [
            f"info_hash={quote_from_bytes(self.info_hash, safe='')}",
            f"peer_id={quote_from_bytes(self.peer_id, safe='')}",
            f"port={self.port}",
            f"uploaded={self.uploaded}",
            f"downloaded={self.downloaded}",
            f"left={self.left}",
            "compact=1",
            f"numwant={self.numwant}",
        ]
        if self.event:
            params.append(f"event={self.event.name.lower()}")
        return "&".join(params)

    def to_udp(self, connection_id: int, transaction_id: int) -> bytes:
        """
        Args:
            connection_id: from the tracker's :class:`.Connected`
            transaction_id: a random number, which the tracker's response will have
        """
        return _udp_header(connection_id, self.action, transaction_id) + struct.pack(
            ">20s20sQQQIIIiH",
            self.info_hash,
            self.peer_id,
            self.downloaded,
            self.left,
            self.uploaded,
            self.event,
            0,  # ip: the one the packet came from
            0,  # key
            self.numwant,
            self.port,
        )


@dataclass(frozen=True)
class AnnounceResponse(TrackerMessage):
    interval: int | None
    """Seconds to wait before announcing again, if the tracker said"""
    peers: list[Addr]

    action = TrackerAction.ANNOUNCE

    @classmethod
    def from_http(cls, value: dict[bytes, Any]) -> Self:
        interval = value.get(b"interval")
        return cls(None if interval is None else int(interval), _http_peers(value))

    @classmethod
    def from_udp(cls, body: bytes, ipv6: bool) -> Self:
        """UDP trackers send IPv6 peers when we announced over IPv6"""
        (interval,) = struct.unpack(">I", body[:4])
        # then leechers and seeders, which we don't use
        return cls(interval, _decode_peers(body[12:], 18 if ipv6 else 6))


@dataclass(frozen=True)
class TrackerFailure(TrackerMessage):
    """The tracker refused a request"""

    reason: str

    action = TrackerAction.ERROR

    @classmethod
    def from_http(cls, value: dict[bytes, Any]) -> Self:
        return cls(bytes(value[b"failure reason"]).decode(errors="replace"))

    @classmethod
    def from_udp(cls, body: bytes, ipv6: bool) -> Self:
        return cls(body.decode(errors="replace"))


def decode_udp(data: bytes, ipv6: bool) -> tuple[int, UDPResponse]:
    """
    A response from a UDP tracker, and its transaction id.

    Args:
        data: the packet
        ipv6: whether we sent the request over IPv6, which decides what kind of peers it has
    """
    if len(data) < 8:
        raise TrackerError("UDP tracker response is too short")
    action, transaction_id = struct.unpack(">II", data[:8])
    if action not in _UDP_RESPONSES:
        raise TrackerError(f"unknown UDP tracker response action {action}")
    kind = _UDP_RESPONSES[action]
    try:
        return transaction_id, kind.from_udp(data[8:], ipv6)
    except struct.error as e:
        raise TrackerError(f"malformed {kind.__name__} from UDP tracker") from e


_UDP_RESPONSES: dict[int, type[UDPResponse]] = {
    TrackerAction.CONNECT: Connected,
    TrackerAction.ANNOUNCE: AnnounceResponse,
    TrackerAction.ERROR: TrackerFailure,
}


def decode_http(data: bytes) -> HTTPResponse:
    """A response from an HTTP tracker"""
    try:
        value = bencode_rs.bdecode(data)
    except bencode_rs.BencodeDecodeError as e:
        raise TrackerError(f"can't decode tracker response: {e}") from e
    if not isinstance(value, dict):
        raise TrackerError("tracker response isn't a dict")
    try:
        if b"failure reason" in value:
            return TrackerFailure.from_http(value)
        return AnnounceResponse.from_http(value)
    except (TypeError, ValueError, AttributeError) as e:
        raise TrackerError(f"malformed tracker response: {e!r}") from e


def _decode_peers(data: bytes, size: int) -> list[Addr]:
    """Compact peers: 4 or 16 bytes of IP and 2 of port per peer"""
    peers = []
    for i in range(0, len(data) - len(data) % size, size):
        ip = ipaddress.ip_address(data[i : i + size - 2])
        port = int.from_bytes(data[i + size - 2 : i + size])
        if port:
            peers.append(Addr(str(ip), port))
    return peers


def _http_peers(value: dict[bytes, Any]) -> list[Addr]:
    """
    The peers in an HTTP tracker's response: IPv4 in ``peers``, compact or as the list of dicts
    sent by trackers that ignore ``compact=1``, and IPv6 in ``peers6``, always compact
    """
    peers = value.get(b"peers", b"")
    if isinstance(peers, list):
        ipv4 = [
            Addr(p[b"ip"].decode(errors="replace"), int(p[b"port"]))
            for p in peers
            if b"ip" in p and b"port" in p
        ]
    else:
        ipv4 = _decode_peers(peers, 6)
    return ipv4 + _decode_peers(value.get(b"peers6", b""), 18)


def _udp_header(connection_id: int, action: TrackerAction, transaction_id: int) -> bytes:
    return struct.pack(">QII", connection_id, action, transaction_id)
