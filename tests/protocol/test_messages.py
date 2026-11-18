import pytest

from btmc.exceptions import MalformedMessage, PeerError, TrackerError
from btmc.protocol.messages import (
    AnnounceEvent,
    AnnounceRequest,
    AnnounceResponse,
    Bitfield,
    BitfieldMessage,
    Cancel,
    Choke,
    Connect,
    Connected,
    Handshake,
    Have,
    Interested,
    KeepAlive,
    NotInterested,
    PeerMessage,
    Piece,
    Request,
    TrackerFailure,
    Unchoke,
    Unknown,
    _decode_peers,
    decode_http,
    decode_udp,
)
from btmc.types import Addr, DomainAddr

INFO_HASH = bytes(range(20))
PEER_ID = bytes(range(20, 40))


# ---- peers ----


@pytest.mark.parametrize(
    "msg,data",
    [
        (KeepAlive(), b"\x00\x00\x00\x00"),
        (Choke(), b"\x00\x00\x00\x01\x00"),
        (Unchoke(), b"\x00\x00\x00\x01\x01"),
        (Interested(), b"\x00\x00\x00\x01\x02"),
        (NotInterested(), b"\x00\x00\x00\x01\x03"),
        (Have(7), b"\x00\x00\x00\x05\x04\x00\x00\x00\x07"),
        (BitfieldMessage(Bitfield(16, b"\xa0\x40")), b"\x00\x00\x00\x03\x05\xa0\x40"),
        (
            Request(1, 16384, 16384),
            b"\x00\x00\x00\x0d\x06\x00\x00\x00\x01\x00\x00\x40\x00\x00\x00\x40\x00",
        ),
        (Piece(2, 0, b"abc"), b"\x00\x00\x00\x0c\x07\x00\x00\x00\x02\x00\x00\x00\x00abc"),
        (
            Cancel(1, 16384, 16384),
            b"\x00\x00\x00\x0d\x08\x00\x00\x00\x01\x00\x00\x40\x00\x00\x00\x40\x00",
        ),
        # an extension message (BEP 10) we don't speak
        (Unknown(20, b"\x00d1:md2:ut_pexi1eee"), b"\x00\x00\x00\x14\x14\x00d1:md2:ut_pexi1eee"),
    ],
)
def test_peer_messages(msg, data):
    """
    Messages are a 4-byte length, counting the id, then the id, then the body.
    PeerMessage reads any of them, and each message's own class reads itself.
    """
    assert msg.encode() == data
    assert PeerMessage.decode(data) == msg
    assert type(msg).decode(data) == msg


def test_handshake():
    """The handshake comes first, with no length prefix, so only Handshake reads it"""
    msg = Handshake(INFO_HASH, PEER_ID)
    data = b"\x13BitTorrent protocol" + bytes(8) + INFO_HASH + PEER_ID
    assert msg.encode() == data
    assert Handshake.decode(data) == msg


@pytest.mark.parametrize(
    "data",
    [
        # a have too short for its index
        b"\x00\x00\x00\x03\x04\x00\x00",
        # a request too short for its fields
        b"\x00\x00\x00\x05\x06\x00\x00\x00\x01",
    ],
)
def test_malformed_peer_message(data):
    with pytest.raises(MalformedMessage):
        PeerMessage.decode(data)


def test_length_prefix_has_to_match():
    with pytest.raises(PeerError):
        PeerMessage.decode(b"\x00\x00\x00\x02\x00")


def test_peer_message_of_the_wrong_type():
    with pytest.raises(PeerError):
        Have.decode(Choke().encode())


def test_bitfield():
    """Bits are read most significant first, and spare bits at the end are ignored"""
    bits = Bitfield(10, bytes([0b10100000, 0b01000000]))
    assert list(bits) == [0, 2, 9]
    assert 10 not in bits
    bits.add(1)
    assert bits.bits == bytearray([0b11100000, 0b01000000])
    assert bits.count() == 4


def test_bitfield_spare_bits():
    """Bits past the last piece aren't pieces, even if a peer sets them"""
    bits = Bitfield(10, bytes([0, 0b00111111]))
    assert list(bits) == []
    assert bits.count() == 0


def test_bitfield_full():
    assert Bitfield.full(10).bits == bytearray([0xFF, 0b11000000])
    assert Bitfield.full(16).bits == bytearray([0xFF, 0xFF])
    assert Bitfield.full(10).count() == 10


def test_bitfield_wrong_size():
    with pytest.raises(ValueError):
        Bitfield(10, bytes(1))


def test_bitfield_for_pieces():
    """A bitfield message covers whole bytes, until we know how many pieces it's for"""
    bits = Bitfield(16, bytes([0b10100000, 0b01000000]))
    assert bits.for_pieces(10) == Bitfield(10, bytes([0b10100000, 0b01000000]))
    with pytest.raises(ValueError):
        bits.for_pieces(20)


@pytest.mark.parametrize(
    "data,size,expected",
    [
        (bytes([127, 0, 0, 1, 0x1A, 0xE1]), 6, [Addr.parse("127.0.0.1", 6881)]),
        # a trailing partial peer is dropped
        (bytes([10, 0, 0, 1, 0, 80, 10, 0]), 6, [Addr.parse("10.0.0.1", 80)]),
        # port 0 is dropped
        (bytes([10, 0, 0, 1, 0, 0]), 6, []),
        (bytes(15) + bytes([1, 0x1A, 0xE1]), 18, [Addr.parse("::1", 6881)]),
    ],
)
def test_decode_peers(data, size, expected):
    assert _decode_peers(data, size) == expected


REQUEST = AnnounceRequest(
    info_hash=INFO_HASH,
    peer_id=PEER_ID,
    port=6881,
    downloaded=1,
    left=2,
    event=AnnounceEvent.STARTED,
)


def test_announce_http():
    """frozen value tests....."""
    assert REQUEST.to_http() == (
        "info_hash=%00%01%02%03%04%05%06%07%08%09%0A%0B%0C%0D%0E%0F%10%11%12%13"
        "&peer_id=%14%15%16%17%18%19%1A%1B%1C%1D%1E%1F%20%21%22%23%24%25%26%27"
        "&port=6881&uploaded=0&downloaded=1&left=2&compact=1&numwant=50&event=started"
    )


def test_udp_requests():
    """frozen value tests....."""
    assert Connect().to_udp(9) == bytes.fromhex("00000417271019800000000000000009")
    assert REQUEST.to_udp(connection_id=0x0102030405060708, transaction_id=9) == (
        bytes.fromhex("0102030405060708")  # connection id
        + bytes.fromhex("00000001")  # action: announce
        + bytes.fromhex("00000009")  # transaction id
        + INFO_HASH
        + PEER_ID
        + bytes.fromhex("0000000000000001")  # downloaded
        + bytes.fromhex("0000000000000002")  # left
        + bytes.fromhex("0000000000000000")  # uploaded
        + bytes.fromhex("00000002")  # event: started
        + bytes.fromhex("00000000")  # ip
        + bytes.fromhex("00000000")  # key
        + bytes.fromhex("00000032")  # numwant: 50
        + bytes.fromhex("1ae1")  # port: 6881
    )


@pytest.mark.parametrize(
    "data,ipv6,expected",
    [
        (
            bytes.fromhex("00000000000000090102030405060708"),
            False,
            (9, Connected(0x0102030405060708)),
        ),
        (
            bytes.fromhex("0000000100000009000007080000000300000004")
            + bytes([127, 0, 0, 1, 0x1A, 0xE1]),
            False,
            (9, AnnounceResponse(interval=1800, peers=[Addr.parse("127.0.0.1", 6881)])),
        ),
        # UDP trackers send IPv6 peers when we announced over IPv6
        (
            bytes.fromhex("0000000100000009000007080000000000000001")
            + bytes(15)
            + bytes([1, 0x1A, 0xE1]),
            True,
            (9, AnnounceResponse(interval=1800, peers=[Addr.parse("::1", 6881)])),
        ),
        # the tracker refusing is a message too, for the caller to deal with
        (
            bytes.fromhex("0000000300000009") + b"unregistered torrent",
            False,
            (9, TrackerFailure("unregistered torrent")),
        ),
    ],
)
def test_udp_responses(data, ipv6, expected):
    assert decode_udp(data, ipv6) == expected


@pytest.mark.parametrize(
    "data",
    [
        bytes.fromhex("0000"),
        # scrape, which we don't know yet
        # (but want to raise to tell us to update tests when we do)
        bytes.fromhex("0000000200000009"),
        # a connect response without its connection id
        bytes.fromhex("0000000000000009"),
        # an announce response without its leechers and seeders
        bytes.fromhex("000000010000000900000708"),
    ],
)
def test_udp_responses_bad(data):
    with pytest.raises(TrackerError):
        decode_udp(data, ipv6=False)


@pytest.mark.parametrize(
    "body,expected",
    [
        (
            b"d8:intervali1800e5:peers6:\x7f\x00\x00\x01\x1a\xe1e",
            AnnounceResponse(interval=1800, peers=[Addr.parse("127.0.0.1", 6881)]),
        ),
        (
            b"d8:intervali60e5:peers0:6:peers618:" + bytes(15) + b"\x01\x1a\xe1e",
            AnnounceResponse(interval=60, peers=[Addr.parse("::1", 6881)]),
        ),
        # trackers that ignore compact=1 send a list of dicts, with peer ids,
        # and whose ip can be a host name
        (
            b"d5:peersld2:ip8:10.0.0.14:porti80eed2:ip11:example.com7:peer id1:x4:porti81eeee",
            AnnounceResponse(
                interval=None,
                peers=[Addr.parse("10.0.0.1", 80), DomainAddr("example.com", 81)],
            ),
        ),
        # entries that aren't addresses are left out, keeping the rest
        (
            b"d5:peersl"
            + b"d2:ip8:10.0.0.1e"  # no port
            + b"d2:ip8:10.0.0.14:port2:80e"  # port isn't a number
            + b"d2:ipi1e4:porti80ee"  # ip isn't a string
            + b"d2:ip2:\xff\xfe4:porti80ee"  # ip isn't text
            + b"i1e"  # not a dict
            + b"d2:ip8:10.0.0.14:porti70000ee"  # port out of range
            + b"d2:ip8:10.0.0.14:porti0ee"
            + b"d2:ip7:a b.com4:porti80ee"  # not a host name
            + b"d2:ip8:10.0.0.34:porti80ee"
            + b"ee",
            AnnounceResponse(
                interval=None,
                peers=[Addr.parse("10.0.0.3", 80)],
            ),
        ),
        # the tracker refusing is a message too, for the caller to deal with
        (
            b"d14:failure reason20:unregistered torrente",
            TrackerFailure("unregistered torrent"),
        ),
        # some trackers send unsorted or duplicated keys; the last duplicate wins
        (
            b"d5:peers0:8:intervali60e8:intervali90ee",
            AnnounceResponse(interval=90, peers=[]),
        ),
        # like libtorrent, peers of the wrong type are no peers, keeping the other key's
        (
            b"d8:intervali60e5:peersi1e6:peers618:" + bytes(15) + b"\x01\x1a\xe1e",
            AnnounceResponse(interval=60, peers=[Addr.parse("::1", 6881)]),
        ),
        (
            b"d8:intervali60e5:peers6:\x7f\x00\x00\x01\x1a\xe16:peers6lee",
            AnnounceResponse(interval=60, peers=[Addr.parse("127.0.0.1", 6881)]),
        ),
    ],
)
def test_http_responses(body, expected):
    assert decode_http(body) == expected


@pytest.mark.parametrize(
    "body",
    [
        b"not bencode",
        b"li1ee",
        # a failure reason that isn't a string. bytes(4000000000) would be 4 GB of zeros
        b"d14:failure reasoni4000000000ee",
        b"d14:failure reasoni99999999999999999999ee",
        b"d14:failure reasonle",
        b"d8:interval2:60e5:peers0:e",
        # nesting this deep crashed the process before it was limited
        b"l" * 60000 + b"e" * 60000,
        b"d5:peers" + b"l" * 101 + b"e" * 101 + b"e",
    ],
)
def test_malformed_http_response(body):
    with pytest.raises(TrackerError):
        decode_http(body)


@pytest.mark.parametrize(
    "kind,valid,invalid",
    [
        (Choke, [0], [1]),
        (Have, [4], [3, 5]),
        # one bit per piece, rounded up to whole bytes
        (BitfieldMessage, [2], [1, 3]),
        (Request, [12], [11, 13]),
        (Cancel, [12], [11, 13]),
        # at most a block, after the index and offset
        (Piece, [8, 8 + 16384], [7, 8 + 16384 + 1]),
    ],
)
def test_valid_length(kind, valid, invalid):
    assert all(PeerMessage.valid_length(kind.id, length, n_pieces=9) for length in valid)
    assert not any(PeerMessage.valid_length(kind.id, length, n_pieces=9) for length in invalid)


def test_extensions_we_dont_speak_can_be_any_length():
    assert PeerMessage.valid_length(20, 2**31, n_pieces=9)
