import struct

import anyio
import httpx
import pytest
from anyio.abc import SocketAttribute

from btmc import Config
from btmc.exceptions import TrackerError
from btmc.protocol.messages import AnnounceRequest, AnnounceResponse
from btmc.protocol.tracker import announce, check_url
from btmc.types import Addr

REQUEST = AnnounceRequest(info_hash=bytes(20), peer_id=bytes(20), port=1)


@pytest.mark.limits
async def test_unanswered_announce_times_out():
    """A tracker that doesn't answer fails the announce once the timeout is up, sent only once"""
    async with await anyio.create_udp_socket(local_host="127.0.0.1") as silent:
        url = f"udp://127.0.0.1:{silent.extra(SocketAttribute.local_port)}/announce"
        with anyio.fail_after(1), pytest.raises(TrackerError):
            await announce(url, REQUEST, config=Config(announce_timeout=0.2))
        packets = []
        with anyio.move_on_after(0.2):
            async for packet, _ in silent:
                packets.append(packet)
    assert len(packets) == 1


@pytest.mark.validation
async def test_udp_junk_is_skipped():
    """
    Packets that are malformed, for another transaction, or of the wrong kind
    don't end the announce, since anyone can send them
    """
    async with await anyio.create_udp_socket(local_host="127.0.0.1") as fake:
        url = f"udp://127.0.0.1:{fake.extra(SocketAttribute.local_port)}/announce"

        async def answer(action: int, body: bytes) -> None:
            """Answer the next request with junk, then with this"""
            packet, addr = await fake.receive()
            (txid,) = struct.unpack(">I", packet[12:16])
            junk = [
                b"\x00\x00\x00\x01",
                struct.pack(">II", 9, txid),
                # a connect and an announce response, too short
                struct.pack(">IIH", 0, txid, 1),
                struct.pack(">III", 1, txid, 60),
                # for another transaction
                struct.pack(">IIQ", 0, txid ^ 1, 5),
                struct.pack(">IIIII", 1, txid ^ 1, 60, 0, 0),
                # the other kind of response
                (
                    struct.pack(">IIIII", 1, txid, 60, 0, 0)
                    if action == 0
                    else struct.pack(">IIQ", 0, txid, 5)
                ),
            ]
            for reply in [*junk, struct.pack(">II", action, txid) + body]:
                await fake.sendto(reply, *addr)

        async def tracker() -> None:
            await answer(0, struct.pack(">Q", 7))
            await answer(1, struct.pack(">III", 60, 0, 0) + bytes([10, 0, 0, 1, 0, 80]))

        with anyio.fail_after(1):
            async with anyio.create_task_group() as tg:
                tg.start_soon(tracker)
                response = await announce(url, REQUEST, config=Config())
    assert response == AnnounceResponse(60, [Addr.parse("10.0.0.1", 80)])


@pytest.mark.parametrize(
    "url",
    [
        "http://tracker.example/announce",
        "https://tracker.example:8443/announce?passkey=abc",
        "udp://tracker.example:6969/announce",
        "http://127.0.0.1/announce",
        "http://[::1]:8000/announce",
        "http://trackér.example/announce",
    ],
)
@pytest.mark.validation
def test_url_ok(url):
    check_url(url)


@pytest.mark.parametrize(
    "url",
    [
        "",
        "ftp://tracker.example/announce",
        "wss://tracker.example/announce",
        # UDP trackers have no default port
        "udp://tracker.example/announce",
        "http://tracker.example:0/announce",
        "http://tracker.example:65536/announce",
        "http:///announce",
        "http://tr_cker example/announce",
        "http://tracker.example/an nounce",
        "http://tracker.example/announce\r\nX-Injected: 1",
        "http://[::1/announce",
        "http://tr_cker.example/announce",
    ],
)
@pytest.mark.validation
def test_url_bad(url):
    with pytest.raises(TrackerError):
        check_url(url)


@pytest.mark.parametrize("status", [200, 204, 301, 404, 500])
@pytest.mark.validation
async def test_only_http_200_is_an_answer(status):
    body = b"d8:intervali60e5:peers6:\x7f\x00\x00\x01\x1a\xe1e"
    transport = httpx.MockTransport(lambda _: httpx.Response(status, content=body))
    async with httpx.AsyncClient(transport=transport) as http:
        url = "http://tracker.example/announce"
        if status == 200:
            response = await announce(url, REQUEST, config=Config(), http=http)
            assert response == AnnounceResponse(60, [Addr.parse("127.0.0.1", 6881)])
        else:
            with pytest.raises(TrackerError):
                await announce(url, REQUEST, config=Config(), http=http)
