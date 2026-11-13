import anyio
import pytest
from anyio.abc import SocketAttribute

from btmc import Config
from btmc.exceptions import TrackerError
from btmc.protocol.messages import AnnounceRequest
from btmc.protocol.tracker import announce


async def test_unanswered_announce_times_out():
    """A tracker that doesn't answer fails the announce once the timeout is up, sent only once"""
    async with await anyio.create_udp_socket(local_host="127.0.0.1") as silent:
        url = f"udp://127.0.0.1:{silent.extra(SocketAttribute.local_port)}/announce"
        request = AnnounceRequest(info_hash=bytes(20), peer_id=bytes(20), port=1)
        with anyio.fail_after(1), pytest.raises(TrackerError):
            await announce(url, request, config=Config(announce_timeout=0.2))
        packets = []
        with anyio.move_on_after(0.2):
            async for packet, _ in silent:
                packets.append(packet)
    assert len(packets) == 1
