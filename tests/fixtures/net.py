import math
import socket
from collections.abc import Callable, Iterator

import anyio
import pytest
from anyio.streams.buffered import BufferedByteStream
from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream
from anyio.streams.stapled import StapledObjectStream

__all__ = ["MakePipe", "make_pipe"]


def free_port(kind: socket.SocketKind = socket.SOCK_STREAM) -> int:
    """A port on localhost that's free for TCP, or for UDP with ``SOCK_DGRAM``"""
    with socket.socket(type=kind) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


type MakePipe = Callable[[], tuple[BufferedByteStream, BufferedByteStream]]


@pytest.fixture
def make_pipe() -> Iterator[MakePipe]:
    """
    Makes connections in memory, returning both ends: ours, and the remote's.
    They're closed after the test.
    """
    streams: list[MemoryObjectSendStream[bytes] | MemoryObjectReceiveStream[bytes]] = []

    def make() -> tuple[BufferedByteStream, BufferedByteStream]:
        a_send, b_receive = anyio.create_memory_object_stream[bytes](math.inf)
        b_send, a_receive = anyio.create_memory_object_stream[bytes](math.inf)
        streams.extend([a_send, a_receive, b_send, b_receive])
        return (
            BufferedByteStream(StapledObjectStream(a_send, a_receive)),
            BufferedByteStream(StapledObjectStream(b_send, b_receive)),
        )

    yield make
    for stream in streams:
        stream.close()
