import socket


def free_port(kind: socket.SocketKind = socket.SOCK_STREAM) -> int:
    """A port on localhost that's free for TCP, or for UDP with ``SOCK_DGRAM``"""
    with socket.socket(type=kind) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]
