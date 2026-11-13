from pathlib import Path

from btmc.protocol.messages import Bitfield
from btmc.storage import PieceStore


def test_resume_file(tmp_path: Path):
    """There's no resume file until one is made, and a wrong-sized one doesn't count"""
    store = PieceStore(tmp_path, 10, "t")
    assert store.load_have() is None
    have = Bitfield(10)
    have.add(3)
    store.create_resume(have)
    store.save_have(9)
    have.add(9)
    assert store.load_have() == have
    store.resume_path.write_bytes(b"\xff")
    assert store.load_have() is None
