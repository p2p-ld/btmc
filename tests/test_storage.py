import shutil
from pathlib import Path

import pytest
from torrent_models import Torrent

from btmc.protocol.messages import Bitfield
from btmc.storage import PieceStore


@pytest.mark.validation
def test_resume_file(tmp_path: Path, torrent: Torrent, test_data: Path):
    """There's no resume file until one is made, and a wrong-sized one doesn't count"""
    n_pieces = torrent.layout.n_pieces
    store = PieceStore(tmp_path / "data", n_pieces, tmp_path / "state" / "t.have")
    shutil.copytree(test_data, store.root, dirs_exist_ok=True)
    assert store.load_have() is None
    have = Bitfield(n_pieces)
    have.add(3)
    store.create_resume(have)
    store.save_have(9)
    have.add(9)
    assert store.load_have() == have
    store.resume_path.write_bytes(b"\xff")
    assert store.load_have() is None
