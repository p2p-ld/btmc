import json
from pathlib import Path

import pytest

from btmc import Config
from btmc.protocol.shared import BanReason, Shared
from btmc.types import Addr

PEER_ID = b"A" * 20


@pytest.mark.bans
def test_save_and_load(tmp_path: Path):
    """Banned peers outlast the client"""
    path = tmp_path / "state.json"
    shared = Shared(Config(), PEER_ID)
    shared.ban(Addr.parse("10.0.0.2", 2), BanReason.BAD_DATA)
    shared.ban(Addr.parse("10.0.0.1", 1), BanReason.MALFORMED_MESSAGE)
    shared.save(path)
    assert json.loads(path.read_text()) == {"banned": [["10.0.0.1", 1], ["10.0.0.2", 2]]}

    loaded = Shared(Config(), PEER_ID)
    loaded.load(path)
    assert loaded.is_banned(Addr.parse("10.0.0.1", 1))
    assert loaded.is_banned(Addr.parse("10.0.0.2", 2))
    assert not loaded.is_banned(Addr.parse("10.0.0.3", 3))


def test_load_nothing(tmp_path: Path):
    """There's nothing to load the first time"""
    shared = Shared(Config(), PEER_ID)
    shared.load(tmp_path / "state.json")
    assert not shared.is_banned(Addr.parse("10.0.0.1", 1))


def test_tracker_backoff():
    """
    A tracker that doesn't answer is backed off, twice as long for each failure in a row,
    up to a limit, and is ready again once it answers
    """
    url = "http://tracker.example/announce"
    shared = Shared(Config(tracker_backoff=10, max_tracker_backoff=25), PEER_ID)
    assert shared.tracker_ready_at(url) == 0
    shared.announced(url, 100, answered=False)
    assert shared.tracker_ready_at(url) == 110
    # other torrents' announces failing at about the same time count once
    shared.announced(url, 101, answered=False)
    assert shared.tracker_ready_at(url) == 110
    shared.announced(url, 110, answered=False)
    assert shared.tracker_ready_at(url) == 130
    shared.announced(url, 130, answered=False)
    assert shared.tracker_ready_at(url) == 155
    shared.announced(url, 155)
    assert shared.tracker_ready_at(url) == 0
