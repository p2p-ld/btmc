from pathlib import Path

import pytest

from btmc import Config


def test_from_toml(tmp_path: Path):
    """Settings in the file replace defaults, and the rest are left as they are"""
    path = tmp_path / "custom.toml"
    path.write_text('max_peers = 10\nannounce_concurrency = 4\nlisten_host = "0.0.0.0"\n')
    assert Config.from_toml(path) == Config(
        max_peers=10, announce_concurrency=4, listen_host="0.0.0.0"
    )


def test_from_toml_in_cwd(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """By default it reads btmc.toml in the current directory, and is all defaults without one"""
    monkeypatch.chdir(tmp_path)
    assert Config.from_toml() == Config()
    (tmp_path / "btmc.toml").write_text("max_connections = 5\n")
    assert Config.from_toml() == Config(max_connections=5)


def test_from_toml_with_settings(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Settings passed in win over the file's, with or without a file"""
    monkeypatch.chdir(tmp_path)
    assert Config.from_toml(port=0) == Config(port=0)
    (tmp_path / "btmc.toml").write_text("max_connections = 5\nport = 7000\n")
    assert Config.from_toml(port=0) == Config(max_connections=5, port=0)
    with pytest.raises(TypeError):
        Config.from_toml(max_connection=5)


def test_paths_from_toml(tmp_path: Path):
    path = tmp_path / "btmc.toml"
    path.write_text('state_file = "state/btmc.json"\n')
    assert Config.from_toml(path).state_file == Path("state/btmc.json")


def test_from_toml_errors(tmp_path: Path):
    """Unknown settings are an error, and so is a file that was asked for but isn't there"""
    path = tmp_path / "btmc.toml"
    path.write_text("max_peers = 10\nmax_peer = 10\n")
    with pytest.raises(ValueError, match="max_peer$"):
        Config.from_toml(path)
    with pytest.raises(FileNotFoundError):
        Config.from_toml(tmp_path / "missing.toml")
