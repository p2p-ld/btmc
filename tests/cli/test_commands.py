"""The commands, run the way a shell would run them"""

import logging

import pytest
from click.testing import CliRunner

from btmc.cli.main import main
from btmc.utils import torrent_name

from ..fixtures import ZIP_MEMBERS

pytestmark = pytest.mark.libtorrent


def test_fetch_everything(torrent_file, torrent, seeders, tmp_path):
    """Without PIECES, fetch gets the whole torrent"""
    dest = tmp_path / "dest"
    result = CliRunner().invoke(
        main,
        [
            "fetch",
            str(torrent_file),
            str(dest),
            "--no-trackers",
            "--peer",
            str(seeders[0].addr),
            "--peer",
            str(seeders[1].addr),
        ],
    )
    assert result.exit_code == 0, result.output
    assert torrent.check(dest)


@pytest.mark.parametrize(
    "flags,level",
    [
        ([], logging.WARNING),
        (["-v"], logging.INFO),
        (["-vv"], logging.DEBUG),
        (["-v", "-v"], logging.DEBUG),
    ],
)
def test_verbosity(torrent_file, tmp_path, flags, level, monkeypatch):
    """Each -v logs more"""
    levels = []
    monkeypatch.setattr(logging, "basicConfig", lambda **kwargs: levels.append(kwargs["level"]))
    CliRunner().invoke(
        main,
        [
            *flags,
            "fetch",
            str(torrent_file),
            str(tmp_path / "dest"),
            "--no-trackers",
            "100000",
        ],
    )
    assert levels == [level]


def test_fetch_piece_out_of_range(torrent_file, tmp_path):
    result = CliRunner().invoke(
        main, ["fetch", str(torrent_file), str(tmp_path / "dest"), "--no-trackers", "100000"]
    )
    assert result.exit_code == 2
    assert "there's no piece 100000" in result.output


def test_torrent_past_the_limits(torrent_file, tmp_path, monkeypatch):
    """The torrent is read with the limits in btmc.toml"""
    monkeypatch.chdir(tmp_path)
    (tmp_path / "btmc.toml").write_text("[torrent_limits]\nmax_pieces = 1\n")
    result = CliRunner().invoke(
        main, ["fetch", str(torrent_file), str(tmp_path / "dest"), "--no-trackers"]
    )
    assert result.exit_code == 2
    assert "max_pieces" in result.output


def test_zip_needs_a_file(torrent_file, tmp_path):
    """A torrent with more than one file has to say which is the zip"""
    result = CliRunner().invoke(
        main, ["zip", str(torrent_file), str(tmp_path / "dest"), "--no-trackers"]
    )
    assert result.exit_code == 2
    assert "pick one with --file" in result.output


def test_zip_lists_members(torrent_file, seeders, tmp_path):
    result = CliRunner().invoke(
        main,
        [
            "zip",
            str(torrent_file),
            str(tmp_path / "dest"),
            "--no-trackers",
            "--file",
            "archive.zip",
            "--peer",
            str(seeders[0].addr),
        ],
    )
    assert result.exit_code == 0, result.output
    # only the listing goes to stdout, so it can be piped
    assert sorted(line.split()[-1] for line in result.stdout.splitlines()) == sorted(ZIP_MEMBERS)
    assert "fetched" in result.stderr


@pytest.mark.parametrize("clear", [True, False])
def test_mount_clear_cache(torrent_file, torrent, tmp_path, state_dir, monkeypatch, clear):
    """
    --clear-cache deletes what's been fetched for this torrent before mounting,
    and its resume files, and no more
    """
    try:
        import btmc.fsspec.fuse as fuse
    except OSError:
        pytest.skip("no FUSE library")
    mounted = []
    monkeypatch.setattr(fuse, "mount", lambda *args: mounted.append(args))
    cache = tmp_path / "cache"
    fetched = cache / torrent_name(torrent) / "fetched.bin"
    other = cache / "another-torrent" / "fetched.bin"
    resume = state_dir / "resume" / torrent_name(torrent) / "elsewhere.have"
    other_resume = state_dir / "resume" / "another-torrent" / "elsewhere.have"
    for path in (fetched, other, resume, other_resume):
        path.parent.mkdir(parents=True)
        path.write_bytes(b"x")
    mountpoint = tmp_path / "mnt"
    mountpoint.mkdir()

    flags = ["--clear-cache"] if clear else []
    result = CliRunner().invoke(
        main,
        [
            "mount",
            str(torrent_file),
            str(mountpoint),
            "--download-dir",
            str(cache),
            "--no-trackers",
            *flags,
        ],
    )
    assert result.exit_code == 0, result.output
    assert len(mounted) == 1
    assert fetched.exists() is not clear
    assert resume.exists() is not clear
    assert other.exists()
    assert other_resume.exists()
