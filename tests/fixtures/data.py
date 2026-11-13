import json
import random
import zipfile
from pathlib import Path

import pytest

__all__ = [
    "EXTRA_PAGES",
    "FILES",
    "PAGES",
    "PIECE_LENGTH",
    "ZIP_MEMBERS",
    "test_data",
]

PIECE_LENGTH = 32 * 1024
FILES = {
    # big enough to spread over several peers' request pipelines
    "big.bin": 200 * PIECE_LENGTH + 1234,
    "sub/mid.bin": 40000,
    "small.txt": 3000,
    "exactly_one_piece.bin": PIECE_LENGTH,
    "empty.dat": 0,
}
ZIP_MEMBERS = {
    "stored.bin": 40 * PIECE_LENGTH + 5,
    "deflated.txt": 20 * PIECE_LENGTH,
    "dir/small.txt": 100,
}
"""name: size of the members of archive.zip"""

PAGES = [
    {
        "id": "1",
        "url": "https://example.com/",
        "ts": "2026-11-04T00:00:00Z",
        "loadState": 4,
        "status": 200,
        "seed": True,
        "depth": 0,
    },
    {
        "id": "2",
        "url": "https://example.com/about",
        "ts": "2026-11-04T00:00:01Z",
        "loadState": 4,
        "status": 200,
        "seed": True,
        "depth": 0,
    },
]
EXTRA_PAGES = [
    {
        "id": "3",
        "url": "https://example.com/contact",
        "ts": "2026-11-04T00:00:02Z",
        "loadState": 2,
        "status": 404,
        "depth": 1,
    },
    {
        "id": "4",
        "url": "https://example.com/blog",
        "ts": "2026-11-04T00:00:03Z",
        "loadState": 0,
        "depth": 1,
    },
]


def _jsonl(header_id: str, records: list[dict]) -> str:
    header = {"format": "json-pages-1.0", "id": header_id, "title": header_id}
    return "\n".join(json.dumps(r) for r in [header, *records]) + "\n"


@pytest.fixture(scope="session")
def test_data(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """
    A directory with a ``testdata`` folder of files to make torrents from,
    including a zip, ``archive.zip``, and a WACZ, ``crawl.wacz``.
    """
    root = tmp_path_factory.mktemp("data")
    src = root / "testdata"
    rng = random.Random(1)
    for name, size in FILES.items():
        path = src / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(rng.randbytes(size))

    with zipfile.ZipFile(src / "archive.zip", "w") as zf:
        for name, size in ZIP_MEMBERS.items():
            if name.endswith(".txt"):
                text = "".join(rng.choice("abcdef \n") for _ in range(size))
                zf.writestr(name, text, compress_type=zipfile.ZIP_DEFLATED)
            else:
                zf.writestr(name, rng.randbytes(size), compress_type=zipfile.ZIP_STORED)

    with zipfile.ZipFile(src / "crawl.wacz", "w") as zf:
        zf.writestr("archive/data.warc.gz", rng.randbytes(30 * PIECE_LENGTH))
        zf.writestr("pages/pages.jsonl", _jsonl("pages", PAGES))
        zf.writestr("pages/extraPages.jsonl", _jsonl("extra-pages", EXTRA_PAGES))
        zf.writestr("datapackage.json", json.dumps({"profile": "data-package"}))
    return root
