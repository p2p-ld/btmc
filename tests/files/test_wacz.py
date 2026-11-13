import pytest

from btmc import TorrentWACZ

from ..fixtures import EXTRA_PAGES, PAGES

pytestmark = pytest.mark.libtorrent


async def test_wacz_pages(client, seeded):
    """We get the seed pages, and the extra pages after them if we ask, in batches"""
    wacz = TorrentWACZ(client, seeded, "crawl.wacz")
    assert [batch async for batch in wacz.pages()] == [PAGES]
    assert [batch async for batch in wacz.pages(extra=True)] == [PAGES, EXTRA_PAGES]
    # the header line takes up the first batch on its own, which isn't yielded empty
    assert [batch async for batch in wacz.pages(extra=True, batch_size=1)] == [
        [page] for page in PAGES + EXTRA_PAGES
    ]
