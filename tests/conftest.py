import pytest

from .fixtures import *


@pytest.fixture(scope="session")
def anyio_backend() -> str:
    """One event loop for the whole session, so the tracker and seeders can be shared by tests"""
    return "asyncio"
