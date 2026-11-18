import pytest

from .fixtures import *


# before pytest deselects by mark, so -m safety sees the marks added here
@pytest.hookimpl(tryfirst=True)
def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    """Tests with a kind of safety mark, described in pyproject.toml as "safety: ...", are safety"""
    kinds = []
    for line in config.getini("markers"):
        name, _, description = line.partition(":")
        if description.strip().startswith("safety:"):
            kinds.append(name)
    for item in items:
        if any(item.get_closest_marker(kind) for kind in kinds):
            item.add_marker(pytest.mark.safety)


@pytest.fixture(scope="session")
def anyio_backend() -> str:
    """One event loop for the whole session, so the tracker and seeders can be shared by tests"""
    return "asyncio"
