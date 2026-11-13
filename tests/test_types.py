import pytest

from btmc.types import parse_addr


@pytest.mark.parametrize(
    "value,expected",
    [
        ("127.0.0.1:6881", ("127.0.0.1", 6881)),
        ("[::1]:6881", ("::1", 6881)),
        (["example.com", "80"], ("example.com", 80)),
        (("10.0.0.1", 1), ("10.0.0.1", 1)),
    ],
)
def test_parse_addr(value, expected):
    assert parse_addr(value) == expected


@pytest.mark.parametrize("value", ["127.0.0.1", ":6881", "host:port"])
def test_parse_addr_invalid(value):
    with pytest.raises(ValueError):
        parse_addr(value)


@pytest.mark.parametrize("text", ["127.0.0.1:6881", "[::1]:6881", "example.com:80"])
def test_addr_str_parses_back(text):
    assert str(parse_addr(text)) == text
