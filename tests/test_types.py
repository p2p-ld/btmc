import dataclasses
import pickle
from ipaddress import IPv4Address, IPv6Address

import pytest

from btmc.types import Addr, DomainAddr, parse_addr


@pytest.mark.parametrize(
    "value,expected",
    [
        ("127.0.0.1:6881", Addr(IPv4Address("127.0.0.1"), 6881)),
        ("[::1]:6881", Addr(IPv6Address("::1"), 6881)),
        ("127.1:6881", Addr(IPv4Address("127.0.0.1"), 6881)),
        ("example.com:80", DomainAddr("example.com", 80)),
        (["example.com", "80"], DomainAddr("example.com", 80)),
        (("10.0.0.1", 1), Addr.parse("10.0.0.1", 1)),
        (Addr.parse("10.0.0.1", 1), Addr.parse("10.0.0.1", 1)),
        (DomainAddr("example.com", 80), DomainAddr("example.com", 80)),
    ],
)
def test_parse_addr(value, expected):
    """IP addresses are Addrs, and host names are DomainAddrs"""
    assert parse_addr(value) == expected


@pytest.mark.parametrize("value", ["127.0.0.1", ":6881", "host:port", "a b:80"])
@pytest.mark.validation
def test_parse_addr_invalid(value):
    with pytest.raises(ValueError):
        parse_addr(value)


@pytest.mark.parametrize("text", ["127.0.0.1:6881", "[::1]:6881", "example.com:80"])
def test_addr_str_parses_back(text):
    assert str(parse_addr(text)) == text


@pytest.mark.parametrize(
    "host,expected",
    [
        ("10.0.0.1", "10.0.0.1"),
        ("0:0::1", "::1"),
        ("2001:DB8::1", "2001:db8::1"),
        # IPv4 mapped into IPv6
        ("::ffff:10.0.0.1", "10.0.0.1"),
        # older ways of writing IPv4, as the system reads them
        ("127.1", "127.0.0.1"),
        ("0x7f.0.0.1", "127.0.0.1"),
        ("2130706433", "127.0.0.1"),
        # a leading zero is octal
        ("010.0.0.1", "8.0.0.1"),
    ],
)
@pytest.mark.validation
def test_addr_is_written_one_way(host, expected):
    assert str(Addr.parse(host, 1).ip) == expected


@pytest.mark.parametrize(
    "host,port",
    [
        ("10.0.0.1", 0),
        ("10.0.0.1", -1),
        ("10.0.0.1", 65536),
        ("", 1),
        # inet_aton ignores everything after a space
        ("10.0.0.1 x", 1),
        ("example.com", 1),
        # a host name made of the letters of hex numbers
        ("dead.beef", 1),
    ],
)
@pytest.mark.validation
def test_addr_invalid(host, port):
    with pytest.raises(ValueError):
        Addr.parse(host, port)


@pytest.mark.validation
def test_addr_is_only_made_from_ips():
    """Text has to be parsed, so an address that wasn't checked can't get in"""
    with pytest.raises(TypeError):
        Addr("10.0.0.1", 1)  # type: ignore[arg-type]


@pytest.mark.validation
def test_addr_cant_be_made_invalid():
    addr = Addr.parse("10.0.0.1", 1)
    with pytest.raises(dataclasses.FrozenInstanceError):
        addr.port = 0  # type: ignore[misc]
    with pytest.raises(ValueError):
        dataclasses.replace(addr, port=0)
    mapped = dataclasses.replace(addr, ip=IPv6Address("::ffff:10.0.0.2"))
    assert mapped.ip == IPv4Address("10.0.0.2")


@pytest.mark.validation
def test_addr_isnt_a_tuple():
    """So an address that wasn't checked can't stand in for one"""
    assert Addr.parse("10.0.0.1", 1) != (IPv4Address("10.0.0.1"), 1)


@pytest.mark.parametrize("addr", [Addr.parse("10.0.0.1", 1), DomainAddr("example.com", 1)])
def test_addr_pickles(addr):
    assert pickle.loads(pickle.dumps(addr)) == addr


@pytest.mark.parametrize(
    "name,expected",
    [
        ("example.com", "example.com"),
        ("Example.COM", "example.com"),
        ("example.com.", "example.com"),
        ("localhost", "localhost"),
        ("dead.beef", "dead.beef"),
        # international names in their ASCII form
        ("trackér.example", "xn--trackr-fva.example"),
        ("xn--trackr-fva.example", "xn--trackr-fva.example"),
    ],
)
@pytest.mark.validation
def test_domain_addr_is_written_one_way(name, expected):
    assert DomainAddr(name, 1).name == expected


@pytest.mark.parametrize(
    "name,port",
    [
        ("example.com", 0),
        ("example.com", 65536),
        ("", 1),
        ("a b", 1),
        ("example.com/x", 1),
        ("ex_ample.com", 1),
        ("-example.com", 1),
        ("example-.com", 1),
        ("a..com", 1),
        ("a" * 64 + ".com", 1),
        ("a." * 127 + "com", 1),
        # IP addresses are Addrs
        ("10.0.0.1", 1),
        ("127.1", 1),
        ("::1", 1),
    ],
)
@pytest.mark.validation
def test_domain_addr_invalid(name, port):
    with pytest.raises(ValueError):
        DomainAddr(name, port)


async def test_domain_addr_resolves():
    addr = await DomainAddr("localhost", 6881).resolve()
    assert addr.ip.is_loopback
    assert addr.port == 6881
