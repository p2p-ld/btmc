from btmc.utils import wire_hash


def test_wire_hash():
    v1 = "aa" * 20
    v2 = "bb" * 32
    assert wire_hash(v1) == bytes.fromhex(v1)
    assert wire_hash(v2) == bytes.fromhex(v2)[:20]
    assert wire_hash(v2[:40]) == bytes.fromhex(v2)[:20]
    assert wire_hash(bytes.fromhex(v2)) == bytes.fromhex(v2)[:20]
