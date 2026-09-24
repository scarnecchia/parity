from datetime import date, datetime
from decimal import Decimal
from fractions import Fraction

import pytest
from hypothesis import given
from hypothesis import strategies as st

from sentinel_parity.core.discovery import FileEntry, pair_files
from sentinel_parity.core.names import normalize_columns, normalize_name
from sentinel_parity.core.value_encoding import canonical_key, numeric_key, typed_value


def test_schema_name_alignment() -> None:
    result = normalize_columns(["B", "a"])
    assert [(x.original, x.normalized) for x in result] == [("a", "a"), ("B", "b")]
    with pytest.raises(ValueError):
        normalize_columns(["Straße", "STRASSE"])


def test_numeric_exactness_boundaries() -> None:
    assert numeric_key(1) == numeric_key(1.0) == numeric_key(Decimal("1.00"))
    assert numeric_key(2**53 + 1) != numeric_key(float(2**53 + 1))
    assert numeric_key(-0.0) == "n:0/1"
    assert numeric_key(float("nan")) == "f:nan"
    assert numeric_key(float("inf")) == "f:+inf"
    assert numeric_key(float("-inf")) == "f:-inf"
    assert Fraction(1, 10) != Fraction(*(0.1).as_integer_ratio())


def test_null_nan_text_policy() -> None:
    assert canonical_key(None) == "null"
    assert canonical_key(float("nan")) == "f:nan"
    assert canonical_key("") != canonical_key(None)
    assert canonical_key("  x ") != canonical_key("x")
    assert canonical_key(True) != canonical_key(1)


def test_temporal_precision_and_families() -> None:
    assert canonical_key(date(2024, 1, 1)) != canonical_key(datetime(2024, 1, 1))
    assert canonical_key(datetime(2024, 1, 1, 0, 0, 0, 1)) != canonical_key(datetime(2024, 1, 1))
    assert canonical_key(
        datetime(2024, 1, 1, tzinfo=__import__("datetime").timezone.utc)
    ) != canonical_key(datetime(2024, 1, 1))


def test_unsupported_types() -> None:
    with pytest.raises(TypeError):
        canonical_key([1, 2])


def test_value_encoding_roundtrip() -> None:
    for value in (
        None,
        "",
        "q\n\u2603",
        b"\x00\xff",
        2**63 - 1,
        Decimal("1.25"),
        date(2020, 2, 3),
        datetime(2020, 2, 3, 4, 5, 6, 123456),
    ):
        typed = typed_value(value)
        assert typed is None or isinstance(typed, dict)


@given(st.text(max_size=30))
def test_name_normalization_idempotence(name: str) -> None:
    assert normalize_name(normalize_name(name)) == normalize_name(name)


def test_discovery_pairing_matrix() -> None:
    sas = [
        FileEntry("dplocal", "A", "A.SAS7BDAT", "/a"),
        FileEntry("msoc", "B", "b.sas7bdat", "/b"),
    ]
    python = [
        FileEntry("DPLOCAL", "a", "a.parquet", "/c"),
        FileEntry("msoc", "c", "c.parquet", "/d"),
    ]
    paired = pair_files(sas, python)
    assert len(paired.matched) == 1
    assert len(paired.sas_only) == len(paired.python_only) == 1
    with pytest.raises(ValueError):
        pair_files(sas + [FileEntry("dplocal", "a", "a.sas7bdat", "/e")], python)
