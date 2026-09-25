import json
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from fractions import Fraction

import adversarial
import polars as pl
import pyarrow as pa
import pytest
from hypothesis import given
from hypothesis import strategies as st

from sentinel_parity.core.discovery import FileEntry, pair_files
from sentinel_parity.core.names import normalize_columns, normalize_name
from sentinel_parity.core.value_encoding import (
    canonical_key,
    numeric_key,
    round_to_decimal_places,
    rounded_display,
    temporal_ns_iso,
    temporal_ns_key,
    typed_value,
)
from sentinel_parity.core.vector_encoding import canonical_keys, float_repr, vector_keys


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


@given(st.integers(min_value=-(10**18), max_value=10**18))
def test_integer_values_match_across_widths(value: int) -> None:
    # Same value matches across declared widths; adjacent values never do.
    assert canonical_key(value) == canonical_key(Decimal(value))
    assert canonical_key(value + 1) != canonical_key(value)


def test_missing_forms_are_one_value() -> None:
    assert canonical_key(None) == "null"
    assert canonical_key(float("nan")) == "null"
    assert canonical_key(Decimal("NaN")) == "null"
    assert canonical_key("") == canonical_key("   ") == canonical_key(None)
    assert canonical_key("x ") == canonical_key("x")
    assert canonical_key(" x") != canonical_key("x")


def test_text_and_numbers_share_one_value_space() -> None:
    assert canonical_key(True) == canonical_key(1) == canonical_key("1") == "n:1/1"
    assert canonical_key(False) == canonical_key(0) == canonical_key("0")
    assert canonical_key("8.0") == canonical_key(8) == canonical_key(8.0)
    assert canonical_key("16.2") != canonical_key(16)
    assert canonical_key("abc") == "s:abc"
    assert canonical_key("abc") != canonical_key(0)


def test_decimal_place_rounding() -> None:
    assert round_to_decimal_places(Decimal("48.32832"), 4) == Decimal("48.3283")
    assert round_to_decimal_places(Decimal("0.84389"), 4) == Decimal("0.8439")
    assert round_to_decimal_places(Decimal("0.84385"), 4) == Decimal("0.8439")
    assert round_to_decimal_places(Decimal("-0.84385"), 4) == Decimal("-0.8439")
    assert round_to_decimal_places(Decimal("123456.7"), 4) == Decimal("123456.7")
    assert round_to_decimal_places(Decimal("-16.24681"), 4) == Decimal("-16.2468")
    assert round_to_decimal_places(Decimal("8"), 4) == Decimal("8")
    assert round_to_decimal_places(Decimal("0.00004"), 4) == Decimal("0")
    assert canonical_key(48.32832, round_digits=4) == canonical_key(
        Decimal("48.3283"), round_digits=4
    )
    assert canonical_key(48.32832) != canonical_key(48.3283)
    assert canonical_key("8.00001", round_digits=4) == canonical_key(8, round_digits=4)
    assert canonical_key(float("nan"), round_digits=4) == "null"
    assert typed_value(48.32832, round_digits=4)["value"] == "48.3283"


def test_temporal_values_match_across_declared_types() -> None:
    from datetime import timedelta, timezone

    # Values decide: a date equals its midnight instant, naive reads as UTC,
    # and aware timestamps normalize to the same instant key.
    assert canonical_key(date(2024, 1, 1)) == canonical_key(datetime(2024, 1, 1))
    assert canonical_key(datetime(2024, 1, 1, 0, 0, 0, 1)) != canonical_key(datetime(2024, 1, 1))
    assert canonical_key(datetime(2024, 1, 1, 5, tzinfo=UTC)) == canonical_key(
        datetime(2024, 1, 1, 5)
    )
    new_york = timezone(timedelta(hours=-5))
    assert canonical_key(datetime(2024, 1, 1, tzinfo=new_york)) == canonical_key(
        datetime(2024, 1, 1, 5)
    )
    assert canonical_key(date(2024, 1, 2)) != canonical_key(date(2024, 1, 1))


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


def test_dataset_identity_ignores_one_leading_run_prefix_on_either_side() -> None:
    sas_prefixed = FileEntry("dplocal", "R01_Products", "R01_Products.SAS7BDAT", "/sas")
    python_plain = FileEntry("DPLOCAL", "products", "products.parquet", "/python")
    first = pair_files([sas_prefixed], [python_plain])
    assert first.matched == ((sas_prefixed, python_plain),)

    sas_plain = FileEntry("dplocal", "products", "products.sas7bdat", "/sas")
    python_prefixed = FileEntry("dplocal", "r02_PRODUCTS", "r02_PRODUCTS.parquet", "/python")
    second = pair_files([sas_plain], [python_prefixed])
    assert second.matched == ((sas_plain, python_prefixed),)
    assert sas_prefixed.filename == "R01_Products.SAS7BDAT"
    assert python_prefixed.filename == "r02_PRODUCTS.parquet"


def test_dataset_identity_strips_only_one_leading_run_prefix() -> None:
    once = FileEntry("dplocal", "r01_r02_x", "r01_r02_x.sas7bdat", "/a")
    assert once.key == ("dplocal", "r02_x")
    plain = FileEntry("dplocal", "x", "x.parquet", "/b")
    result = pair_files([once], [plain])
    assert result.sas_only == (once,)
    assert result.python_only == (plain,)

    embedded = FileEntry("dplocal", "xr01_y", "xr01_y.sas7bdat", "/a")
    unrelated = FileEntry("dplocal", "y", "y.parquet", "/b")
    result = pair_files([embedded], [unrelated])
    assert result.sas_only == (embedded,)
    assert result.python_only == (unrelated,)


def test_stripped_dataset_identity_collision_is_rejected() -> None:
    prefixed = FileEntry("dplocal", "r01_x", "r01_x.sas7bdat", "/a")
    plain = FileEntry("dplocal", "x", "x.sas7bdat", "/b")
    with pytest.raises(ValueError, match="ambiguous case-insensitive dataset identity"):
        pair_files([prefixed, plain], [])


@given(st.text())
def test_text_keys_ignore_trailing_whitespace(text: str) -> None:
    assert canonical_key(text) == canonical_key(text.rstrip())


@given(st.decimals(allow_nan=False, allow_infinity=False), st.integers(min_value=0, max_value=12))
def test_rounding_is_idempotent(value: Decimal, digits: int) -> None:
    rounded = round_to_decimal_places(value, digits)
    assert round_to_decimal_places(rounded, digits) == rounded


@given(st.text(alphabet=" \t\n\r\x0b\x0c", max_size=8))
def test_whitespace_text_unifies_with_null(blank: str) -> None:
    assert canonical_key(blank) == "null"
    assert canonical_key(blank, round_digits=3) == "null"


@given(st.integers(min_value=-(2**54), max_value=2**54))
def test_temporal_keys_roundtrip_through_iso(micros: int) -> None:
    epoch_ns = micros * 1000
    for timezone_aware in (False, True):
        parsed = datetime.fromisoformat(temporal_ns_iso(epoch_ns, timezone_aware))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        delta = parsed.astimezone(UTC) - datetime(1970, 1, 1, tzinfo=UTC)
        assert (delta // timedelta(microseconds=1)) * 1000 == epoch_ns
        assert canonical_key(parsed) == temporal_ns_key(epoch_ns)


@given(st.integers(min_value=-(2**70), max_value=2**70))
def test_integer_and_float_keys_agree_only_when_exactly_representable(value: int) -> None:
    assert (canonical_key(value) == canonical_key(float(value))) == (
        Fraction(value) == Fraction(float(value))
    )


def _value_strategy() -> st.SearchStrategy[object]:
    return st.one_of(
        st.integers(),
        st.floats(),
        st.decimals(allow_nan=True, allow_infinity=True),
        st.text(),
        st.binary(),
        st.dates(),
        st.datetimes(timezones=st.one_of(st.none(), st.just(UTC))),
    )


@given(_value_strategy())
def test_envelopes_roundtrip_through_json(value: object) -> None:
    envelope = typed_value(value)
    assert envelope == json.loads(json.dumps(envelope, ensure_ascii=False))


def test_rounded_display_huge_integral_envelope() -> None:
    envelope = typed_value(1e308, round_digits=2)
    assert envelope is not None
    shown = str(envelope["value"])
    assert "E" not in shown
    assert Decimal(shown) == round_to_decimal_places(Decimal(1e308), 2)
    assert envelope["canonical"] == canonical_key(1e308, round_digits=2)
    assert rounded_display(Decimal("-0.0"), 2) == Decimal("-0")


def _series(name: str, values: list[object], dtype: pl.DataType | None = None) -> pl.Series:
    return pl.Series(name, values, dtype=dtype)  # type: ignore[arg-type]


def _assert_builder_matches_scalar(values: pl.Series, round_digits: int | None = None) -> None:
    built = vector_keys(values, round_digits)
    assert built is not None, values.dtype
    keys, residual = built
    assert keys.len() == values.len()
    for index in range(values.len()):
        expected = canonical_key(values[index], round_digits=round_digits)
        if residual[index]:
            assert keys[index] is None, index
        else:
            assert keys[index] == expected, index


def test_vector_keys_match_scalar_golden() -> None:
    """Every adversarial column keys identically to the scalar originals."""
    table = adversarial.adversarial_table("a")
    for column in table.column_names:
        if column == "Instant":
            # ns timestamps have no Python scalar; staging's scalar path keys
            # them from the epoch-ns integer via temporal_ns_key.
            raw = table.column(column).cast(pa.int64()).to_pylist()
            series = _series("instant", raw, pl.Int64).cast(pl.Datetime("ns"))
            keys, residual = vector_keys(series, None)
            assert keys is not None and residual is not None and not residual.any()
            for index, value in enumerate(raw):
                assert keys[index] == ("null" if value is None else temporal_ns_key(value)), index
            continue
        raw = table.column(column).to_pylist()
        if column in ("Exact", "Blob", "Tiny"):
            # Decimal, binary, and unsigned widths have no vector builder.
            dtype = {"Exact": pl.Object, "Blob": pl.Binary, "Tiny": pl.UInt8}[column]
            assert vector_keys(_series(column.lower(), raw, dtype), None) is None, column
            continue
        series = _series("moment", raw) if column == "Moment" else _series(column.lower(), raw)
        _assert_builder_matches_scalar(series)
        if column == "Amount":
            # Exact decimal rounding of binary floats has no vectorized form,
            # so round_digits sends the whole float column through the scalar
            # path.
            assert vector_keys(series, round_digits=2) is None
        elif column == "Code":
            # Pure-integer text is round-invariant, so its builder survives.
            _assert_builder_matches_scalar(series, round_digits=2)


@given(
    st.lists(
        st.one_of(st.integers(min_value=-(2**63), max_value=2**63 - 1), st.none()), max_size=40
    )
)
def test_vector_key_properties_integers(values: list[int | None]) -> None:
    _assert_builder_matches_scalar(_series("v", values))


@given(st.lists(st.one_of(st.floats(allow_nan=True, allow_infinity=True), st.none()), max_size=40))
def test_vector_key_properties_floats(values: list[float | None]) -> None:
    _assert_builder_matches_scalar(_series("v", values))


@given(st.lists(st.one_of(st.text(), st.none()), max_size=40))
def test_vector_key_properties_text(values: list[str | None]) -> None:
    _assert_builder_matches_scalar(_series("v", values))
    # Integer text is round-invariant, so the builder also holds with digits.
    _assert_builder_matches_scalar(_series("v", values), round_digits=2)


@given(st.lists(st.one_of(st.dates(), st.none()), max_size=40))
def test_vector_key_properties_dates(values: list[date | None]) -> None:
    _assert_builder_matches_scalar(_series("v", values))


@given(
    st.lists(
        st.one_of(st.datetimes(timezones=st.one_of(st.none(), st.just(UTC))), st.none()),
        max_size=40,
    )
)
def test_vector_key_properties_datetimes(values: list[datetime | None]) -> None:
    _assert_builder_matches_scalar(_series("v", values))


@given(st.lists(st.integers(min_value=-(2**63), max_value=2**63 - 1), max_size=20))
def test_vector_nanosecond_keys_match_temporal_ns_key(values: list[int]) -> None:
    series = _series("v", values, pl.Int64).cast(pl.Datetime("ns"))
    keys = canonical_keys(series, None)
    for index, value in enumerate(values):
        assert keys[index] == temporal_ns_key(value), index


def test_vector_builder_dispatch_whole_column_fallbacks() -> None:
    assert vector_keys(_series("v", [b"\x00", None]), None) is None
    assert vector_keys(_series("v", [Decimal("1.5"), None]), None) is None
    assert vector_keys(_series("v", [1, None], pl.UInt8), None) is None
    assert vector_keys(_series("v", [2**40, None], pl.UInt64), None) is None
    assert vector_keys(_series("v", [0.5, None], pl.Float32), 3) is None
    assert vector_keys(_series("v", [0.5, None], pl.Float64), 3) is None
    # Float32 widens to Float64 value-exactly, so its builder still applies.
    assert vector_keys(_series("v", [0.5, None], pl.Float32), None) is not None
    built = vector_keys(_series("v", [True, False, None]), None)
    assert built is not None
    keys, residual = built
    assert keys.to_list() == ["n:1/1", "n:0/1", "null"] and not residual.any()


def test_float_repr_matches_python_repr_on_specials() -> None:
    specials = [
        16.0,
        -0.0,
        0.0,
        5e-324,
        1e308,
        -1e308,
        1e-308,
        0.1,
        -16.2,
        float(2**53),
        1e15,
        1e16,
        1e-4,
        1e-5,
        1.5e-5,
        0.0000999,
        1e20,
        1e300,
        2.5,
        float("nan"),
        float("inf"),
        float("-inf"),
    ]
    rendered = float_repr(_series("v", specials))
    for value, got in zip(specials, rendered.to_list(), strict=True):
        assert got == repr(value), value


@given(st.lists(st.floats(allow_nan=True, allow_infinity=True), min_size=1, max_size=64))
def test_float_repr_matches_python_repr(values: list[float]) -> None:
    rendered = float_repr(_series("v", values))
    for value, got in zip(values, rendered.to_list(), strict=True):
        assert got == repr(value), value


def test_float_repr_matches_python_repr_seeded_sweep() -> None:
    import math
    import random
    import struct

    rng = random.Random(20260925)
    values = [rng.uniform(-1e6, 1e6) for _ in range(20000)]
    values += [rng.uniform(-1, 1) for _ in range(20000)]
    values += [10.0 ** rng.randint(-300, 300) for _ in range(20000)]
    values += [rng.getrandbits(53) / math.ldexp(1.0, -rng.randint(0, 1074)) for _ in range(20000)]
    values += [float(rng.getrandbits(64)) for _ in range(20000)]
    # Arbitrary bit patterns: subnormals, NaNs, both zeros, huge exponents.
    values += [
        struct.unpack("<d", struct.pack("<Q", bits))[0]
        for bits in (rng.getrandbits(64) for _ in range(20000))
    ]
    rendered = float_repr(pl.Series("v", values))
    for value, got in zip(values, rendered.to_list(), strict=True):
        assert got == repr(value), value
