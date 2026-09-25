# pattern: Functional Core
"""Exact vectorized canonical-key builders with scalar fallback masks.

Every builder produces key strings byte-identical to
`core.value_encoding.canonical_key`: a builder either covers a row exactly or
marks it residual for the scalar path.  Dtypes without a proven-exact builder
(unsigned widths, Decimal, binary) route the whole column through the scalar
path, and float columns do the same whenever `round_digits` is set, because
exact decimal rounding of binary floats has no vectorized form.
"""

from __future__ import annotations

import polars as pl

from sentinel_parity.core.value_encoding import canonical_key

_SIGNED_INTS = (pl.Int8, pl.Int16, pl.Int32, pl.Int64)
_EPOCH_SCALES = {"s": 1_000_000_000, "ms": 1_000_000, "us": 1_000, "ns": 1}

# den = 2**k must fit Int128 (whose maximum is 2**127 - 1).
_BIGGEST_DEN_EXPONENT = 126
# |value| below this keeps num = M << e within Int128 for integral floats.
_FLOAT_BOUND = float(2**75)
_NUMBER_PATTERN = r"^[+-]?[0-9]{1,18}$"  # int64-safe digits; rest is scalar


def vector_keys(values: pl.Series, round_digits: int | None) -> tuple[pl.Series, pl.Series] | None:
    """Canonical keys for a column when an exact vector builder exists.

    Returns `(keys, residual)`: `keys[i]` holds the canonical key string, or
    null exactly where row `i` must fall back to the scalar path.  Null
    values are keyed "null" in-vector, never residual.  `None` means the
    dtype has no exact builder and the whole column takes the scalar path.
    """
    if round_digits is not None and round_digits < 1:
        # The integer fast paths are only round-invariant for positive digit
        # counts; RunConfig rejects anything else at the CLI boundary.
        raise ValueError("round_digits must be a positive integer")
    dtype = values.dtype
    if dtype == pl.Boolean or dtype in _SIGNED_INTS:
        return _integer_keys(values), _no_residual(len(values))
    if dtype == pl.Null:
        return pl.Series(values.name, ["null"] * len(values), dtype=pl.Utf8), _no_residual(
            len(values)
        )
    if dtype == pl.Float64:
        return None if round_digits is not None else _float_keys(values)
    if dtype == pl.Float32:
        if round_digits is not None:
            return None
        return _float_keys(values.cast(pl.Float64))
    if dtype == pl.String:
        return _string_keys(values)
    if dtype == pl.Date or isinstance(dtype, pl.Datetime):
        return _temporal_keys(values), _no_residual(len(values))
    return None


def canonical_keys(values: pl.Series, round_digits: int | None) -> pl.Series:
    """Canonical keys for a column: vectorized where exact, scalar elsewhere."""
    built = vector_keys(values, round_digits)
    if built is None:
        return pl.Series(
            values.name,
            [canonical_key(value, round_digits=round_digits) for value in values.to_list()],
            dtype=pl.Utf8,
        )
    keys, residual = built
    if residual.any():
        rows = residual.arg_true()
        keys = keys.scatter(
            rows,
            [
                canonical_key(value, round_digits=round_digits)
                for value in values.gather(rows).to_list()
            ],
        )
    return keys


def _no_residual(length: int) -> pl.Series:
    return pl.repeat(False, length, dtype=pl.Boolean, eager=True)


def _integer_keys(values: pl.Series) -> pl.Series:
    return (
        values.to_frame()
        .select(
            pl.concat_str(
                pl.lit("n:"), pl.col(values.name).cast(pl.Int64).cast(pl.Utf8), pl.lit("/1")
            )
            .fill_null("null")
            .alias(values.name)
        )
        .to_series()
    )


def _float_keys(values: pl.Series) -> tuple[pl.Series, pl.Series]:
    name = values.name
    value = pl.col(name)
    # Infinities, huge magnitudes, and denominators beyond Int128 fall back;
    # nulls and NaNs stay in-vector as the "null" key.
    probe = value * pl.lit(float(2**_BIGGEST_DEN_EXPONENT))
    overflowing = (value.abs() >= pl.lit(_FLOAT_BOUND)) | ~(probe == probe.floor())
    residual = (value.is_infinite() | (value.is_finite() & overflowing)) & value.is_not_null()
    integral = value.is_not_null() & ~residual
    # Bisect the smallest k with value * 2**k integral; den tracks 2**k as an
    # exact power-of-two float.  Scaling a float by a power of two is exact,
    # so every integrality probe is exact, and k never goes below zero.
    frame = values.to_frame().with_columns(
        integral=integral,
        hi=pl.lit(_BIGGEST_DEN_EXPONENT, dtype=pl.Int32),
        den=pl.lit(float(2**_BIGGEST_DEN_EXPONENT)),
    )
    for step in (64, 32, 16, 8, 4, 2, 1):
        candidate_den = pl.col("den") / float(2**step)
        probe = value * candidate_den
        accepted = ((probe == probe.floor()) & (pl.col("hi") >= step)).fill_null(False) & pl.col(
            "integral"
        )
        frame = frame.with_columns(
            hi=pl.when(accepted).then(pl.col("hi") - step).otherwise(pl.col("hi")),
            den=pl.when(accepted).then(candidate_den).otherwise(pl.col("den")),
        )
    keys = frame.select(
        pl.when(pl.col(name).is_null())
        .then(pl.lit("null"))
        .when(pl.col(name).is_nan())
        .then(pl.lit("null"))
        .when(integral)
        .then(
            pl.concat_str(
                pl.lit("n:"),
                (pl.col(name) * pl.col("den")).cast(pl.Int128).cast(pl.Utf8),
                pl.lit("/"),
                pl.col("den").cast(pl.Int128).cast(pl.Utf8),
            )
        )
        .otherwise(None)
        .alias(name)
    ).to_series()
    return keys, values.to_frame().select(residual.alias(name)).to_series()


def _string_keys(values: pl.Series) -> tuple[pl.Series, pl.Series]:
    name = values.name
    text = pl.col(name)
    # Integers only: _text_key decides the numeric/text split, so every other
    # Decimal-accepted spelling routes through the scalar path untouched.
    matches = text.str.contains(_NUMBER_PATTERN).fill_null(False)
    keys = (
        values.to_frame()
        .select(
            pl.when(text.is_null())
            .then(pl.lit("null"))
            .when(matches)
            .then(
                pl.concat_str(
                    pl.lit("n:"),
                    text.str.to_integer(base=10, strict=False).cast(pl.Utf8),
                    pl.lit("/1"),
                )
            )
            .otherwise(None)
            .alias(name)
        )
        .to_series()
    )
    residual = (values.is_not_null() & ~matches).fill_null(False)
    return keys, values.to_frame().select(residual.alias(name)).to_series()


def _temporal_keys(values: pl.Series) -> pl.Series:
    dtype = values.dtype
    if dtype == pl.Date:
        epoch = values.cast(pl.Int128)
        scale = pl.lit(86_400_000_000_000, dtype=pl.Int128)
    else:
        unit = dtype.time_unit  # type: ignore[attr-defined]
        epoch = values.dt.epoch(time_unit=unit).cast(pl.Int128)
        scale = pl.lit(_EPOCH_SCALES[unit], dtype=pl.Int128)
    return (
        values.to_frame()
        .select(
            pl.when(pl.col(values.name).is_not_null())
            .then(pl.concat_str(pl.lit("dt-ns:"), (epoch * scale).cast(pl.Utf8)))
            .otherwise(pl.lit("null"))
            .alias(values.name)
        )
        .to_series()
    )


# repr() and Polars agree on the shortest round-trip digits of a float; they
# differ only in three spellings, fixed by float_repr: positional values in
# [1e-5, 1e-4) that repr() writes scientifically (always four leading
# fractional zeros, so the exponent is the constant e-05), single-digit
# negative exponents repr() zero-pads, and NaN spelled "NaN".
_SCIENTIFIC_SMALL = r"^(-?[1-9](?:\.\d+)?)e-([0-9])$"
_POSITIONAL_SMALL = r"^(-?)0\.0{4}([1-9][0-9]*)$"


def float_repr(values: pl.Series) -> pl.Series:
    """Python repr() display strings for a Float64 column."""
    name = values.name
    rendered = values.cast(pl.Utf8)
    signs = rendered.str.extract(_POSITIONAL_SMALL, 1)
    digits = rendered.str.extract(_POSITIONAL_SMALL, 2)
    mantissa = (
        pl.when(digits.str.len_bytes() == 1)
        .then(digits)
        .otherwise(pl.concat_str(digits.str.head(1), pl.lit("."), digits.str.slice(1)))
    )
    return (
        rendered.to_frame()
        .select(
            pl.when(signs.is_not_null())
            .then(pl.concat_str(signs.fill_null(""), mantissa, pl.lit("e-05")))
            .otherwise(pl.col(name).str.replace(_SCIENTIFIC_SMALL, "${1}e-0${2}"))
            .str.replace("NaN", "nan")
            .alias(name)
        )
        .to_series()
    )
