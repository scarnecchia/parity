# pattern: Imperative Shell
"""Bounded SAS/Parquet staging into canonical-key Parquet relations."""

from __future__ import annotations

import json
import uuid
from datetime import date, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any

import polars as pl
import polars_readstat
import pyarrow as pa
import pyarrow.parquet as pq

from sentinel_parity.core.names import normalize_columns
from sentinel_parity.core.value_encoding import (
    canonical_key,
    rounded_display,
    temporal_ns_iso,
    typed_value,
)
from sentinel_parity.core.vector_encoding import canonical_keys, float_repr
from sentinel_parity.io.sas_input import stage_sas_input

if TYPE_CHECKING:
    from pathlib import Path

BATCH_ROWS = 65536

# Strings needing JSON escapes ride the scalar path; everything else embeds
# verbatim between quotes.
_JSON_ESCAPABLE = r'["\\\x00-\x1f]'
_SIGNED_INTS = (pl.Int8, pl.Int16, pl.Int32, pl.Int64)


def _string_payloads(series: pl.Series) -> tuple[pl.Series, pl.Series]:
    name = series.name
    text = pl.col(name)
    escapable = text.str.contains(_JSON_ESCAPABLE).fill_null(False)
    payloads = (
        series.to_frame()
        .select(
            pl.when(text.is_null())
            .then(pl.lit("null"))
            .when(escapable)
            .then(None)
            .otherwise(pl.concat_str(pl.lit('{"type":"string","value":"'), text, pl.lit('"}')))
            .alias(name)
        )
        .to_series()
    )
    residual = (
        series.to_frame()
        .select((series.is_not_null() & escapable).fill_null(False).alias(name))
        .to_series()
    )
    return payloads, residual


def _payloads(
    series: pl.Series, keys: pl.Series, round_digits: int | None
) -> tuple[pl.Series, pl.Series]:
    """Vector envelope JSON strings, or the scalar path for rare shapes.

    Mirrors the vector_keys dispatch: unsigned widths, Decimal, binary, and
    floats under round_digits encode scalar-only; escaped strings mark
    individual rows residual.
    """
    dtype = series.dtype
    name = series.name
    if dtype == pl.Boolean:
        payloads = (
            series.to_frame()
            .select(
                pl.when(pl.col(name).is_null())
                .then(pl.lit("null"))
                .when(pl.col(name))
                .then(pl.lit('{"type":"boolean","value":true}'))
                .otherwise(pl.lit('{"type":"boolean","value":false}'))
                .alias(name)
            )
            .to_series()
        )
        return payloads, _no_residual(len(series))
    if dtype in _SIGNED_INTS:
        shown = pl.col(name).cast(pl.Int64).cast(pl.Utf8)
        payloads = (
            series.to_frame()
            .select(
                pl.when(pl.col(name).is_null())
                .then(pl.lit("null"))
                .otherwise(
                    pl.concat_str(
                        pl.lit('{"type":"int","value":"'),
                        shown,
                        pl.lit('","canonical":"n:'),
                        shown,
                        pl.lit('/1"}'),
                    )
                )
                .alias(name)
            )
            .to_series()
        )
        return payloads, _no_residual(len(series))
    if dtype in (pl.Float64, pl.Float32) and round_digits is None:
        display = float_repr(series.cast(pl.Float64))
        payloads = (
            series.to_frame()
            .select(
                pl.when(pl.col(name).is_null())
                .then(pl.lit("null"))
                .otherwise(
                    pl.concat_str(
                        pl.lit('{"type":"float","value":"'),
                        display,
                        pl.lit('","canonical":"'),
                        keys,
                        pl.lit('"}'),
                    )
                )
                .alias(name)
            )
            .to_series()
        )
        return payloads, _no_residual(len(series))
    if dtype == pl.String:
        return _string_payloads(series)
    # Scalar-only dtype: every row rides _encode_scalar, whose null payload
    # is the literal "null" string.
    return (
        pl.Series(name, [None] * len(series), dtype=pl.Utf8),
        pl.repeat(True, len(series), dtype=pl.Boolean, eager=True),
    )


def _no_residual(length: int) -> pl.Series:
    return pl.repeat(False, length, dtype=pl.Boolean, eager=True)


def _temporal_encode(series: pl.Series) -> tuple[pl.Series, pl.Series]:
    """Keys and envelope payloads for Date/Datetime columns; no fallbacks.

    Envelope values format from the column's own precision (nanosecond
    fraction padded to nine digits, aware instants converted to UTC), so
    even sub-1970 or year-9999 values stay exact.  Beyond year 9999 chrono
    renders its expanded-year form (leading +, six-digit year); the old
    scalar path raised there instead.
    """
    keys = canonical_keys(series, None)
    dtype = series.dtype
    name = series.name
    if dtype == pl.Date:
        value = pl.col(name).dt.to_string("%Y-%m-%d")
        shown = pl.concat_str(pl.lit('{"type":"date","value":"'), value, pl.lit('"}'))
    else:
        aware = dtype.time_zone is not None  # type: ignore[attr-defined]
        base = pl.col(name).dt.convert_time_zone("UTC") if aware else pl.col(name)
        shown = base.dt.to_string("%Y-%m-%dT%H:%M:%S%.9f")
        if aware:
            shown = pl.concat_str(shown, pl.lit("+00:00"))
        shown = pl.concat_str(pl.lit('{"type":"timestamp","value":"'), shown, pl.lit('"}'))
    payloads = (
        series.to_frame()
        .select(
            pl.when(pl.col(name).is_not_null()).then(shown).otherwise(pl.lit("null")).alias(name)
        )
        .to_series()
    )
    return keys, payloads


def _encode_column(series: pl.Series, round_digits: int | None) -> tuple[pl.Series, pl.Series]:
    """Canonical keys and envelope payloads for one staged column."""
    dtype = series.dtype
    if dtype == pl.Date or isinstance(dtype, pl.Datetime):
        return _temporal_encode(series)
    keys = canonical_keys(series, round_digits)
    payloads, residual = _payloads(series, keys, round_digits)
    if residual.any():
        rows = residual.arg_true()
        payloads = payloads.scatter(
            rows,
            [
                _encode_scalar(value, round_digits=round_digits)
                for value in series.gather(rows).to_list()
            ],
        )
    return keys, payloads


def _encode_scalar(
    value: Any,
    *,
    epoch_ns: int | None = None,
    timezone_aware: bool = False,
    round_digits: int | None = None,
) -> str:
    if epoch_ns is not None:
        return json.dumps(
            {"type": "timestamp", "value": temporal_ns_iso(epoch_ns, timezone_aware)},
            ensure_ascii=False,
            separators=(",", ":"),
        )
    if isinstance(value, datetime):
        return json.dumps(
            {"type": "timestamp", "value": value.isoformat(timespec="microseconds")},
            ensure_ascii=False,
            separators=(",", ":"),
        )
    if isinstance(value, date):
        return json.dumps(
            {"type": "date", "value": value.isoformat()}, ensure_ascii=False, separators=(",", ":")
        )
    if isinstance(value, Decimal):
        shown = rounded_display(value, round_digits)
        return json.dumps(
            {
                "type": "decimal",
                "value": str(shown),
                "canonical": canonical_key(value, round_digits=round_digits),
            },
            ensure_ascii=False,
            separators=(",", ":"),
        )
    return json.dumps(
        typed_value(value, round_digits=round_digits), ensure_ascii=False, separators=(",", ":")
    )


def stage(
    source: Path,
    kind: str,
    work: Path,
    batch_size: int = BATCH_ROWS,
    round_digits: int | None = None,
) -> dict[str, Any]:
    artifact_id = uuid.uuid4().hex
    raw_path = work / f"{artifact_id}.raw.parquet"
    canonical_path = work / f"{artifact_id}.parquet"
    staged_source: Path | None = None
    if kind == "sas":
        staged_source = stage_sas_input(source, work)
        reader = polars_readstat.ScanReadstat(str(staged_source), batch_size=batch_size)
        schema = reader.schema
        metadata = reader.metadata
        polars_readstat.scan_readstat(str(staged_source), batch_size=batch_size).sink_parquet(
            str(raw_path)
        )
    else:
        frame = pl.scan_parquet(source)
        schema = frame.collect_schema()
        metadata = {}
        frame.sink_parquet(str(raw_path))
    names = list(schema.keys())
    normalized = normalize_columns(names)
    by_normal = {column.normalized: column.original for column in normalized}
    ordered = list(by_normal)
    types = {column.normalized: str(schema[column.original]) for column in normalized}
    parquet = pq.ParquetFile(raw_path)  # type: ignore[no-untyped-call]
    rows = 0
    observed_max_batch_size = 0
    writer: pq.ParquetWriter | None = None
    progress_path = work / f"{artifact_id}.progress"
    for batch in parquet.iter_batches(batch_size=batch_size, columns=names):  # type: ignore[no-untyped-call]
        observed_max_batch_size = max(observed_max_batch_size, batch.num_rows)
        batch_frame = pl.from_arrow(pa.Table.from_batches([batch]))
        if not isinstance(batch_frame, pl.DataFrame):  # pragma: no cover - from_arrow contract
            raise TypeError("expected a DataFrame from the staged batch")
        arrays: list[pa.Array] = [
            batch_frame.select(
                pl.arange(rows, rows + batch_frame.height, dtype=pl.Int64).alias("ordinal")
            )
            .to_series()
            .to_arrow()
        ]
        fields = [pa.field("ordinal", pa.int64())]
        for normal_name in ordered:
            series = batch_frame.get_column(by_normal[normal_name]).rename(normal_name)
            keys, payloads = _encode_column(series, round_digits)
            # Polars renders Utf8 as large_string; the declared schema keeps
            # the physical type string, so the cast here is explicit.
            arrays.append(keys.to_arrow().cast(pa.string()))
            arrays.append(payloads.to_arrow().cast(pa.string()))
            fields.extend(
                (
                    pa.field(f"k_{normal_name}", pa.string()),
                    pa.field(f"v_{normal_name}", pa.string()),
                )
            )
        table = pa.Table.from_arrays(arrays, schema=pa.schema(fields))
        if writer is None:
            writer = pq.ParquetWriter(canonical_path, table.schema, compression="zstd")  # type: ignore[no-untyped-call]
        writer.write_table(table)  # type: ignore[no-untyped-call]
        rows += batch_frame.height
        progress_path.write_text(str(rows), encoding="ascii")
    if writer:
        writer.close()  # type: ignore[no-untyped-call]
    else:
        empty_schema = pa.schema(
            [pa.field("ordinal", pa.int64())]
            + [
                field
                for name in ordered
                for field in (
                    pa.field(f"k_{name}", pa.string()),
                    pa.field(f"v_{name}", pa.string()),
                )
            ]
        )
        pq.write_table(pa.Table.from_batches([], schema=empty_schema), canonical_path)  # type: ignore[no-untyped-call]
    import duckdb

    connection = duckdb.connect()
    try:
        original_describe = connection.execute(
            "DESCRIBE SELECT * FROM read_parquet(?)", [str(raw_path)]
        ).fetchall()
        describe = connection.execute(
            "DESCRIBE SELECT * FROM read_parquet(?)", [str(canonical_path)]
        ).fetchall()
    finally:
        connection.close()
    return {
        "id": artifact_id,
        "path": canonical_path,
        "rows": rows,
        "observed_max_batch_size": observed_max_batch_size,
        "columns": tuple(ordered),
        "original_columns": tuple(names),
        "types": types,
        "metadata": metadata,
        "original_describe": original_describe,
        "describe": describe,
    }
