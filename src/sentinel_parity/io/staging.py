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
from sentinel_parity.io.sas_input import stage_sas_input

if TYPE_CHECKING:
    from pathlib import Path
from sentinel_parity.core.value_encoding import (
    canonical_key,
    temporal_ns_iso,
    temporal_ns_key,
    typed_value,
)

BATCH_ROWS = 65536


def _encode_scalar(value: Any, *, epoch_ns: int | None = None, timezone_aware: bool = False) -> str:
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
        return json.dumps(
            {"type": "decimal", "value": str(value), "canonical": canonical_key(value)},
            ensure_ascii=False,
            separators=(",", ":"),
        )
    return json.dumps(typed_value(value), ensure_ascii=False, separators=(",", ":"))


def stage(source: Path, kind: str, work: Path, batch_size: int = BATCH_ROWS) -> dict[str, Any]:
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
    timezone_aware: dict[str, bool] = {}
    for column in normalized:
        dtype = schema[column.original]
        if isinstance(dtype, pl.Datetime):
            timezone_aware[column.normalized] = dtype.time_zone is not None
    parquet = pq.ParquetFile(raw_path)  # type: ignore[no-untyped-call]
    rows = 0
    observed_max_batch_size = 0
    writer: pq.ParquetWriter | None = None
    progress_path = work / f"{artifact_id}.progress"
    for batch in parquet.iter_batches(batch_size=batch_size, columns=names):  # type: ignore[no-untyped-call]
        observed_max_batch_size = max(observed_max_batch_size, batch.num_rows)
        arrays: list[pa.Array] = [pa.array(range(rows, rows + batch.num_rows), type=pa.int64())]
        fields = [pa.field("ordinal", pa.int64())]
        for normal_name in ordered:
            original = by_normal[normal_name]
            index = names.index(original)
            arrow_array = batch.column(index)
            if pa.types.is_timestamp(arrow_array.type):
                array_timezone_aware = arrow_array.type.tz is not None
                epoch_values = arrow_array.cast(pa.int64()).to_pylist()
                unit_scale = {"s": 1_000_000_000, "ms": 1_000_000, "us": 1_000, "ns": 1}
                scale = unit_scale[arrow_array.type.unit]
                epoch_ns_values = [
                    None if value is None else value * scale for value in epoch_values
                ]
                keys = [
                    "null" if value is None else temporal_ns_key(value, array_timezone_aware)
                    for value in epoch_ns_values
                ]
                payloads = [
                    "null"
                    if value is None
                    else _encode_scalar(None, epoch_ns=value, timezone_aware=array_timezone_aware)
                    for value in epoch_ns_values
                ]
            else:
                values = arrow_array.to_pylist()
                keys = [canonical_key(value) for value in values]
                payloads = [_encode_scalar(value) for value in values]
            arrays.extend((pa.array(keys, type=pa.string()), pa.array(payloads, type=pa.string())))
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
        rows += batch.num_rows
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
        "timezone_aware": timezone_aware,
        "metadata": metadata,
        "original_describe": original_describe,
        "describe": describe,
    }
