# pattern: Imperative Shell
"""Run safe dataset discovery, staged comparison, and report publication."""

from __future__ import annotations

import json
import os
import shutil
import tempfile
import uuid
from importlib.metadata import version
from pathlib import Path
from typing import TYPE_CHECKING, Any

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from sentinel_parity.core.discovery import FileEntry, pair_files
from sentinel_parity.io.discovery import discover, validate_roots_and_output
from sentinel_parity.io.duckdb_comparison import _unsupported, compare
from sentinel_parity.io.report_writer import publish
from sentinel_parity.io.sas_input import stage_sas_input
from sentinel_parity.io.staging import _encode_scalar, stage

if TYPE_CHECKING:
    from sentinel_parity.config import RunConfig


def run(config: RunConfig) -> int:
    validate_roots_and_output(config)
    for root in (config.sas_root, config.python_root):
        if not root.is_dir() or not os.access(root, os.R_OK):
            raise ValueError("input roots must exist and be readable")
    sas_files = discover(config.sas_root, ".sas7bdat")
    python_files = discover(config.python_root, ".parquet")
    pairing = pair_files(sas_files, python_files)
    if not pairing.matched:
        raise ValueError("no matched dataset pairs found")
    if config.output_dir.exists() and any(config.output_dir.iterdir()):
        raise ValueError("output directory must be empty")
    output = config.output_dir.resolve()
    output.mkdir(parents=True, exist_ok=True)
    parent = config.temp_dir.resolve() if config.temp_dir else None
    run_temp = Path(tempfile.mkdtemp(prefix="sentinel-parity-", dir=parent))
    datasets: list[dict[str, Any]] = []
    details: dict[str, Path] = {}
    previews: dict[str, list[dict[str, Any]]] = {}
    preview_truncation: dict[str, dict[str, int]] = {}
    fatal = False
    any_fail = bool(pairing.sas_only or pairing.python_only)
    any_warn = False
    try:
        for entry in pairing.sas_only:
            ident = uuid.uuid4().hex
            detail = run_temp / f"{ident}.jsonl"
            try:
                staged_sas = stage_sas_input(Path(entry.path), run_temp)
                rows = _row_count(str(staged_sas), "sas")
                metadata = _metadata(str(staged_sas), "sas")
                _write_single_side_failure(
                    detail,
                    ident,
                    entry,
                    "sas",
                    config.batch_size,
                    "missing_counterpart",
                    reader_path=str(staged_sas),
                    round_digits=config.round_digits,
                )
                details[ident] = detail
                previews[ident], preview_truncation[ident] = _preview(detail, config.preview_rows)
                datasets.append(
                    {
                        "id": ident,
                        "name": f"{entry.directory}/{entry.filename}",
                        "status": "FAIL",
                        "reason": "missing_counterpart",
                        "sas_rows": rows,
                        "python_rows": 0,
                        "matched_pairs": 0,
                        "sas_only": rows,
                        "python_only": 0,
                        "detail_complete": True,
                        "metadata": metadata,
                    }
                )
            except Exception as exc:
                fatal = True
                datasets.append(_one_sided_error(ident, entry, exc))
        for entry in pairing.python_only:
            ident = uuid.uuid4().hex
            detail = run_temp / f"{ident}.jsonl"
            try:
                rows = _row_count(entry.path, "python")
                metadata = _metadata(entry.path, "python")
                _write_single_side_failure(
                    detail,
                    ident,
                    entry,
                    "python",
                    config.batch_size,
                    "missing_counterpart",
                    round_digits=config.round_digits,
                )
                details[ident] = detail
                previews[ident], preview_truncation[ident] = _preview(detail, config.preview_rows)
                datasets.append(
                    {
                        "id": ident,
                        "name": f"{entry.directory}/{entry.filename}",
                        "status": "FAIL",
                        "reason": "missing_counterpart",
                        "sas_rows": 0,
                        "python_rows": rows,
                        "matched_pairs": 0,
                        "sas_only": 0,
                        "python_only": rows,
                        "detail_complete": True,
                        "metadata": metadata,
                    }
                )
            except Exception as exc:
                fatal = True
                datasets.append(_one_sided_error(ident, entry, exc))
        for sas, python in pairing.matched:
            ident = uuid.uuid4().hex
            dataset_work = run_temp / f"dataset-{ident}"
            dataset_work.mkdir()
            try:
                sas_stage = stage(
                    Path(sas.path), "sas", dataset_work, config.batch_size, config.round_digits
                )
                python_stage = stage(
                    Path(python.path),
                    "python",
                    dataset_work,
                    config.batch_size,
                    config.round_digits,
                )
                result = compare(
                    sas_stage,
                    python_stage,
                    dataset_work,
                    config.memory_limit,
                    config.max_temp_size,
                    ident,
                )
                details[ident] = Path(result["details_path"])
                any_fail |= result["status"] == "FAIL"
                any_warn |= result["status"] == "WARN"
                metadata = {
                    "sas": {
                        "columns": sas_stage["original_columns"],
                        "types": sas_stage["types"],
                        "reader": sas_stage["metadata"],
                    },
                    "python": {
                        "columns": python_stage["original_columns"],
                        "types": python_stage["types"],
                    },
                }
                datasets.append(
                    {
                        "id": ident,
                        "name": f"{sas.directory}/{sas.filename}",
                        "sas_filename": sas.filename,
                        "python_filename": python.filename,
                        "sas_original_describe": sas_stage["original_describe"],
                        "sas_describe": sas_stage["describe"],
                        "python_original_describe": python_stage["original_describe"],
                        "python_describe": python_stage["describe"],
                        "status": result["status"],
                        "reason": result["reason"],
                        "sas_rows": sas_stage["rows"],
                        "python_rows": python_stage["rows"],
                        "matched_pairs": result["matched"],
                        "sas_only": result["sas_only"],
                        "python_only": result["python_only"],
                        "row_order_mismatches": result["order_mismatches"],
                        "type_mismatched_columns": result["type_mismatched_columns"],
                        "detail_complete": True,
                        "metadata": metadata,
                    }
                )
                previews[ident], preview_truncation[ident] = _preview(
                    Path(result["details_path"]), config.preview_rows
                )
            except Exception as exc:
                fatal = True
                datasets.append(
                    {
                        "id": ident,
                        "name": f"{sas.directory}/{sas.filename}",
                        "status": "ERROR",
                        "reason": type(exc).__name__,
                        "sas_rows": 0,
                        "python_rows": 0,
                        "matched_pairs": 0,
                        "sas_only": 0,
                        "python_only": 0,
                        "detail_complete": False,
                        "metadata": {},
                    }
                )
        status = "ERROR" if fatal else ("FAIL" if any_fail else ("WARN" if any_warn else "PASS"))
        datasets.sort(key=lambda item: item["name"].casefold())
        summary = {
            "schema_version": 1,
            "status": status,
            "versions": _versions(),
            "limits": {
                "memory_limit": config.memory_limit,
                "max_temp_size": config.max_temp_size,
                "batch_size": config.batch_size,
                "preview_rows": config.preview_rows,
                "round_digits": config.round_digits,
            },
            "datasets": datasets,
            "preview_truncation": preview_truncation,
        }
        publish(output, summary, details, previews, preview_truncation)
        print(f"{status}: {len(datasets)} datasets; report {output / 'index.html'}")
        return 2 if fatal else 1 if any_fail else 0
    finally:
        shutil.rmtree(run_temp, ignore_errors=True)


def _versions() -> dict[str, str]:
    return {
        name: version(name)
        for name in ("sentinel-parity", "polars-readstat", "polars", "pyarrow", "duckdb", "jinja2")
    }


def _row_count(path: str, kind: str) -> int:
    if kind == "sas":
        import polars_readstat

        return int(polars_readstat.ScanReadstat(path).metadata["row_count"])
    return int(pq.ParquetFile(path).metadata.num_rows)  # type: ignore[no-untyped-call]


def _metadata(path: str, kind: str) -> dict[str, Any]:
    if kind == "sas":
        import polars_readstat

        reader = polars_readstat.ScanReadstat(path)
        return {
            "schema": {key: str(value) for key, value in reader.schema.items()},
            "reader": reader.metadata,
        }
    return {"schema": {key: str(value) for key, value in pl.read_parquet_schema(path).items()}}


def _write_single_side_failure(
    destination: Path,
    ident: str,
    entry: FileEntry,
    side: str,
    batch_size: int,
    reason: str,
    *,
    reader_path: str | None = None,
    round_digits: int | None = None,
) -> None:
    if side == "sas":
        import polars_readstat

        frame = polars_readstat.scan_readstat(reader_path or entry.path, batch_size=batch_size)
    else:
        frame = pl.scan_parquet(entry.path)
    schema = frame.collect_schema()
    if any(_unsupported(str(dtype)) for dtype in schema.dtypes()):
        raise TypeError("unsupported logical column type")
    columns = list(schema.names())
    names = {name.casefold(): name for name in columns}
    if len(names) != len(columns):
        raise ValueError("duplicate normalized column names")
    with destination.open("w", encoding="utf-8") as output:
        ordinal = 0
        for batch in frame.collect_batches(chunk_size=batch_size):
            arrow = batch.to_arrow()
            encoded_columns: dict[str, list[Any]] = {}
            for normalized, original in names.items():
                array = arrow.column(original)
                if pa.types.is_timestamp(array.type):
                    raw_values = array.cast(pa.int64()).to_pylist()
                    scale = {"s": 1_000_000_000, "ms": 1_000_000, "us": 1_000, "ns": 1}[
                        array.type.unit
                    ]
                    aware = array.type.tz is not None
                    encoded_columns[normalized] = [
                        None
                        if value is None
                        else json.loads(
                            _encode_scalar(None, epoch_ns=value * scale, timezone_aware=aware)
                        )
                        for value in raw_values
                    ]
                else:
                    encoded_columns[normalized] = [
                        json.loads(_encode_scalar(value, round_digits=round_digits))
                        if value is not None
                        else None
                        for value in array.to_pylist()
                    ]
            for row_index in range(batch.height):
                values = {name: cells[row_index] for name, cells in encoded_columns.items()}
                output.write(
                    json.dumps(
                        {
                            "schema_version": 1,
                            "dataset_id": ident,
                            "side": side,
                            "staging_row_number": ordinal,
                            "status": "FAIL",
                            "reason": reason,
                            "values": values,
                        },
                        ensure_ascii=False,
                    )
                    + "\n"
                )
                ordinal += 1


def _one_sided_error(ident: str, entry: FileEntry, exc: Exception) -> dict[str, Any]:
    return {
        "id": ident,
        "name": f"{entry.directory}/{entry.filename}",
        "status": "ERROR",
        "reason": type(exc).__name__,
        "sas_rows": 0,
        "python_rows": 0,
        "matched_pairs": 0,
        "sas_only": 0,
        "python_only": 0,
        "detail_complete": False,
        "metadata": {},
    }


def _preview(path: Path, limit: int) -> tuple[list[dict[str, Any]], dict[str, int]]:
    result: list[dict[str, Any]] = []
    shown = {"sas": 0, "python": 0}
    total = {"sas": 0, "python": 0}
    with path.open(encoding="utf-8") as source:
        for line in source:
            row = json.loads(line)
            side = row["side"]
            if row["status"] == "FAIL" and side in total:
                total[side] += 1
                if shown[side] < limit:
                    result.append(row)
                    shown[side] += 1
    return result, {side: total[side] - shown[side] for side in total}
