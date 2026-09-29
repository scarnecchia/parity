# pattern: Imperative Shell
"""Run safe dataset discovery, staged comparison, and report publication."""

from __future__ import annotations

import os
import shutil
import sys
import tempfile
import time
import uuid
from contextlib import suppress
from datetime import UTC, datetime
from importlib.metadata import version
from pathlib import Path
from typing import TYPE_CHECKING, Any

from sentinel_parity.core.discovery import normalize_identity_stem, pair_files
from sentinel_parity.core.export_policy import ExcelLimits, WorkbookMeasurements, excel_omissions
from sentinel_parity.io import run_log
from sentinel_parity.io.discovery import discover, validate_roots_and_output
from sentinel_parity.io.duckdb_comparison import compare
from sentinel_parity.io.report_writer import publish
from sentinel_parity.io.schema_config import load_schema
from sentinel_parity.io.staging import stage
from sentinel_parity.io.workbook_writer import safe_sheet_names, write_workbook

if TYPE_CHECKING:
    from sentinel_parity.config import RunConfig
    from sentinel_parity.core.discovery import Pairing


def _table_pair_keys(config: RunConfig, pairing: Pairing) -> dict[str, tuple[str, ...]]:
    """Resolve per-table pairing keys from the optional schema.toml file.

    Validation always runs so a typo is reported even when the keys would not
    be applied; application is suppressed by ignore_pair_keys.
    """
    if config.schema is None:
        return {}
    tables = load_schema(config.schema)
    compared = {normalize_identity_stem(sas.stem) for sas, _ in pairing.matched}
    unknown = sorted(set(tables) - compared)
    if unknown:
        raise ValueError(
            "schema.toml has sections matching no compared table: " + ", ".join(unknown)
        )
    run_log.event("schema_loaded", tables=len(tables))
    return tables


def run(config: RunConfig, *, verbose: bool = False) -> int:
    """Run safe dataset discovery, staged comparison, and report publication."""
    run_log.start()
    run_log.set_verbose(verbose)
    try:
        return _execute(config)
    finally:
        run_log.close()


def _execute(config: RunConfig) -> int:
    run_log.event(
        "run_start",
        sas_root=str(config.sas_root),
        python_root=str(config.python_root),
        output_dir=str(config.effective_output_dir),
        request_id=config.id,
        threads=config.threads,
        memory_limit=config.memory_limit,
        max_temp_size=config.max_temp_size,
        batch_size=config.batch_size,
        preview_rows=config.preview_rows,
        round_digits=config.round_digits,
        temp_dir=str(config.temp_dir) if config.temp_dir else None,
    )
    _warn_on_tmpfs_temp(config)
    validate_roots_and_output(config)
    for root in (config.sas_root, config.python_root):
        if not root.is_dir() or not os.access(root, os.R_OK):
            raise ValueError("input roots must exist and be readable")
    sas_files = discover(config.sas_root, ".sas7bdat")
    python_files = discover(config.python_root, ".parquet")
    pairing = pair_files(sas_files, python_files)
    run_log.event(
        "discovery_done",
        sas_files=len(sas_files),
        python_files=len(python_files),
        matched=len(pairing.matched),
        sas_only=len(pairing.sas_only),
        python_only=len(pairing.python_only),
    )
    if not pairing.matched:
        raise ValueError("no matched dataset pairs found")
    table_pair_keys = _table_pair_keys(config, pairing)
    output = config.effective_output_dir.resolve()
    output.mkdir(parents=True, exist_ok=True)
    run_log.attach(output / "run.jsonl")
    parent = config.temp_dir.resolve() if config.temp_dir else None
    run_temp = Path(tempfile.mkdtemp(prefix="sentinel-parity-", dir=parent))
    datasets: list[dict[str, Any]] = []
    details: dict[str, Path] = {}
    previews: dict[str, list[dict[str, Any]]] = {}
    preview_truncation: dict[str, dict[str, int]] = {}
    staged_paths: dict[str, Path] = {}
    staged_ids: set[str] = set()
    workbook_reasons: list[dict[str, str | None]] = []
    workbook_remaining = {
        "rows": min(config.excel_max_rows, 1_048_575),
        "rows_per_dataset": min(config.excel_max_rows_per_sheet, 1_048_575),
        "bytes": config.excel_max_bytes,
        "sheets": min(config.excel_max_sheets, 100) - 1,
    }
    fatal = False
    any_fail = bool(pairing.sas_only or pairing.python_only)
    any_warn = False
    try:
        # Pairing already proved these files have no equivalent on the other
        # side, so they are never opened: unmatched inputs can be huge, and
        # staging them would only recover counts the comparison cannot use.
        for side, entries in (("sas", pairing.sas_only), ("python", pairing.python_only)):
            for entry in entries:
                ident = uuid.uuid4().hex
                name = f"{entry.directory}/{entry.filename}"
                run_log.event("dataset_start", dataset=ident, name=name)
                started = time.monotonic()
                rows: int | None = None  # unknown: the file is deliberately not read
                datasets.append(
                    {
                        "id": ident,
                        "name": name,
                        "status": "FAIL",
                        "reason": "missing_counterpart",
                        "sas_rows": rows if side == "sas" else 0,
                        "python_rows": 0 if side == "sas" else rows,
                        "matched_pairs": 0,
                        "sas_only": rows if side == "sas" else 0,
                        "python_only": 0 if side == "sas" else rows,
                        "differing_column_counts": {},
                        "differing_pair_count": 0,
                        "conditions": [{"severity": "FAIL", "reason": "missing_counterpart"}],
                        "detail_complete": False,
                        "metadata": {},
                    }
                )
                run_log.event(
                    "dataset_done",
                    dataset=ident,
                    name=name,
                    status="FAIL",
                    reason="missing_counterpart",
                    duration_s=round(time.monotonic() - started, 3),
                )
        matched_pairs = sorted(
            pairing.matched, key=lambda pair: f"{pair[0].directory}/{pair[0].filename}".casefold()
        )
        for sas, python in matched_pairs:
            ident = uuid.uuid4().hex
            # Per-table schema.toml keys override the global pair_keys default;
            # both are then filtered to the columns this dataset shares.
            # ignore_pair_keys (--no-pair-keys) suppresses every declared
            # source, including the schema file.
            pair_keys = (
                ()
                if config.ignore_pair_keys
                else table_pair_keys.get(normalize_identity_stem(sas.stem), config.pair_keys)
            )
            dataset_work = run_temp / f"dataset-{ident}"
            dataset_work.mkdir()
            name = f"{sas.directory}/{sas.filename}"
            run_log.event("dataset_start", dataset=ident, name=name)
            started = time.monotonic()
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
                    threads=config.threads,
                    preview_rows=config.preview_rows,
                    preview_cell_chars=config.preview_cell_chars,
                    pair_keys=pair_keys,
                    workbook_stage_path=(
                        dataset_work / "workbook.parquet"
                        if config.excel and not workbook_reasons
                        else None
                    ),
                    workbook_remaining=workbook_remaining,
                )
                details[ident] = Path(result["details_path"])
                stage_reasons = result["workbook_stage_reasons"]
                if stage_reasons and not workbook_reasons:
                    # Only per-sheet row-limit reasons identify a dataset.
                    for reason in stage_reasons:
                        code, _, attributed = reason.partition(":")
                        workbook_reasons.append(
                            {
                                "code": code,
                                "dataset_id": attributed if code == "sheet_row_limit" else None,
                            }
                        )
                    for leftover in staged_paths.values():
                        leftover.unlink(missing_ok=True)
                    staged_paths.clear()
                staged_path = result.get("workbook_stage_path")
                if staged_path is not None and not workbook_reasons:
                    staged_paths[ident] = Path(staged_path)
                    staged_ids.add(ident)
                    measurements = result["workbook_measurements"]
                    workbook_remaining["rows"] -= measurements["rows"]
                    workbook_remaining["bytes"] -= measurements["text_bytes"]
                    workbook_remaining["sheets"] -= measurements["sheets"] - 1
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
                preview_records = result["preview_rows"]
                difference_count = result["difference_row_count"]
                details_bytes = result["details_bytes"]
                truncation: dict[str, Any] = {}
                column_counts = result["differing_column_counts"]
                pair_count = result["differing_pair_count"]
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
                        "sas_only_columns": result.get("sas_only_columns", []),
                        "python_only_columns": result.get("python_only_columns", []),
                        "conditions": result.get("conditions", []),
                        "differing_column_counts": dict(
                            sorted(column_counts.items(), key=lambda item: (-item[1], item[0]))
                        ),
                        "differing_pair_count": pair_count,
                        "difference_row_count": difference_count,
                        "details_bytes": details_bytes,
                        "pair_keys": result["pair_keys"],
                        "unused_pair_keys": result["unused_pair_keys"],
                        "preview_truncation": truncation,
                        "workbook_measurements": result["workbook_measurements"],
                        "detail_complete": True,
                        "metadata": metadata,
                    }
                )
                previews[ident] = preview_records
                preview_truncation[ident] = truncation
                run_log.event(
                    "dataset_done",
                    dataset=ident,
                    name=name,
                    status=result["status"],
                    reason=result["reason"],
                    matched=result["matched"],
                    sas_only=result["sas_only"],
                    python_only=result["python_only"],
                    order_mismatches=result["order_mismatches"],
                    duration_s=round(time.monotonic() - started, 3),
                )
            except Exception as exc:
                fatal = True
                run_log.error("dataset_error", exc, dataset=ident, name=name)
                datasets.append(
                    {
                        "id": ident,
                        "name": f"{sas.directory}/{sas.filename}",
                        "status": "ERROR",
                        "reason": type(exc).__name__,
                        "error_message": str(exc),
                        "sas_rows": 0,
                        "python_rows": 0,
                        "matched_pairs": 0,
                        "sas_only": 0,
                        "python_only": 0,
                        "conditions": [],
                        "detail_complete": False,
                        "metadata": {},
                    }
                )
        status = "ERROR" if fatal else ("FAIL" if any_fail else ("WARN" if any_warn else "PASS"))
        datasets.sort(key=lambda item: item["name"].casefold())
        # Include measurements from staging released after a mid-run budget trip.
        staged_items = [item for item in datasets if item["id"] in staged_ids]
        workbook_rows = sum(
            int(item.get("workbook_measurements", {}).get("rows", 0)) for item in staged_items
        )
        workbook_data_bytes = sum(
            int(item.get("workbook_measurements", {}).get("text_bytes", 0)) for item in staged_items
        )
        staged_datasets = staged_items
        sheet_names = safe_sheet_names([item["name"] for item in staged_datasets])
        index_rows = [
            (
                item["name"],
                item["status"],
                ", ".join(
                    str(condition.get("reason", "")) for condition in item.get("conditions", [])
                ),
                sheet_names.get(item["name"], ""),
            )
            for item in datasets
        ]
        index_bytes = sum(len(value.encode("utf-8")) for row in index_rows for value in row) + sum(
            len(value.encode("utf-8")) for value in ("Dataset", "Status", "Conditions", "Sheet")
        )
        workbook_bytes = workbook_data_bytes + index_bytes
        index_max_cell = max(
            (len(value) for row in index_rows for value in row),
            default=0,
        )
        workbook_max_cell = max(
            [index_max_cell]
            + [
                int(item.get("workbook_measurements", {}).get("max_cell_chars", 0))
                for item in staged_items
            ]
        )
        workbook_sheets = 1 + len(staged_datasets)
        workbook_max_sheet_rows = max(
            (int(item.get("workbook_measurements", {}).get("rows", 0)) for item in staged_items),
            default=0,
        )
        workbook_measurements = WorkbookMeasurements(
            workbook_rows,
            workbook_bytes,
            workbook_max_cell,
            workbook_sheets,
            workbook_max_sheet_rows,
        )
        excel_limits = ExcelLimits(
            config.excel,
            config.excel_max_sheets,
            config.excel_max_rows,
            config.excel_max_rows_per_sheet,
            config.excel_max_bytes,
        )
        excel_reasons = [
            {"code": reason, "dataset_id": None}
            for reason in excel_omissions(workbook_measurements, excel_limits)
        ]
        if len(index_rows) + 1 > 1_048_576 and config.excel:
            excel_reasons.append({"code": "index_row_limit", "dataset_id": None})
        # workbook_max_cell includes Index cells. Staging checks per-dataset row limits.
        has_workbook_differences = workbook_rows > 0 or any(
            item.get("conditions") for item in datasets if item["status"] != "PASS"
        )
        if config.excel:
            excel_reasons.extend(
                reason
                for reason in workbook_reasons
                if not any(
                    existing["code"] == reason["code"]
                    and existing["dataset_id"] == reason["dataset_id"]
                    for existing in excel_reasons
                )
            )
        workbook_status = (
            "disabled"
            if not config.excel
            else "omitted"
            if excel_reasons
            else "generated"
            if has_workbook_differences
            else "no_differences"
        )
        if workbook_status != "generated" and staged_paths:
            # The workbook will not be written; release staging now instead of
            # holding the files until run-temp cleanup.
            for leftover in staged_paths.values():
                leftover.unlink(missing_ok=True)
            staged_paths.clear()
        summary = {
            "schema_version": 3,
            "details_schema_version": 2,
            "excel": {
                "status": workbook_status,
                "path": None,
                "reasons": excel_reasons,
                "limits": {
                    "max_sheets": config.excel_max_sheets,
                    "max_rows": config.excel_max_rows,
                    "max_rows_per_sheet": config.excel_max_rows_per_sheet,
                    "max_bytes": config.excel_max_bytes,
                },
                "measurements": {
                    "rows": workbook_rows,
                    "text_bytes": workbook_bytes,
                    "max_cell_chars": workbook_max_cell,
                },
            },
            "status": status,
            "run_at": datetime.now(UTC).isoformat(timespec="seconds"),
            "request_id": config.id,
            "inputs": {
                "sas_root": str(config.sas_root.resolve()),
                "python_root": str(config.python_root.resolve()),
            },
            "versions": _versions(),
            "limits": {
                "memory_limit": config.memory_limit,
                "max_temp_size": config.max_temp_size,
                "batch_size": config.batch_size,
                "preview_rows": config.preview_rows,
                "preview_max_bytes": config.preview_max_bytes,
                "preview_total_max_bytes": config.preview_total_max_bytes,
                "preview_cell_chars": config.preview_cell_chars,
                "excel": config.excel,
                "excel_max_sheets": config.excel_max_sheets,
                "excel_max_rows": config.excel_max_rows,
                "excel_max_rows_per_sheet": config.excel_max_rows_per_sheet,
                "excel_max_bytes": config.excel_max_bytes,
                "round_digits": config.round_digits,
                "threads": config.threads,
            },
            "datasets": datasets,
            "difference_row_count": sum(item.get("difference_row_count", 0) for item in datasets),
            "details_bytes": sum(item.get("details_bytes", 0) for item in datasets),
            "preview_truncation": preview_truncation,
        }
        workbook_source: Path | None = None
        if workbook_status == "generated":
            workbook_source = run_temp / "differences.xlsx"
            write_workbook(workbook_source, datasets, dict(staged_paths), run_temp)
            summary["excel"]["path"] = "differences.xlsx"
        run_log.event("publish_start", datasets=len(datasets), details=len(details))
        try:
            publish(output, summary, details, previews, preview_truncation, workbook_source)
        except BaseException as exc:
            with suppress(Exception):
                run_log.error("publish_error", exc)
            raise
        run_log.event("publish_done", report=str(output / "index.html"))
        print(f"{status}: {len(datasets)} datasets; report {output / 'index.html'}")
        run_log.event("run_done", status=status, datasets=len(datasets))
        return 2 if fatal else 1 if any_fail else 0
    finally:
        shutil.rmtree(run_temp, ignore_errors=True)


def _versions() -> dict[str, str]:
    return {
        name: version(name)
        for name in ("sentinel-parity", "polars-readstat", "polars", "pyarrow", "duckdb", "jinja2")
    }


def _mount_entries() -> list[tuple[str, str]]:
    """(mount point, filesystem type) pairs; empty where /proc is absent."""
    try:
        text = Path("/proc/mounts").read_text(encoding="utf-8")
    except OSError:
        return []
    entries: list[tuple[str, str]] = []
    for line in text.splitlines():
        parts = line.split()
        if len(parts) >= 3:
            entries.append((parts[1].replace("\\040", " "), parts[2]))
    return entries


def _is_tmpfs(path: Path, entries: list[tuple[str, str]] | None = None) -> bool:
    """True when the deepest mount covering path is a tmpfs filesystem."""
    if entries is None:
        entries = _mount_entries()
    covering = [(mount, fstype) for mount, fstype in entries if path.is_relative_to(mount)]
    if not covering:
        return False
    mount, fstype = max(covering, key=lambda entry: len(entry[0]))
    return fstype == "tmpfs"


def _warn_on_tmpfs_temp(config: RunConfig) -> None:
    base = config.temp_dir.resolve() if config.temp_dir else Path(tempfile.gettempdir())
    if not _is_tmpfs(base):
        return
    run_log.event("temp_on_tmpfs", temp_dir=str(base), filesystem="tmpfs")
    print(
        f"warning: run temp directory {base} is on tmpfs, so DuckDB spill "
        "counts against RAM; pass --temp-dir to place the run temp on real disk",
        file=sys.stderr,
    )
