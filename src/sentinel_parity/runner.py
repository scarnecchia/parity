# pattern: Imperative Shell
"""Run safe dataset discovery, staged comparison, and report publication."""

from __future__ import annotations

import json
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

from sentinel_parity.core.discovery import pair_files
from sentinel_parity.io import run_log
from sentinel_parity.io.discovery import discover, validate_roots_and_output
from sentinel_parity.io.duckdb_comparison import compare
from sentinel_parity.io.report_writer import publish
from sentinel_parity.io.staging import stage

if TYPE_CHECKING:
    from sentinel_parity.config import RunConfig


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
    output = config.effective_output_dir.resolve()
    output.mkdir(parents=True, exist_ok=True)
    run_log.attach(output / "run.jsonl")
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
        for sas, python in pairing.matched:
            ident = uuid.uuid4().hex
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
                preview_records, truncation, column_counts, pair_count = _preview(
                    Path(result["details_path"]), config.preview_rows
                )
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
        summary = {
            "schema_version": 2,
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
                "round_digits": config.round_digits,
                "threads": config.threads,
            },
            "datasets": datasets,
            "preview_truncation": preview_truncation,
        }
        run_log.event("publish_start", datasets=len(datasets), details=len(details))
        try:
            publish(output, summary, details, previews, preview_truncation)
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


def _preview(
    path: Path, limit: int
) -> tuple[list[dict[str, Any]], dict[str, int], dict[str, int], int]:
    """Collect bounded preview records plus mismatch stats from the full stream.

    The stats scan every detail record so the report story reflects all
    mismatches, not only the previewed prefix. Paired rows are counted once
    per pair (on the SAS record) so per-column counts describe row pairs.
    """
    result: list[dict[str, Any]] = []
    shown = {"sas": 0, "python": 0}
    total = {"sas": 0, "python": 0}
    column_counts: dict[str, int] = {}
    pair_count = 0
    with path.open(encoding="utf-8") as source:
        for line in source:
            row = json.loads(line)
            side = row["side"]
            if row["status"] == "FAIL" and side in total:
                total[side] += 1
                if shown[side] < limit:
                    result.append(row)
                    shown[side] += 1
                pair_id = row.get("pair_id")
                if pair_id and side == "sas":
                    pair_count += 1
                    for name in row.get("differing_columns") or []:
                        column_counts[name] = column_counts.get(name, 0) + 1
    return (
        result,
        {side: total[side] - shown[side] for side in total},
        column_counts,
        pair_count,
    )
