# pattern: Imperative Shell
"""Measure comparison resource use in child processes with fixed DuckDB limits."""

from __future__ import annotations

import argparse
import json
import os
import resource
import shutil
import subprocess
import sys
import time
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parents[1]


def run_worker(rows: int, mismatches: int) -> dict[str, int | float]:
    """Generate one synthetic pair, compare it, and report output and peak-RSS statistics."""
    from sentinel_parity.io.duckdb_comparison import compare
    from sentinel_parity.io.staging import stage

    started = time.monotonic()
    token = os.getpid()
    work = Path(f"resource-work-{token}")
    left_path = Path(f"left-{token}.parquet")
    right_path = Path(f"right-{token}.parquet")
    try:
        frame = pl.DataFrame({"id": list(range(rows)), "payload": ["a" * 96] * rows})
        frame.write_parquet(left_path)
        frame.with_columns(
            pl.when(pl.col("id") < mismatches)
            .then(pl.lit("b" * 96))
            .otherwise(pl.col("payload"))
            .alias("payload")
        ).write_parquet(right_path)
        work.mkdir()
        left = stage(left_path, "python", work, batch_size=4096)
        right = stage(right_path, "python", work, batch_size=4096)
        staged_bytes = sum(Path(item["path"]).stat().st_size for item in (left, right))
        result = compare(
            left,
            right,
            work,
            "128MB",
            "2GB",
            "resource-harness",
            threads=2,
            preview_rows=10,
        )
        return {
            "rows_per_side": rows,
            "mismatch_rows": result["difference_row_count"],
            "detail_bytes": result["details_bytes"],
            "staged_bytes": staged_bytes,
            "peak_rss_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
            "elapsed_seconds": round(time.monotonic() - started, 3),
        }
    finally:
        for path in (left_path, right_path):
            path.unlink(missing_ok=True)
        shutil.rmtree(work, ignore_errors=True)


def _measure(sizes: list[tuple[int, int]]) -> list[dict[str, int | float]]:
    measurements = []
    for rows, mismatches in sizes:
        completed = subprocess.run(
            [
                sys.executable,
                str(Path(__file__).resolve()),
                "--worker",
                "--rows",
                str(rows),
                "--mismatches",
                str(mismatches),
            ],
            cwd=ROOT / ".tmp",
            check=True,
            capture_output=True,
            text=True,
        )
        measurements.append(json.loads(completed.stdout))
    return measurements


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--worker", action="store_true")
    parser.add_argument("--rows", type=int, default=10_000)
    parser.add_argument("--mismatches", type=int, default=1_000)
    parser.add_argument("--experiment", action="store_true")
    args = parser.parse_args()
    if args.worker:
        print(json.dumps(run_worker(args.rows, args.mismatches), sort_keys=True))
        return
    (ROOT / ".tmp").mkdir(exist_ok=True)
    sizes = [(10_000, 1_000), (50_000, 5_000)]
    if args.experiment:
        sizes = [(500_000, 50_000)]
    print(json.dumps(_measure(sizes), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
