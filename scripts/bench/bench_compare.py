"""20M-row benchmark of stage() + compare() on synthetic Parquet.

Exercises the production code paths end to end except the readstat SAS
decode (both sides use the Parquet staging path). Timings print per step.
"""

import shutil
import time
from pathlib import Path

import polars as pl

from sentinel_parity.io.duckdb_comparison import compare
from sentinel_parity.io.staging import stage

ROOT = Path(__file__).resolve().parent
WORK = ROOT / "bench-work"
ROWS = 20_000_000
T0 = time.perf_counter()


def log(message: str) -> None:
    print(f"[{time.perf_counter() - T0:8.1f}s] {message}", flush=True)


if WORK.exists():
    shutil.rmtree(WORK)
WORK.mkdir(parents=True)

log("generating synthetic parquet (20M rows, 5 columns)")
frame = pl.select(
    pl.int_range(0, ROWS, dtype=pl.Int64).alias("account_id"),
    ((pl.int_range(0, ROWS, dtype=pl.Int64) % 1_000_000) / 100.0).alias("amount"),
    (pl.int_range(0, ROWS, dtype=pl.Int64) % 500).cast(pl.Utf8).alias("category"),
    pl.when(pl.int_range(0, ROWS, dtype=pl.Int64) % 97 == 0)
    .then(None)
    .otherwise(pl.int_range(0, ROWS, dtype=pl.Int64) % 1000)
    .cast(pl.Int64)
    .alias("nullable_count"),
).with_columns(
    pl.from_epoch(pl.int_range(0, ROWS, dtype=pl.Int64) % 50_000, time_unit="d").alias("event_date")
)
frame.write_parquet(WORK / "side_a.parquet")
perturbed = frame.with_columns(
    (
        pl.col("amount")
        + pl.when(pl.int_range(0, ROWS, dtype=pl.Int64) % 1000 == 0).then(0.01).otherwise(0.0)
    ).alias("amount")
)
perturbed.write_parquet(WORK / "side_b.parquet")
log(
    f"generated: side_a={(WORK / 'side_a.parquet').stat().st_size / 1e6:.0f}MB "
    f"side_b={(WORK / 'side_b.parquet').stat().st_size / 1e6:.0f}MB"
)

log("stage(side_a) — raw sink + vectorized encode with scalar fallbacks")
started = time.perf_counter()
side_a = stage(WORK / "side_a.parquet", "python", WORK)
log(f"stage a: {time.perf_counter() - started:.1f}s rows={side_a['rows']}")

log("stage(side_b)")
started = time.perf_counter()
side_b = stage(WORK / "side_b.parquet", "python", WORK)
log(f"stage b: {time.perf_counter() - started:.1f}s rows={side_b['rows']}")

dataset_work = WORK / "compare-work"
dataset_work.mkdir()
log("compare(threads=8, memory_limit=2GB, max_temp=200GB)")
started = time.perf_counter()
result = compare(side_a, side_b, dataset_work, "2GB", "200GB", "bench-dataset", threads=8)
elapsed = time.perf_counter() - started
details = Path(result["details_path"])
size_mb = details.stat().st_size / 1e6
line_count = sum(1 for _ in details.open(encoding="utf-8", buffering=1 << 20))
log(
    f"compare: {elapsed:.1f}s status={result['status']} matched={result['matched']} "
    f"sas_only={result['sas_only']} python_only={result['python_only']}"
)
log(f"details: {size_mb:.0f}MB {line_count} lines")
log("DONE")
