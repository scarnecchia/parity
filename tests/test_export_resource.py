import json
import subprocess
import sys
from pathlib import Path

import duckdb
import polars as pl
import pytest

from sentinel_parity.io.duckdb_comparison import compare
from sentinel_parity.io.report_writer import publish
from sentinel_parity.io.staging import stage

HARNESS = Path(__file__).with_name("resource_harness.py")
# Generous fixed tolerance calibrated on this host; see DEVELOPMENT-RESULTS.md.
RSS_GROWTH_TOLERANCE_KIB = 768 * 1024
RSS_CEILING_KIB = 2 * 1024 * 1024


def _compare(
    tmp_path: Path, left: dict[str, list[object]], right: dict[str, list[object]], **kwargs: object
) -> dict:
    for name in ("sas", "python", "compare"):
        (tmp_path / name).mkdir(parents=True)
    left_path, right_path = tmp_path / "left.parquet", tmp_path / "right.parquet"
    pl.DataFrame(left).write_parquet(left_path)
    pl.DataFrame(right).write_parquet(right_path)
    return compare(
        stage(left_path, "python", tmp_path / "sas"),
        stage(right_path, "python", tmp_path / "python"),
        tmp_path / "compare",
        "128MB",
        "1GB",
        "resource",
        **kwargs,
    )


def test_export_does_not_read_jsonl(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    result = _compare(tmp_path, {"v": ["a"]}, {"v": ["b"]}, preview_rows=2)
    original_read_text = Path.read_text

    def reject_read_text(path: Path, *args: object, **kwargs: object) -> str:
        if path.suffix == ".jsonl":
            raise AssertionError("details JSONL must not be read back")
        return original_read_text(path, *args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(Path, "read_text", reject_read_text)
    summary = {
        "status": "FAIL",
        "limits": {
            "preview_rows": 2,
            "preview_max_bytes": 1_000_000,
            "preview_total_max_bytes": 1_000_000,
        },
        "datasets": [
            {
                "id": "resource",
                "name": "resource",
                "status": "FAIL",
                "difference_row_count": 1,
                "conditions": [],
            }
        ],
    }
    previews = {"resource": result["preview_rows"]}
    publish(
        tmp_path / "report",
        summary,
        {"resource": Path(result["details_path"])},
        previews,
        {},
    )
    published = json.loads((tmp_path / "report" / "summary.json").read_text())
    assert published["preview_truncation"]["resource"]["shown_rows"] == 1
    assert (tmp_path / "report" / "details" / "resource.jsonl").is_file()


def test_export_fetches_only_bounded_batches(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    original_fetchall = duckdb.DuckDBPyConnection.fetchall
    seen: list[int] = []

    def counted(connection: duckdb.DuckDBPyConnection) -> list[tuple]:
        rows = original_fetchall(connection)
        seen.append(len(rows))
        return rows

    monkeypatch.setattr(duckdb.DuckDBPyConnection, "fetchall", counted)
    result = _compare(tmp_path, {"v": ["a"] * 12}, {"v": ["b"] * 12}, preview_rows=3)
    assert result["difference_row_count"] == 12
    assert seen and max(seen) <= 3


def test_preview_marks_missing_values_and_clipping_without_changing_details(tmp_path: Path) -> None:
    huge = "z" * 80
    for name in ("sas", "python", "compare"):
        (tmp_path / name).mkdir(parents=True)
    left_path, right_path = tmp_path / "left.parquet", tmp_path / "right.parquet"
    pl.DataFrame({"id": [1, 2, 3, 4]}).with_columns(
        pl.Series("value", [float("nan"), None, 1.0, 2.0], dtype=pl.Float64)
    ).write_parquet(left_path)
    pl.DataFrame({"id": [1, 2, 3, 4]}).with_columns(
        pl.Series("value", ["right", "right", huge, ""], dtype=pl.String)
    ).write_parquet(right_path)
    result = compare(
        stage(left_path, "python", tmp_path / "sas"),
        stage(right_path, "python", tmp_path / "python"),
        tmp_path / "compare",
        "128MB",
        "1GB",
        "resource",
        preview_rows=4,
        preview_cell_chars=12,
    )
    rows = result["preview_rows"]
    assert any(row["sas"] == "nan (compares as missing)" for row in rows)
    assert any(row["python"] == " (compares as missing)" for row in rows)
    assert any(row["python"].endswith("…") for row in rows)
    records = [json.loads(line) for line in Path(result["details_path"]).read_text().splitlines()]
    assert any(
        difference["python"]["value"] == huge
        for record in records
        for difference in record["differences"]
    )


def test_huge_cell_is_clipped_sql_side(tmp_path: Path) -> None:
    huge = "x" * 200_000
    stage_path = tmp_path / "stage.parquet"
    result = _compare(
        tmp_path,
        {"v": ["a"]},
        {"v": [huge]},
        preview_rows=5,
        preview_cell_chars=32,
        workbook_stage_path=stage_path,
        workbook_remaining={"rows": 10, "rows_per_dataset": 10, "bytes": 10**9, "sheets": 5},
    )
    preview = result["preview_rows"]
    assert len(preview) == 1
    assert len(preview[0]["python"]) <= 32
    assert result["details_bytes"] > 200_000
    assert result["workbook_stage_reasons"] == ["cell_limit"]
    assert result["workbook_stage_path"] is None
    assert not stage_path.exists()


def _flatten(record: dict) -> list[dict]:
    return [
        {
            "pair_id": record["pair_id"],
            "kind": record["kind"],
            "column": item["column"],
            "sas": item["sas"],
            "python": item["python"],
            "sas_row": record["sas_row"],
            "python_row": record["python_row"],
        }
        for item in record["differences"]
    ]


def _canonical(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _measure_workload(tmp_path: Path, name: str, left: dict, right: dict) -> dict:
    result = _compare(tmp_path / name, left, right, preview_rows=0)
    raw = Path(result["details_path"]).read_text()
    records = [json.loads(line) for line in raw.splitlines()]
    nested = [_flatten(record) for record in records]
    flat_entries = [entry for group in nested for entry in group]
    flat_bytes = sum(len(_canonical(entry)) + 1 for entry in flat_entries)
    legacy_bytes = 0
    for side, data in (("sas", left), ("python", right)):
        for ordinal, (row_id, value) in enumerate(zip(data["id"], data["v"], strict=True)):
            legacy = {
                "dataset_id": name,
                "side": side,
                "ordinal": ordinal,
                "values": {"id": str(row_id), "v": value},
            }
            legacy_bytes += len(_canonical(legacy)) + 1
    return {
        "rows_left": len(left["id"]),
        "rows_right": len(right["id"]),
        "detail_records": len(records),
        "difference_entries": result["difference_row_count"],
        "nested_bytes": len(raw.encode()),
        "flat_bytes": flat_bytes,
        "legacy_bytes": legacy_bytes,
        "nested": nested,
        "flat_entries": flat_entries,
        "records": records,
    }


def test_detail_size_sparse_dense_one_sided(tmp_path: Path) -> None:
    sparse_right = {"id": list(range(10_000)), "v": ["a"] * 10_000}
    for index in range(10):
        sparse_right["v"][index * 1_000] = "b"
    workloads = {
        "sparse": ({"id": list(range(10_000)), "v": ["a"] * 10_000}, sparse_right, 10, 10),
        "dense": (
            {"id": list(range(10_000)), "v": ["a"] * 10_000},
            {"id": list(range(10_000)), "v": ["c"] * 10_000},
            10_000,
            10_000,
        ),
        "one_sided": (
            {"id": list(range(5_000)), "v": ["a"] * 5_000},
            {"id": list(range(4_000)), "v": ["a"] * 4_000},
            1_000,
            2_000,
        ),
    }
    measurements = {}
    for name, (left, right, expected_pairs, expected_entries) in workloads.items():
        measured = _measure_workload(tmp_path, name, left, right)
        assert measured["detail_records"] == expected_pairs
        assert measured["difference_entries"] == expected_entries
        nested_set = {_canonical(entry) for group in measured["nested"] for entry in group}
        flat_set = {_canonical(entry) for entry in measured["flat_entries"]}
        assert nested_set == flat_set
        if name == "one_sided":
            # Excess-tail rows pair by coalesced rank; only no-shared-column
            # datasets use pair_id null.
            assert {record["kind"] for record in measured["records"]} == {"only_sas"}
            assert all(record["pair_id"] is not None for record in measured["records"])
        measurements[name] = {
            key: value
            for key, value in measured.items()
            if key not in ("nested", "flat_entries", "records")
        }
    # Byte ratios are recorded in DEVELOPMENT-RESULTS.md; nested is not always smallest.
    print(json.dumps(measurements, indent=2, sort_keys=True))


@pytest.mark.slow
def test_export_resource_scaling(tmp_path: Path) -> None:
    def measure(rows: int, mismatches: int) -> dict:
        completed = subprocess.run(
            [
                sys.executable,
                str(HARNESS),
                "--worker",
                "--rows",
                str(rows),
                "--mismatches",
                str(mismatches),
            ],
            cwd=tmp_path,
            check=True,
            capture_output=True,
            text=True,
        )
        return json.loads(completed.stdout)

    small = measure(10_000, 1_000)
    large = measure(50_000, 25_000)
    assert small["mismatch_rows"] == 1_000
    assert large["mismatch_rows"] == 25_000
    assert 0 < small["detail_bytes"] < large["detail_bytes"]
    assert large["peak_rss_kib"] < RSS_CEILING_KIB
    assert large["peak_rss_kib"] - small["peak_rss_kib"] < RSS_GROWTH_TOLERANCE_KIB
