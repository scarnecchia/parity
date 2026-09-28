import json
import os
import zipfile
from pathlib import Path

import polars as pl
import pytest
from jinja2 import Environment, FileSystemLoader, select_autoescape

from sentinel_parity.config import RunConfig
from sentinel_parity.core.export_policy import (
    ExcelLimits,
    WorkbookMeasurements,
    excel_omissions,
)
from sentinel_parity.io.duckdb_comparison import _WHITESPACE, compare
from sentinel_parity.io.report_writer import _atomic_copy, publish
from sentinel_parity.io.staging import stage
from sentinel_parity.io.workbook_writer import safe_sheet_names, write_workbook


def _compare(
    tmp_path: Path,
    left: dict[str, list[object]],
    right: dict[str, list[object]],
    *,
    workbook_stage_path: Path | None = None,
    workbook_remaining: dict[str, int] | None = None,
    pair_keys: tuple[str, ...] = (),
) -> dict:
    for name in ("sas", "python", "compare"):
        (tmp_path / name).mkdir(parents=True)
    left_path, right_path = tmp_path / "left.parquet", tmp_path / "right.parquet"
    pl.DataFrame(left).write_parquet(left_path)
    pl.DataFrame(right).write_parquet(right_path)
    left_staged = stage(left_path, "python", tmp_path / "sas")
    right_staged = stage(right_path, "python", tmp_path / "python")
    return compare(
        left_staged,
        right_staged,
        tmp_path / "compare",
        "128MB",
        "1GB",
        "fixture",
        preview_rows=1,
        workbook_stage_path=workbook_stage_path,
        workbook_remaining=workbook_remaining,
        pair_keys=pair_keys,
    )


def test_v2_pair_contract_exports_only_changed_columns(tmp_path: Path) -> None:
    result = _compare(
        tmp_path, {"a": [1, 2], "b": ["same", "old"]}, {"a": [1, 3], "b": ["same", "new"]}
    )
    records = [json.loads(line) for line in Path(result["details_path"]).read_text().splitlines()]
    assert len(records) == 1
    assert records[0]["schema_version"] == 2
    assert records[0]["kind"] == "paired_mismatch"
    assert {item["column"] for item in records[0]["differences"]} == {"a", "b"}
    assert result["difference_row_count"] == 2
    assert result["differing_pair_count"] == 1
    assert result["differing_column_counts"] == {"a": 1, "b": 1}
    assert len(result["preview_rows"]) == 1


def test_value_ordered_excess_pairs_align_shuffled_rows(tmp_path: Path) -> None:
    result = _compare(
        tmp_path,
        {"sort_key": [0, 1, 2, 3], "value": ["same", "left-a", "left-b", "left-c"]},
        {"sort_key": [3, 0, 2, 1], "value": ["right-c", "same", "right-b", "right-a"]},
    )
    records = [json.loads(line) for line in Path(result["details_path"]).read_text().splitlines()]
    paired = [record for record in records if record["kind"] == "paired_mismatch"]
    assert len(paired) == 3
    assert all([item["column"] for item in record["differences"]] == ["value"] for record in paired)
    assert {(record["sas_row"], record["python_row"]) for record in paired} == {
        (1, 3),
        (2, 2),
        (3, 0),
    }
    assert result["matched"] == 1
    assert result["order_mismatches"] == 1
    assert result["differing_pair_count"] == 3
    assert result["differing_column_counts"] == {"value": 3}


def test_duplicate_count_excess_remains_one_sided(tmp_path: Path) -> None:
    result = _compare(tmp_path, {"value": ["a", "b", "c", "c"]}, {"value": ["a", "b", "c"]})
    records = [json.loads(line) for line in Path(result["details_path"]).read_text().splitlines()]
    assert len(records) == 1
    assert records[0]["kind"] == "only_sas"
    assert records[0]["differences"][0]["sas"]["value"] == "c"
    assert records[0]["pair_id"] == 1
    assert records[0]["sas_row"] == 3
    assert records[0]["python_row"] is None
    assert result["sas_only"] == 1
    assert result["python_only"] == 0
    assert result["differing_pair_count"] == 0
    assert result["difference_row_count"] == 1


def test_one_sided_rows_do_not_count_as_differing_columns(tmp_path: Path) -> None:
    result = _compare(tmp_path, {"id": [1, 2], "v": ["a", "b"]}, {"id": [1], "v": ["a"]})
    records = [json.loads(line) for line in Path(result["details_path"]).read_text().splitlines()]
    assert len(records) == 1
    assert records[0]["kind"] == "only_sas"
    assert records[0]["sas_row"] == 1
    assert records[0]["python_row"] is None
    assert result["sas_only"] == 1
    assert result["differing_pair_count"] == 0
    assert result["differing_column_counts"] == {}


def test_no_shared_columns_keep_absent_row_distinct_from_null_cell(tmp_path: Path) -> None:
    result = _compare(tmp_path, {"sas_value": [None]}, {"py_value": ["x"]})
    records = [json.loads(line) for line in Path(result["details_path"]).read_text().splitlines()]
    assert {row["kind"] for row in records} == {"only_sas", "only_python"}
    assert all(row["pair_id"] is None for row in records)
    sas = next(row for row in records if row["kind"] == "only_sas")
    assert sas["sas_row"] == 0 and sas["python_row"] is None
    assert sas["differences"][0]["sas"] is None
    assert sas["differences"][0]["python"] is None


def test_paired_mismatch_preserves_json_null_cell(tmp_path: Path) -> None:
    result = _compare(tmp_path, {"id": [1], "value": [None]}, {"id": [1], "value": ["text"]})
    record = json.loads(Path(result["details_path"]).read_text().splitlines()[0])
    assert record["kind"] == "paired_mismatch"
    assert record["sas_row"] is not None and record["python_row"] is not None
    difference = next(item for item in record["differences"] if item["column"] == "value")
    assert difference["sas"] is None
    assert difference["python"]["value"] == "text"


def test_workbook_text_marks_missing_values_not_absent_rows(tmp_path: Path) -> None:
    stage_path = tmp_path / "workbook.parquet"
    _compare(
        tmp_path,
        {"id": [1, 2, 3, 4, 5], "value": [None, "  ", "\t\n", "\x1c", "\xa0"]},
        {"id": [1, 2, 3, 4, 5], "value": ["text", "full", "y", "z", "w"]},
        workbook_stage_path=stage_path,
        workbook_remaining={"rows": 10, "rows_per_dataset": 10, "bytes": 10_000, "sheets": 5},
    )
    staged = pl.read_parquet(stage_path).sort("pair_id")
    assert staged["sas"].to_list() == [
        "(missing)",
        "   (compares as missing)",
        "\t\n (compares as missing)",
        "\x1c (compares as missing)",
        "\xa0 (compares as missing)",
    ]


def test_preview_marks_whitespace_only_text_as_missing(tmp_path: Path) -> None:
    result = _compare(tmp_path, {"v": ["\t\n"]}, {"v": ["y"]})
    preview = result["preview_rows"][0]
    assert preview["sas"] == "\t\n (compares as missing)"


def test_preview_marks_nbsp_only_text_as_missing(tmp_path: Path) -> None:
    result = _compare(tmp_path, {"v": ["\xa0"]}, {"v": ["x"]})
    assert result["status"] == "FAIL"
    preview = result["preview_rows"][0]
    assert preview["sas"] == "\xa0 (compares as missing)"


def test_missing_whitespace_set_matches_python_rstrip() -> None:
    codes = sorted(int(item[4:-1]) for item in _WHITESPACE.split("||"))
    assert codes == sorted(cp for cp in range(0x110000) if chr(cp).rstrip() == "")


def test_pair_keys_order_diagnostic_pairing(tmp_path: Path) -> None:
    # Staging may reorder columns alphabetically, so the automatic order and
    # the declared-key order must differ by name, not position: "a" sorts
    # before "z", and declaring ("z",) flips the pairing priority.
    left = {"a": [1, 2], "z": [2000, 1990]}
    right = {"a": [1, 2], "z": [1990, 2000]}
    automatic = _compare(tmp_path / "auto", left, right)
    keyed = _compare(tmp_path / "keyed", left, right, pair_keys=("z",))

    def paired_columns(result: dict) -> list[set[str]]:
        records = [
            json.loads(line)
            for line in Path(result["details_path"]).read_text().splitlines()
            if json.loads(line)["kind"] == "paired_mismatch"
        ]
        return [{item["column"] for item in record["differences"]} for record in records]

    # Automatic ordering ranks by a first: excess rows pair on equal a and
    # differ only in z.
    assert all(columns == {"z"} for columns in paired_columns(automatic))
    # Declared keys rank by z first: the same excess rows now pair on equal z
    # and differ only in a.
    assert keyed["pair_keys"] == ["z"]
    assert all(columns == {"a"} for columns in paired_columns(keyed))


def test_pair_keys_dropped_when_missing_from_shared_columns(tmp_path: Path) -> None:
    result = _compare(
        tmp_path,
        {"g": [1, 2], "sas_extra": ["a", "b"]},
        {"g": [2, 1], "py_extra": ["x", "y"]},
        pair_keys=("g", "missing_col"),
    )
    assert result["pair_keys"] == ["g"]


def test_report_explains_pairing_basis_flags_key_rows_and_reasons(tmp_path: Path) -> None:
    row = {
        "pair_id": 1,
        "column": "group",
        "sas": "west",
        "python": "east",
        "kind": "paired_mismatch",
        "sas_row": 0,
        "python_row": 0,
    }
    source = tmp_path / "details.jsonl"
    source.write_text("", encoding="utf-8")
    summary = {
        "status": "FAIL",
        "limits": {
            "preview_rows": 4,
            "preview_max_bytes": 1048576,
            "preview_total_max_bytes": 10485760,
        },
        "excel": {
            "status": "omitted",
            "path": None,
            "reasons": [
                {"code": "row_limit", "dataset_id": None},
                {"code": "sheet_row_limit", "dataset_id": "dataset-2"},
            ],
            "limits": {
                "max_sheets": 100,
                "max_rows": 100000,
                "max_rows_per_sheet": 25000,
                "max_bytes": 104857600,
            },
        },
        "datasets": [
            {
                "id": "dataset-2",
                "name": "dplocal/people",
                "status": "FAIL",
                "difference_row_count": 1,
                "conditions": [],
                "pair_keys": ["group"],
            }
        ],
        "preview_truncation": {},
    }
    publish(tmp_path, summary, {}, {"dataset-2": [row]}, {"dataset-2": {}})
    html = (tmp_path / "index.html").read_text()
    assert "declared pairing keys (group), then remaining shared columns" in html
    assert 'class="key-diff"' in html
    assert "the run exceeds --excel-max-rows (100000 flat difference rows)" in html
    assert (
        "a dataset exceeds --excel-max-rows-per-sheet (25000 flat difference rows) — dplocal/people"
        in html
    )
    assert "(dataset-2)" not in html

    summary["datasets"][0]["pair_keys"] = []
    publish(tmp_path / "auto", summary, {}, {"dataset-2": [row]}, {"dataset-2": {}})
    auto_html = (tmp_path / "auto/index.html").read_text()
    assert "ascending value order across all shared columns" in auto_html
    assert 'class="key-diff"' not in auto_html


def test_preview_marks_clipped_whitespace_only_text_as_missing(tmp_path: Path) -> None:
    result = _compare(tmp_path, {"v": [" " * 600]}, {"v": ["x"]})
    fragment = result["preview_rows"][0]["sas"]
    assert fragment.endswith(" (compares as missing)")
    assert "…" in fragment


def test_excess_ties_pair_deterministically_by_staging_order(tmp_path: Path) -> None:
    result = _compare(
        tmp_path,
        {"key": [1, 1, 1], "sas_extra": ["a", "b", "c"]},
        {"key": [1, 2], "py_extra": ["x", "y"]},
    )
    records = [json.loads(line) for line in Path(result["details_path"]).read_text().splitlines()]
    paired = [row for row in records if row["kind"] == "paired_mismatch"]
    one_sided = [row for row in records if row["kind"] == "only_sas"]
    assert len(paired) == 1 and len(one_sided) == 1
    assert paired[0]["sas_row"] == 1 and paired[0]["python_row"] == 1
    assert one_sided[0]["sas_row"] == 2


def test_type_only_crossed_differences_warn(tmp_path: Path) -> None:
    result = _compare(tmp_path, {"number": [1]}, {"number": ["2"]})
    assert result["status"] == "WARN"
    assert result["reason"] == "type_mismatch"


def test_workbook_policy() -> None:
    measurements = WorkbookMeasurements(
        rows=1, text_bytes=4, max_cell_chars=5, sheets=2, max_rows_per_sheet=1
    )
    assert excel_omissions(measurements, ExcelLimits(rows=0)) == ("row_limit",)
    assert excel_omissions(measurements, ExcelLimits(enabled=False, rows=0)) == ()
    limits = ExcelLimits(rows=100, rows_per_sheet=7)
    # The per-sheet budget bounds each dataset sheet, never the run total:
    # two datasets of 6 rows stay eligible although the total exceeds 7.
    assert (
        excel_omissions(
            WorkbookMeasurements(
                rows=12, text_bytes=4, max_cell_chars=5, sheets=3, max_rows_per_sheet=6
            ),
            limits,
        )
        == ()
    )
    assert excel_omissions(
        WorkbookMeasurements(
            rows=12, text_bytes=4, max_cell_chars=5, sheets=3, max_rows_per_sheet=8
        ),
        limits,
    ) == ("sheet_row_limit",)
    assert excel_omissions(
        WorkbookMeasurements(
            rows=1_048_575, text_bytes=4, max_cell_chars=5, sheets=2, max_rows_per_sheet=1_048_576
        ),
        ExcelLimits(rows=1_048_576, rows_per_sheet=2_000_000),
    ) == ("sheet_row_limit",)
    assert (
        excel_omissions(
            WorkbookMeasurements(
                rows=1_048_575,
                text_bytes=4,
                max_cell_chars=5,
                sheets=2,
                max_rows_per_sheet=1_048_575,
            ),
            ExcelLimits(rows=1_048_576, rows_per_sheet=1_048_575),
        )
        == ()
    )


def test_safe_sheet_names_strip_edge_apostrophes() -> None:
    names = safe_sheet_names(["'leading", "trailing'", "'", "ok'name"])
    assert names["'leading"] == "leading"
    assert names["trailing'"] == "trailing"
    assert names["'"] == "Dataset"
    assert names["ok'name"] == "ok'name"


def test_excel_preflight_boundaries_and_rejection(tmp_path: Path) -> None:
    values = ({"value": [1, 2]}, {"value": [1, 3]})
    # The probe stages with generous budgets so its measurements reflect real
    # workbook output; comparisons that cannot stage report zero measurements.
    probe = _compare(
        tmp_path / "probe",
        *values,
        workbook_stage_path=tmp_path / "probe" / "measure.parquet",
        workbook_remaining={
            "rows": 1_000_000,
            "rows_per_dataset": 1_000_000,
            "bytes": 1_000_000_000,
            "sheets": 99,
        },
    )
    measurements = probe["workbook_measurements"]
    accepted_path = tmp_path / "accepted.parquet"
    accepted = _compare(
        tmp_path / "accepted-run",
        *values,
        workbook_stage_path=accepted_path,
        workbook_remaining={
            "rows": measurements["rows"] + 1,
            "rows_per_dataset": measurements["rows"],
            "bytes": measurements["text_bytes"],
            "sheets": measurements["sheets"] - 1,
        },
    )
    assert accepted["workbook_stage_reasons"] == []
    assert accepted_path.is_file()
    staged = pl.read_parquet(accepted_path)
    assert staged["sas"].to_list() == ["2"]
    assert staged["python"].to_list() == ["3"]
    assert staged["sas_type"].to_list() == ["int"]
    assert measurements["cell_bytes"] == sum(
        len(str(value).encode("utf-8"))
        for value in (1, "value", "2", "3", "int", "int", 1, 1, "paired_mismatch")
    )

    rejected_path = tmp_path / "rejected.parquet"
    rejected = _compare(
        tmp_path / "rejected-run",
        *values,
        workbook_stage_path=rejected_path,
        workbook_remaining={
            "rows": measurements["rows"] + 1,
            "rows_per_dataset": measurements["rows"],
            "bytes": measurements["text_bytes"] - 1,
            "sheets": measurements["sheets"] - 1,
        },
    )
    assert "byte_limit" in rejected["workbook_stage_reasons"]
    assert rejected["workbook_stage_path"] is None
    assert not rejected_path.exists()


def test_excel_sheet_names_and_untrusted_text(tmp_path: Path) -> None:
    names = safe_sheet_names(["Index", "A/long/name", "a/long/name"])
    assert len({name.casefold() for name in names.values()}) == len(names)
    output = tmp_path / "book.xlsx"
    staged_path = tmp_path / "staged.parquet"
    pl.DataFrame(
        {
            "pair_id": [9007199254740993],
            "column": ["x"],
            "sas": ["=1+1"],
            "python": ["https://example.com"],
            "sas_type": ["string"],
            "python_type": ["string"],
            "sas_row": [9007199254740993],
            "python_row": [None],
            "kind": ["paired_mismatch"],
        }
    ).write_parquet(staged_path)
    write_workbook(
        output,
        [{"id": "d", "name": "dataset", "status": "FAIL", "conditions": []}],
        {"d": staged_path},
        tmp_path,
    )
    with zipfile.ZipFile(output) as archive:
        xml = archive.read("xl/worksheets/sheet2.xml").decode()
    assert "<f>" not in xml and "<hyperlinks" not in xml
    assert 't="inlineStr"' in xml
    assert "9007199254740993" in xml


def test_config_export_limits_must_be_positive(tmp_path: Path) -> None:
    with pytest.raises(ValueError):
        RunConfig(tmp_path, tmp_path, preview_max_bytes=0)


def test_report_flags_condition_only_dataset(tmp_path: Path) -> None:
    summary = {
        "status": "FAIL",
        "limits": {},
        "datasets": [
            {
                "id": "cond",
                "name": "cond/only",
                "status": "FAIL",
                "reason": "sas_only_columns",
                "conditions": [
                    {"severity": "FAIL", "reason": "sas_only_columns", "columns": ["extra"]}
                ],
                "difference_row_count": 0,
            }
        ],
        "preview_truncation": {},
    }
    publish(
        tmp_path,
        summary,
        {},
        {"cond": []},
        {"cond": {"shown_rows": 0, "omitted_rows": 0, "rendered_bytes": 0, "reasons": []}},
    )
    html = (tmp_path / "index.html").read_text()
    assert "no value-difference rows" in html


def test_preview_byte_boundaries_measure_the_rendered_fragment(tmp_path: Path) -> None:
    row = {
        "pair_id": 1,
        "column": "x<&",
        "sas": "café",
        "python": "other",
        "kind": "paired_mismatch",
        "sas_row": 2,
        "python_row": 3,
    }
    resource_dir = Path(__file__).parents[1] / "src/sentinel_parity/resources"
    env = Environment(loader=FileSystemLoader(resource_dir), autoescape=select_autoescape(["html"]))
    fragment = env.get_template("preview_row.html").render(row=row)
    exact_bytes = len(fragment.encode("utf-8"))
    source = tmp_path / "details.jsonl"
    source.write_text("", encoding="utf-8")
    summary = {
        "status": "FAIL",
        "limits": {
            "preview_rows": 4,
            "preview_max_bytes": exact_bytes,
            "preview_total_max_bytes": exact_bytes,
        },
        "datasets": [
            {
                "id": "d1",
                "name": "first",
                "status": "FAIL",
                "difference_row_count": 1,
                "conditions": [],
            },
            {
                "id": "d2",
                "name": "second",
                "status": "FAIL",
                "difference_row_count": 1,
                "conditions": [],
            },
        ],
    }
    previews = {"d1": [row], "d2": [row]}
    truncation = {"d1": {}, "d2": {}}
    publish(tmp_path / "report", summary, {}, previews, truncation)
    output = json.loads((tmp_path / "report/summary.json").read_text())
    assert output["preview_truncation"]["d1"] == {
        "shown_rows": 1,
        "omitted_rows": 0,
        "rendered_bytes": exact_bytes,
        "reasons": [],
    }
    assert output["preview_truncation"]["d2"] == {
        "shown_rows": 0,
        "omitted_rows": 1,
        "rendered_bytes": 0,
        "reasons": ["run_byte_limit"],
    }
    assert output["datasets"][0]["preview_truncation"]["rendered_bytes"] == exact_bytes
    html = (tmp_path / "report/index.html").read_text()
    assert fragment.strip() in html


def test_preview_reasons_record_every_exceeded_budget(tmp_path: Path) -> None:
    row = {
        "pair_id": 1,
        "column": "x",
        "sas": "left",
        "python": "right",
        "kind": "paired_mismatch",
        "sas_row": 0,
        "python_row": 0,
    }
    resource_dir = Path(__file__).parents[1] / "src/sentinel_parity/resources"
    env = Environment(loader=FileSystemLoader(resource_dir), autoescape=select_autoescape(["html"]))
    exact = len(env.get_template("preview_row.html").render(row=row).encode("utf-8"))
    source = tmp_path / "details.jsonl"
    source.write_text("", encoding="utf-8")
    summary = {
        "status": "FAIL",
        "limits": {
            "preview_rows": 4,
            "preview_max_bytes": exact - 1,
            "preview_total_max_bytes": exact - 1,
        },
        "datasets": [
            {
                "id": "d1",
                "name": "first",
                "status": "FAIL",
                "difference_row_count": 1,
                "conditions": [],
            }
        ],
    }
    publish(tmp_path, summary, {}, {"d1": [row]}, {"d1": {}})
    output = json.loads((tmp_path / "summary.json").read_text())
    assert output["preview_truncation"]["d1"]["reasons"] == [
        "dataset_byte_limit",
        "run_byte_limit",
    ]


def test_atomic_copy_fsyncs_before_publish(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    events: list[str] = []
    real_fsync = os.fsync
    real_replace = os.replace

    def track_fsync(fd: int) -> None:
        events.append("fsync")
        real_fsync(fd)

    def track_replace(src: object, dst: object) -> None:
        events.append("replace")
        real_replace(src, dst)

    source = tmp_path / "source.jsonl"
    source.write_text("{}\n", encoding="utf-8")
    target = tmp_path / "details" / "copy.jsonl"
    target.parent.mkdir()
    monkeypatch.setattr(os, "fsync", track_fsync)
    monkeypatch.setattr(os, "replace", track_replace)
    _atomic_copy(source, target)
    assert target.read_text() == "{}\n"
    assert events == ["fsync", "replace"]


def test_summary_preview_truncation_accounts_for_every_difference_row(tmp_path: Path) -> None:
    rows = [
        {
            "pair_id": index,
            "column": "value",
            "sas": "left",
            "python": "right",
            "sas_type": "string",
            "python_type": "string",
            "kind": "paired_mismatch",
            "sas_row": index,
            "python_row": index,
        }
        for index in range(3)
    ]
    summary = {
        "status": "FAIL",
        "limits": {"preview_rows": 1, "preview_max_bytes": 10000, "preview_total_max_bytes": 10000},
        "datasets": [
            {"id": "d", "name": "d", "status": "FAIL", "difference_row_count": 3, "conditions": []}
        ],
    }
    publish(tmp_path / "report", summary, {}, {"d": rows[:1]}, {"d": {}})
    dataset = json.loads((tmp_path / "report/summary.json").read_text())["datasets"][0]
    truncation = dataset["preview_truncation"]
    assert truncation["shown_rows"] + truncation["omitted_rows"] == dataset["difference_row_count"]
    assert truncation["shown_rows"] == 1
    assert truncation["reasons"] == ["row_limit"]


def test_preview_zero_and_byte_limit_reasons(tmp_path: Path) -> None:
    row = {"pair_id": 1, "column": "x", "sas": "a", "python": "b", "kind": "paired_mismatch"}
    source = tmp_path / "details.jsonl"
    source.write_text("", encoding="utf-8")
    summary = {
        "status": "FAIL",
        "limits": {"preview_rows": 0, "preview_max_bytes": 100, "preview_total_max_bytes": 100},
        "datasets": [
            {"id": "d", "name": "d", "status": "FAIL", "difference_row_count": 2, "conditions": []}
        ],
    }
    previews = {"d": [row]}
    truncation = {"d": {}}
    publish(tmp_path / "report", summary, {}, previews, truncation)
    output = json.loads((tmp_path / "report/summary.json").read_text())
    assert output["preview_truncation"]["d"]["shown_rows"] == 0
    assert output["preview_truncation"]["d"]["omitted_rows"] == 2
    assert output["preview_truncation"]["d"]["reasons"] == ["row_limit"]
