"""Rendered copy tests for the HTML report, driven through the real publish function.

Synthetic summaries exercise report copy without downloaded SAS fixtures. Each
scenario renders into a temporary directory and asserts on visible text of the
relevant section, extracted with the standard-library HTML parser.
"""

from __future__ import annotations

from html.parser import HTMLParser
from typing import TYPE_CHECKING, Any

import pytest

from sentinel_parity.io.report_writer import publish

if TYPE_CHECKING:
    from pathlib import Path


class _SectionText(HTMLParser):
    """Collects visible text per report section, page-level text, and link targets."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.page: list[str] = []
        self.sections: dict[str, list[str]] = {}
        self.hrefs: list[str] = []
        self._section: str | None = None
        self._hidden = 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        names = dict(attrs)
        if tag in {"style", "script"}:
            self._hidden += 1
        elif tag == "section":
            self._section = names.get("id") or ""
            self.sections.setdefault(self._section, [])
        elif tag == "a" and names.get("href"):
            self.hrefs.append(names["href"])

    def handle_endtag(self, tag: str) -> None:
        if tag in {"style", "script"}:
            self._hidden = max(0, self._hidden - 1)
        elif tag == "section":
            self._section = None

    def handle_data(self, data: str) -> None:
        if self._hidden:
            return
        target = self.sections[self._section] if self._section is not None else self.page
        target.append(data)


def _visible(html: str) -> tuple[str, dict[str, str], list[str]]:
    """Return normalized page text, per-section text, and anchor hrefs."""
    parser = _SectionText()
    parser.feed(html)
    page = "".join(parser.page).split()
    sections = {key: " ".join("".join(parts).split()) for key, parts in parser.sections.items()}
    return " ".join(page), sections, parser.hrefs


def _dataset(dataset_id: str, name: str, **overrides: Any) -> dict[str, Any]:
    dataset: dict[str, Any] = {
        "id": dataset_id,
        "name": name,
        "status": "FAIL",
        "reason": "value_mismatch",
        "conditions": [],
        "sas_rows": 2,
        "python_rows": 2,
        "matched_pairs": 1,
        "sas_only": 1,
        "python_only": 1,
        "row_order_mismatches": 0,
        "type_mismatched_columns": [],
        "pair_keys": [],
        "differing_pair_count": 1,
        "differing_column_counts": {"country": 1},
        "difference_row_count": 1,
        "detail_complete": True,
    }
    dataset.update(overrides)
    if dataset["difference_row_count"] is None:
        # Real summaries omit this count for datasets that were not compared.
        del dataset["difference_row_count"]
    return dataset


def _row(
    pair_id: int | None,
    column: str,
    kind: str,
    *,
    sas: str = "3",
    python: str = "4",
    sas_row: int | None = 0,
    python_row: int | None = 0,
) -> dict[str, Any]:
    return {
        "pair_id": pair_id,
        "column": column,
        "sas": sas,
        "sas_marker": False,
        "python": python,
        "python_marker": False,
        "sas_type": "double",
        "python_type": "double",
        "sas_row": sas_row,
        "python_row": python_row,
        "kind": kind,
    }


def _summary(
    datasets: list[dict[str, Any]], *, excel: dict[str, Any] | None = None
) -> dict[str, Any]:
    statuses = [dataset["status"] for dataset in datasets]
    status = "PASS"
    for candidate in ("ERROR", "FAIL", "WARN"):
        if candidate in statuses:
            status = candidate
            break
    return {
        "schema_version": 3,
        "details_schema_version": 2,
        "status": status,
        "limits": {
            "preview_rows": 100,
            "preview_max_bytes": 1_048_576,
            "preview_total_max_bytes": 10_485_760,
            "round_digits": None,
        },
        "excel": excel,
        "datasets": datasets,
    }


def _publish_html(
    tmp_path: Path,
    summary: dict[str, Any],
    previews: dict[str, list[dict[str, Any]]],
    *,
    details_ids: tuple[str, ...] = (),
) -> str:
    sources: dict[str, Path] = {}
    for dataset_id in details_ids:
        source = tmp_path / f"{dataset_id}.source.jsonl"
        source.parent.mkdir(parents=True, exist_ok=True)
        source.write_text("", encoding="utf-8")
        sources[dataset_id] = source
    output = tmp_path / "report"
    publish(output, summary, sources, previews, {})
    return (output / "index.html").read_text(encoding="utf-8")


_CONDITION_CASES = [
    pytest.param(
        "error",
        {
            "status": "ERROR",
            "reason": "error",
            "error_message": "staging failed",
            "difference_row_count": None,
        },
        [],
        ["Comparison failed: staging failed."],
        ["did not run"],
        id="error",
    ),
    pytest.param(
        "missing-sas-counterpart",
        {
            "reason": "missing_counterpart",
            "conditions": [{"severity": "FAIL", "reason": "missing_counterpart"}],
            "sas_rows": None,
            "python_rows": 0,
            "matched_pairs": None,
            "sas_only": None,
            "python_only": 0,
            "difference_row_count": None,
        },
        [],
        ["No Parquet counterpart. File not read. Row count unavailable."],
        ["can be compared"],
        id="missing-sas",
    ),
    pytest.param(
        "missing-parquet-counterpart",
        {
            "reason": "missing_counterpart",
            "conditions": [{"severity": "FAIL", "reason": "missing_counterpart"}],
            "sas_rows": 0,
            "python_rows": None,
            "matched_pairs": None,
            "sas_only": 0,
            "python_only": None,
            "difference_row_count": None,
        },
        [],
        ["No SAS counterpart. File not read. Row count unavailable."],
        ["can be compared"],
        id="missing-parquet",
    ),
    pytest.param(
        "sas-only-column-singular",
        {
            "conditions": [
                {"severity": "FAIL", "reason": "sas_only_columns", "columns": ["country"]}
            ],
            "difference_row_count": 0,
            "sas_only": 0,
            "python_only": 0,
            "matched_pairs": 2,
        },
        [],
        ["Parquet lacks 1 SAS column: country."],
        ["no row can fully match"],
        id="sas-only-singular",
    ),
    pytest.param(
        "sas-only-columns-plural",
        {
            "conditions": [
                {"severity": "FAIL", "reason": "sas_only_columns", "columns": ["country", "region"]}
            ],
            "difference_row_count": 0,
            "sas_only": 0,
            "python_only": 0,
            "matched_pairs": 2,
        },
        [],
        ["Parquet lacks 2 SAS columns: country, region."],
        ["no row can fully match"],
        id="sas-only-plural",
    ),
    pytest.param(
        "parquet-only-column-singular",
        {
            "status": "WARN",
            "reason": "python_only_columns",
            "conditions": [
                {"severity": "WARN", "reason": "python_only_columns", "columns": ["extra"]}
            ],
            "difference_row_count": 0,
            "sas_only": 0,
            "python_only": 0,
            "matched_pairs": 2,
        },
        [],
        ["Parquet has 1 extra column: extra. These values are not compared."],
        ["SAS lacks"],
        id="parquet-only-singular",
    ),
    pytest.param(
        "parquet-only-columns-plural",
        {
            "status": "WARN",
            "reason": "python_only_columns",
            "conditions": [
                {"severity": "WARN", "reason": "python_only_columns", "columns": ["extra", "more"]}
            ],
            "difference_row_count": 0,
            "sas_only": 0,
            "python_only": 0,
            "matched_pairs": 2,
        },
        [],
        ["Parquet has 2 extra columns: extra, more. These values are not compared."],
        ["SAS lacks"],
        id="parquet-only-plural",
    ),
    pytest.param(
        "type-mismatch",
        {
            "status": "WARN",
            "reason": "type_mismatch",
            "conditions": [{"severity": "WARN", "reason": "type_mismatch"}],
            "type_mismatched_columns": ["year"],
            "difference_row_count": 0,
            "sas_only": 0,
            "python_only": 0,
            "matched_pairs": 2,
        },
        [],
        [
            "Value differences occur only in character vs numeric columns.",
            "Character vs numeric columns: year",
        ],
        ["passes"],
        id="type-mismatch",
    ),
    pytest.param(
        "value-mismatch-singular",
        {
            "conditions": [{"severity": "FAIL", "reason": "value_mismatch"}],
            "sas_only": 2,
            "python_only": 2,
        },
        [_row(1, "country", "paired_mismatch")],
        [
            "1 row pair does not match. Differing columns: country (1 row).",
            "1 row appears only in SAS.",
            "1 row appears only in Parquet.",
        ],
        [],
        id="value-mismatch-singular",
    ),
    pytest.param(
        "value-mismatch-plural",
        {
            "conditions": [{"severity": "FAIL", "reason": "value_mismatch"}],
            "differing_pair_count": 2,
            "differing_column_counts": {"country": 2},
            "sas_only": 5,
            "python_only": 0,
            "matched_pairs": 1,
            "python_rows": 1,
            "difference_row_count": 5,
        },
        [
            _row(1, "country", "paired_mismatch"),
            _row(2, "country", "paired_mismatch"),
        ],
        [
            "2 row pairs do not match. Differing columns: country (2 rows).",
            "3 rows appear only in SAS.",
        ],
        ["rows appear only in Parquet"],
        id="value-mismatch-plural",
    ),
]


@pytest.mark.parametrize(("name", "overrides", "rows", "expected", "forbidden"), _CONDITION_CASES)
def test_report_condition_copy(
    tmp_path: Path,
    name: str,
    overrides: dict[str, Any],
    rows: list[dict[str, Any]],
    expected: list[str],
    forbidden: list[str],
) -> None:
    dataset = _dataset("d1", "dplocal/data", **overrides)
    details_ids = ("d1",) if rows else ()
    html = _publish_html(tmp_path, _summary([dataset]), {"d1": rows}, details_ids=details_ids)
    _, sections, _ = _visible(html)
    text = sections["dataset-d1"]
    for fragment in expected:
        assert fragment in text, name
    for fragment in forbidden:
        assert fragment not in text, name


def test_report_compound_conditions_do_not_claim_pass(tmp_path: Path) -> None:
    dataset = _dataset(
        "d1",
        "dplocal/data",
        conditions=[
            {"severity": "FAIL", "reason": "sas_only_columns", "columns": ["gone"]},
            {"severity": "WARN", "reason": "type_mismatch"},
        ],
        type_mismatched_columns=["g"],
        difference_row_count=0,
        sas_only=0,
        python_only=0,
        matched_pairs=2,
    )
    html = _publish_html(tmp_path, _summary([dataset]), {})
    _, sections, _ = _visible(html)
    text = sections["dataset-d1"]
    assert "Parquet lacks 1 SAS column: gone." in text
    assert "Value differences occur only in character vs numeric columns." in text
    assert "FAIL" in text
    assert "passes" not in text


def test_report_pairing_copy(tmp_path: Path) -> None:
    keyed = _dataset(
        "d1",
        "dplocal/data",
        pair_keys=["group", "level"],
        unused_pair_keys=[
            {"column": "sas_missing", "where": "sas"},
            {"column": "parquet_missing", "where": "python"},
            {"column": "gone", "where": "absent"},
        ],
    )
    rows = [_row(1, "group", "paired_mismatch"), _row(2, "level", "paired_mismatch")]
    html = _publish_html(tmp_path, _summary([keyed]), {"d1": rows}, details_ids=("d1",))
    _, sections, _ = _visible(html)
    text = sections["dataset-d1"]
    assert "Pairing keys: group, level. Only equal keys pair." in text
    assert "Rows without a key match remain one-sided." in text
    assert (
        "Ignored pairing keys: sas_missing (SAS only), "
        "parquet_missing (Parquet only), gone (neither side)." in text
    )
    assert "Rows pair by ascending values" not in text

    auto = _dataset("d1", "dplocal/data")
    html = _publish_html(tmp_path / "auto", _summary([auto]), {"d1": rows}, details_ids=("d1",))
    _, sections, _ = _visible(html)
    text = sections["dataset-d1"]
    assert "Rows pair by ascending values across shared columns." in text
    assert "Pairs do not establish row correspondence." in text
    assert "Pairing keys:" not in text

    one_sided = _dataset("d1", "dplocal/data", sas_only=2, python_only=0, difference_row_count=2)
    one_sided_rows = [_row(None, "level", "only_sas", sas="7", python="", python_row=None)]
    html = _publish_html(
        tmp_path / "one-sided", _summary([one_sided]), {"d1": one_sided_rows}, details_ids=("d1",)
    )
    _, sections, _ = _visible(html)
    text = sections["dataset-d1"]
    assert "only_sas" in text
    assert "Rows pair by ascending values across shared columns." in text


_PREVIEW_CASES = [
    pytest.param(
        "no-omission",
        2,
        2,
        {},
        ["2 of 2 difference rows shown.", "… marks clipped values."],
        ["omitted"],
        id="no-omission",
    ),
    pytest.param(
        "row-limit-partial",
        1,
        3,
        {},
        ["1 of 3 difference rows shown. 2 omitted (row_limit)."],
        [],
        id="row-limit-partial",
    ),
    pytest.param(
        "all-omitted-preview-zero",
        0,
        2,
        {"preview_rows": 0},
        [
            "0 of 2 difference rows shown. 2 omitted (row_limit).",
            "… marks clipped values.",
            "JSONL contains full differences.",
        ],
        [],
        id="all-omitted-preview-zero",
    ),
    pytest.param(
        "dataset-byte-limit",
        1,
        1,
        {"preview_max_bytes": 10},
        ["0 of 1 difference rows shown. 1 omitted (dataset_byte_limit)."],
        [],
        id="dataset-byte-limit",
    ),
    pytest.param(
        "run-byte-limit",
        1,
        1,
        {"preview_total_max_bytes": 10},
        ["1 omitted (run_byte_limit)."],
        [],
        id="run-byte-limit",
    ),
    pytest.param(
        "both-byte-limits",
        1,
        1,
        {"preview_max_bytes": 10, "preview_total_max_bytes": 10},
        ["1 omitted (dataset_byte_limit, run_byte_limit)."],
        [],
        id="both-byte-limits",
    ),
]


@pytest.mark.parametrize(
    ("name", "row_count", "difference_row_count", "limit_overrides", "expected", "forbidden"),
    _PREVIEW_CASES,
)
def test_report_preview_limits_copy(
    tmp_path: Path,
    name: str,
    row_count: int,
    difference_row_count: int,
    limit_overrides: dict[str, int],
    expected: list[str],
    forbidden: list[str],
) -> None:
    dataset = _dataset("d1", "dplocal/data", difference_row_count=difference_row_count)
    summary = _summary([dataset])
    summary["limits"].update(limit_overrides)
    rows = [_row(index + 1, "country", "paired_mismatch") for index in range(row_count)]
    html = _publish_html(tmp_path, summary, {"d1": rows}, details_ids=("d1",))
    _, sections, hrefs = _visible(html)
    text = sections["dataset-d1"]
    for fragment in expected:
        assert fragment in text, name
    for fragment in forbidden:
        assert fragment not in text, name
    if row_count == 0:
        assert "<table" not in html, name
    assert "details/d1.jsonl" in hrefs, name


def test_report_copy_has_no_redundant_guidance(tmp_path: Path) -> None:
    bare = _dataset(
        "d1",
        "dplocal/data",
        conditions=[{"severity": "FAIL", "reason": "sas_only_columns", "columns": ["gone"]}],
        difference_row_count=0,
        sas_only=0,
        python_only=0,
        matched_pairs=2,
    )
    html = _publish_html(tmp_path, _summary([bare]), {})
    page_text, sections, _ = _visible(html)
    full_text = " ".join([page_text, *sections.values()])
    # Removed copy must stay gone everywhere, including inside dataset sections.
    for removed in (
        "This dataset has schema or counterpart conditions",
        "omitted by preview limits",
        "Cell values ending in",
        "Values are clipped for display",
        "absent from one side",
        "complete row-level result",
        "Previews are bounded",
    ):
        assert removed not in full_text

    first = _dataset("d1", "dplocal/data", difference_row_count=2, sas_only=0, python_only=0)
    second = _dataset("d2", "msoc/other", difference_row_count=2, sas_only=0, python_only=0)
    rows = [_row(1, "country", "paired_mismatch"), _row(2, "country", "paired_mismatch")]
    html = _publish_html(
        tmp_path / "previews",
        _summary([first, second]),
        {"d1": rows, "d2": rows},
        details_ids=("d1", "d2"),
    )
    page_text, sections, hrefs = _visible(html)
    full_text = " ".join([page_text, *sections.values()])
    # Old preview prose must not return now that the footer carries the legend.
    for removed in (
        "Values are clipped for display",
        "the JSONL link has the complete differences",
        "Ignored pairing keys, absent from one side",
    ):
        assert removed not in full_text
    assert full_text.count("… marks clipped values.") == 2
    assert full_text.count("JSONL contains full differences") == 2
    assert page_text.count("Reports may contain sensitive data.") == 1
    for dataset_id in ("d1", "d2"):
        assert sections[f"dataset-{dataset_id}"].count("marks clipped values") == 1
    assert hrefs.count("details/d1.jsonl") == 2


_WORKBOOK_CASES = [
    pytest.param(
        "generated",
        {"status": "generated", "path": "differences.xlsx", "reasons": [], "limits": {}},
        ["Excel workbook: generated"],
        [],
        id="generated",
    ),
    pytest.param(
        "disabled",
        {"status": "disabled", "path": None, "reasons": [], "limits": {}},
        ["Excel workbook: disabled"],
        [],
        id="disabled",
    ),
    pytest.param(
        "no-differences",
        {"status": "no_differences", "path": None, "reasons": [], "limits": {}},
        ["Excel workbook: no_differences"],
        [],
        id="no-differences",
    ),
    pytest.param(
        "row-limit-configured",
        {
            "status": "omitted",
            "path": None,
            "reasons": [{"code": "row_limit", "dataset_id": None}],
            "limits": {"max_rows": 5000},
        },
        ["Excel workbook: omitted. The run exceeds --excel-max-rows (5000 difference rows)."],
        [],
        id="row-limit-configured",
    ),
    pytest.param(
        "row-limit-hard",
        {
            "status": "omitted",
            "path": None,
            "reasons": [{"code": "row_limit", "dataset_id": None}],
            "limits": {"max_rows": 2000000},
        },
        [
            "Excel workbook: omitted. The run exceeds --excel-max-rows "
            "(Excel's hard limit of 1048575 difference rows)."
        ],
        [],
        id="row-limit-hard",
    ),
    pytest.param(
        "sheet-row-limit-attributed",
        {
            "status": "omitted",
            "path": None,
            "reasons": [{"code": "sheet_row_limit", "dataset_id": "d1"}],
            "limits": {"max_rows_per_sheet": 25000},
        },
        ["A dataset exceeds --excel-max-rows-per-sheet (25000 difference rows) — dplocal/data."],
        ["(dataset-1)"],
        id="sheet-row-limit-attributed",
    ),
    pytest.param(
        "byte-limit",
        {
            "status": "omitted",
            "path": None,
            "reasons": [{"code": "byte_limit", "dataset_id": None}],
            "limits": {"max_bytes": 1000},
        },
        ["The run exceeds --excel-max-bytes (1000 bytes of cell text)."],
        [],
        id="byte-limit",
    ),
    pytest.param(
        "sheet-limit-configured",
        {
            "status": "omitted",
            "path": None,
            "reasons": [{"code": "sheet_limit", "dataset_id": None}],
            "limits": {"max_sheets": 5},
        },
        [
            "More dataset sheets than --excel-max-sheets allows "
            "(5 sheets including the Index sheet)."
        ],
        [],
        id="sheet-limit-configured",
    ),
    pytest.param(
        "sheet-limit-hard",
        {
            "status": "omitted",
            "path": None,
            "reasons": [{"code": "sheet_limit", "dataset_id": None}],
            "limits": {"max_sheets": 200},
        },
        [
            "More dataset sheets than --excel-max-sheets allows "
            "(100 sheets including the Index sheet, Excel's hard limit)."
        ],
        [],
        id="sheet-limit-hard",
    ),
    pytest.param(
        "cell-limit",
        {
            "status": "omitted",
            "path": None,
            "reasons": [{"code": "cell_limit", "dataset_id": None}],
            "limits": {},
        },
        ["A value exceeds Excel's 32,767-character cell limit."],
        [],
        id="cell-limit",
    ),
    pytest.param(
        "index-row-limit",
        {
            "status": "omitted",
            "path": None,
            "reasons": [{"code": "index_row_limit", "dataset_id": None}],
            "limits": {},
        },
        ["Too many datasets for the workbook Index sheet."],
        [],
        id="index-row-limit",
    ),
    pytest.param(
        "unknown-code",
        {
            "status": "omitted",
            "path": None,
            "reasons": [{"code": "mystery_code", "dataset_id": "d1"}],
            "limits": {},
        },
        ["mystery_code (dplocal/data)."],
        [],
        id="unknown-code",
    ),
    pytest.param(
        "multiple-reasons",
        {
            "status": "omitted",
            "path": None,
            "reasons": [
                {"code": "row_limit", "dataset_id": None},
                {"code": "byte_limit", "dataset_id": None},
            ],
            "limits": {"max_rows": 5000, "max_bytes": 1000},
        },
        [
            "The run exceeds --excel-max-rows (5000 difference rows). "
            "The run exceeds --excel-max-bytes (1000 bytes of cell text)."
        ],
        [", and"],
        id="multiple-reasons",
    ),
]


@pytest.mark.parametrize(("name", "excel", "expected", "forbidden"), _WORKBOOK_CASES)
def test_report_workbook_copy(
    tmp_path: Path,
    name: str,
    excel: dict[str, Any],
    expected: list[str],
    forbidden: list[str],
) -> None:
    dataset = _dataset(
        "d1",
        "dplocal/data",
        status="PASS",
        reason="pass",
        matched_pairs=2,
        sas_only=0,
        python_only=0,
        difference_row_count=0,
    )
    html = _publish_html(tmp_path, _summary([dataset], excel=excel), {})
    page_text, _, hrefs = _visible(html)
    for fragment in expected:
        assert fragment in page_text, name
    for fragment in forbidden:
        assert fragment not in page_text, name
    if excel["path"]:
        assert excel["path"] in hrefs, name
    else:
        assert "download differences.xlsx" not in page_text, name


def test_report_missing_counterpart_copy(tmp_path: Path) -> None:
    sas_only = _dataset(
        "d1",
        "msoc/only.sas7bdat",
        reason="missing_counterpart",
        conditions=[{"severity": "FAIL", "reason": "missing_counterpart"}],
        sas_rows=None,
        python_rows=0,
        matched_pairs=None,
        sas_only=None,
        python_only=0,
        difference_row_count=None,
    )
    parquet_only = _dataset(
        "d2",
        "dplocal/junk.parquet",
        reason="missing_counterpart",
        conditions=[{"severity": "FAIL", "reason": "missing_counterpart"}],
        sas_rows=0,
        python_rows=None,
        matched_pairs=None,
        sas_only=0,
        python_only=None,
        difference_row_count=None,
    )
    html = _publish_html(tmp_path, _summary([sas_only, parquet_only]), {})
    page_text, sections, _ = _visible(html)
    assert "Files without a counterpart. These files were not read or compared." in page_text
    assert "SAS-only: msoc/only.sas7bdat" in page_text
    assert "Parquet-only: dplocal/junk.parquet" in page_text
    assert "No Parquet counterpart. File not read. Row count unavailable." in sections["dataset-d1"]
    assert "No SAS counterpart. File not read. Row count unavailable." in sections["dataset-d2"]
    assert "an equivalent" not in page_text


@pytest.mark.parametrize(
    ("count", "expected"),
    [
        (1, "Row order differs: 1 matched row has a different position."),
        (1440, "Row order differs: 1440 matched rows have different positions."),
    ],
)
def test_report_row_order_copy(tmp_path: Path, count: int, expected: str) -> None:
    dataset = _dataset(
        "d1",
        "dplocal/data",
        status="PASS",
        reason="pass",
        row_order_mismatches=count,
        matched_pairs=1440,
        sas_only=0,
        python_only=0,
        difference_row_count=0,
    )
    html = _publish_html(tmp_path, _summary([dataset]), {})
    _, sections, _ = _visible(html)
    assert expected in sections["dataset-d1"]
    assert "misplaced" not in sections["dataset-d1"]


def test_report_copy_escapes_dynamic_values(tmp_path: Path) -> None:
    marked = _dataset(
        "d1",
        '<b>ds & "co"</b>',
        pair_keys=["<em>k</em>"],
        unused_pair_keys=[{"column": "<u>ghost</u>", "where": "sas"}],
        conditions=[{"severity": "FAIL", "reason": "value_mismatch"}],
        differing_pair_count=1,
        differing_column_counts={"<i>col</i>": 1},
        difference_row_count=1,
    )
    summary = _summary(
        [marked],
        excel={
            "status": "omitted",
            "path": None,
            "reasons": [{"code": "sheet_row_limit", "dataset_id": "d1"}],
            "limits": {"max_rows_per_sheet": 1},
        },
    )
    rows = [_row(1, "<i>col</i>", "paired_mismatch", sas="3 <b>bold</b>")]
    html = _publish_html(tmp_path, summary, {"d1": rows}, details_ids=("d1",))
    page_text, sections, hrefs = _visible(html)
    section = sections["dataset-d1"]
    assert '<b>ds & "co"</b>' in section
    assert "Pairing keys: <em>k</em>. Only equal keys pair." in section
    assert "Ignored pairing keys: <u>ghost</u> (SAS only)." in section
    assert "1 row pair does not match. Differing columns: <i>col</i> (1 row)." in section
    # Workbook attribution renders the escaped dataset name for the flagged sheet.
    attribution = (
        'A dataset exceeds --excel-max-rows-per-sheet (1 difference rows) — <b>ds & "co"</b>.'
    )
    assert attribution in page_text
    assert "&lt;b&gt;ds &amp; &#34;co&#34;&lt;/b&gt;" in html
    assert "details/d1.jsonl" in hrefs
    assert (
        page_text.count(
            "Reports may contain sensitive data. Follow local storage and deletion rules."
        )
        == 1
    )

    broken = _dataset(
        "d2",
        "dplocal/broken",
        status="ERROR",
        reason="error",
        error_message='<script>alert("x")</script>',
        difference_row_count=None,
    )
    html = _publish_html(
        tmp_path / "error", _summary([marked, broken]), {"d1": rows}, details_ids=("d1",)
    )
    page_text, sections, _ = _visible(html)
    assert "<script>" not in html
    assert '<script>alert("x")</script>' in sections["dataset-d2"]
    assert 'Comparison failed: <script>alert("x")</script>.' in sections["dataset-d2"]
