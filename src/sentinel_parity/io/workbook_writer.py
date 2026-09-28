# pattern: Imperative Shell
"""Create a constant-memory Excel workbook from bounded flat difference rows."""

from __future__ import annotations

import os
import re
import tempfile
import warnings
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
import xlsxwriter

WORKBOOK_COLUMNS = (
    "Pair",
    "Column",
    "SAS value",
    "Parquet value",
    "SAS type",
    "Parquet type",
    "SAS row",
    "Parquet row",
    "Kind",
)


def safe_sheet_names(names: list[str]) -> dict[str, str]:
    used = {"index"}
    result: dict[str, str] = {}
    for name in names:
        stem = re.sub(r"[\\/*?:\[\]]", "_", name)[:31].strip("'") or "Dataset"
        candidate = stem
        suffix = 1
        while candidate.casefold() in used:
            tail = f"_{suffix}"
            candidate = stem[: 31 - len(tail)] + tail
            suffix += 1
        used.add(candidate.casefold())
        result[name] = candidate
    return result


def _write_text(worksheet: Any, row: int, column: int, value: Any) -> None:
    text = "" if value is None else str(value)
    if len(text) > 32_767:
        raise ValueError("workbook cell exceeds Excel text limit")
    if worksheet.write_string(row, column, text) != 0:
        raise OSError("failed to write workbook cell")


def write_workbook(
    path: Path,
    datasets: list[dict[str, Any]],
    staged: dict[str, Path],
    temp_dir: Path,
    *,
    batch_size: int = 4096,
) -> None:
    temp_dir.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(prefix=".sentinel-parity-", suffix=".xlsx", dir=temp_dir)
    os.close(fd)
    try:
        workbook = xlsxwriter.Workbook(
            temp_name,
            {
                "constant_memory": True,
                "tmpdir": str(temp_dir),
                "strings_to_formulas": False,
                "strings_to_urls": False,
                "strings_to_numbers": False,
            },
        )
        try:
            workbook.add_worksheet("Index")
            index = workbook.get_worksheet_by_name("Index")
            if index is None:
                raise OSError("failed to create workbook index sheet")
            index.freeze_panes(1, 0)
            headers = ("Dataset", "Status", "Conditions", "Sheet")
            for col, value in enumerate(headers):
                _write_text(index, 0, col, value)
            sheet_map = safe_sheet_names(
                [item["name"] for item in datasets if item["id"] in staged]
            )
            row = 1
            for dataset in datasets:
                sheet = sheet_map.get(dataset["name"], "")
                conditions = ", ".join(c["reason"] for c in dataset.get("conditions", []))
                for col, value in enumerate(
                    (dataset["name"], dataset["status"], conditions, sheet)
                ):
                    _write_text(index, row, col, value)
                row += 1
                if not sheet:
                    continue
                worksheet = workbook.add_worksheet(sheet)
                worksheet.freeze_panes(1, 0)
                for col, value in enumerate(WORKBOOK_COLUMNS):
                    _write_text(worksheet, 0, col, value)
                reader = pq.ParquetFile(staged[dataset["id"]]).iter_batches(  # type: ignore[no-untyped-call]
                    batch_size=batch_size
                )
                row_number = 1
                for batch in reader:
                    columns = {
                        name: batch.column(column_index).to_pylist()
                        for column_index, name in enumerate(batch.schema.names)
                    }
                    for row_index in range(batch.num_rows):
                        values = tuple(
                            columns[column][row_index]
                            for column in (
                                "pair_id",
                                "column",
                                "sas",
                                "python",
                                "sas_type",
                                "python_type",
                                "sas_row",
                                "python_row",
                                "kind",
                            )
                        )
                        for col, raw_value in enumerate(values):
                            _write_text(worksheet, row_number, col, raw_value)
                        row_number += 1
                worksheet.autofilter(0, 0, row_number - 1, len(WORKBOOK_COLUMNS) - 1)
            workbook.close()
        except BaseException:
            with warnings.catch_warnings():
                # A failure after the first close leaves cleanup closing an
                # already finalized workbook; that repeat close is intentional.
                warnings.filterwarnings(
                    "ignore", message=r"Calling close\(\) on already closed file\."
                )
                workbook.close()
            raise
        os.replace(temp_name, path)
    except BaseException:
        Path(temp_name).unlink(missing_ok=True)
        raise
