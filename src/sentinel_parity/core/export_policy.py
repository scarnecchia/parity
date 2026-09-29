# pattern: Functional Core
"""Pure policies for bounded difference previews and workbook eligibility."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class PreviewLimits:
    rows: int = 100
    dataset_bytes: int = 1_048_576
    run_bytes: int = 10_485_760
    cell_chars: int = 512


@dataclass(frozen=True)
class ExcelLimits:
    enabled: bool = True
    sheets: int = 100
    rows: int = 100_000
    rows_per_sheet: int = 25_000
    bytes: int = 104_857_600


@dataclass(frozen=True)
class WorkbookMeasurements:
    rows: int
    text_bytes: int
    max_cell_chars: int
    sheets: int
    max_rows_per_sheet: int


def excel_omissions(measurements: WorkbookMeasurements, limits: ExcelLimits) -> tuple[str, ...]:
    """Return every policy reason that prevents complete workbook output."""
    if not limits.enabled:
        return ()
    reasons: list[str] = []
    if measurements.sheets > min(limits.sheets, 100):
        reasons.append("sheet_limit")
    if measurements.rows > min(limits.rows, 1_048_575):
        reasons.append("row_limit")
    if measurements.max_rows_per_sheet > min(limits.rows_per_sheet, 1_048_575):
        reasons.append("sheet_row_limit")
    if measurements.text_bytes > limits.bytes:
        reasons.append("byte_limit")
    if measurements.max_cell_chars > 32_767:
        reasons.append("cell_limit")
    return tuple(reasons)
