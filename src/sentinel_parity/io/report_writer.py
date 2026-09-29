# pattern: Imperative Shell
"""Atomic JSON/JSONL and offline HTML report publication."""

from __future__ import annotations

import json
import os
import shutil
import tempfile
from pathlib import Path
from typing import Any

from jinja2 import Environment, FileSystemLoader, select_autoescape

from sentinel_parity.config import DETAIL_DIR_NAME
from sentinel_parity.io.workbook_writer import WORKBOOK_COLUMNS

# Column emphasis classes for the preview table, positional over
# WORKBOOK_COLUMNS: right-aligned identifiers, wrapping text columns.
_PREVIEW_COLUMN_CLASSES = ("number", "wrap", "wrap", "wrap", "", "", "number", "number", "wrap")
# Column width classes for the colgroup, same positions (see report.html CSS).
_PREVIEW_COL_WIDTH_CLASSES = (
    "c-pair",
    "c-column",
    "c-value",
    "c-value",
    "c-type",
    "c-type",
    "c-row",
    "c-row",
    "c-kind",
)

TEMPLATE = Path(__file__).parents[1] / "resources"


def publish(
    output: Path,
    summary: dict[str, Any],
    details: dict[str, Path],
    previews: dict[str, list[dict[str, Any]]],
    preview_truncation: dict[str, dict[str, Any]],
    workbook_source: Path | None = None,
) -> None:
    output.mkdir(parents=True, exist_ok=True)
    detail_dir = output / DETAIL_DIR_NAME
    summary_path = output / "summary.json"
    index_path = output / "index.html"
    workbook_path = output / "differences.xlsx"
    try:
        detail_dir.mkdir(exist_ok=True)
        if workbook_source is not None:
            _atomic_copy(workbook_source, workbook_path)
        else:
            workbook_path.unlink(missing_ok=True)
        links: dict[str, str] = {}
        for artifact_id, source in details.items():
            target = detail_dir / f"{artifact_id}.jsonl"
            _atomic_copy(source, target)
            links[artifact_id] = f"{DETAIL_DIR_NAME}/{artifact_id}.jsonl"
        summary["detail_links"] = links
        env = Environment(loader=FileSystemLoader(TEMPLATE), autoescape=select_autoescape(["html"]))
        row_template = env.get_template("preview_row.html")
        datasets = summary.get("datasets", [])
        limits = summary.get("limits", {})
        remaining_bytes = int(limits.get("preview_total_max_bytes", 10_485_760))
        dataset_limit = int(limits.get("preview_max_bytes", 1_048_576))
        row_limit = int(limits.get("preview_rows", 100))
        for dataset in datasets:
            dataset_id = dataset["id"]
            if "difference_row_count" not in dataset:
                continue
            rows = previews.get(dataset_id, [])
            original_count = int(dataset["difference_row_count"])
            accepted: list[dict[str, Any]] = []
            rendered_bytes = 0
            reasons: list[str] = []
            if original_count > len(rows):
                reasons.append("row_limit")
            for row in rows[:row_limit]:
                # Render the exact fragment that the final include produces so
                # measured bytes match the published row; the include's leading
                # indentation stays outside the measurement.
                fragment = row_template.render(row=row)
                size = len(fragment.encode("utf-8"))
                if rendered_bytes + size > dataset_limit or size > remaining_bytes:
                    if rendered_bytes + size > dataset_limit:
                        reasons.append("dataset_byte_limit")
                    if size > remaining_bytes:
                        reasons.append("run_byte_limit")
                    break
                accepted.append(row)
                rendered_bytes += size
                remaining_bytes -= size
            previews[dataset_id] = accepted
            omitted = max(0, original_count - len(accepted))
            preview_truncation[dataset_id] = {
                "shown_rows": len(accepted),
                "omitted_rows": omitted,
                "rendered_bytes": rendered_bytes,
                "reasons": list(dict.fromkeys(reasons)),
            }
            dataset["preview_truncation"] = preview_truncation[dataset_id]
        summary["preview_truncation"] = preview_truncation
        html = env.get_template("report.html").render(
            summary=summary,
            previews=previews,
            links=links,
            preview_truncation=preview_truncation,
            workbook_columns=WORKBOOK_COLUMNS,
            preview_columns=list(zip(WORKBOOK_COLUMNS, _PREVIEW_COLUMN_CLASSES, strict=True)),
            preview_col_classes=_PREVIEW_COL_WIDTH_CLASSES,
        )
        _atomic_text(summary_path, json.dumps(summary, ensure_ascii=False, indent=2) + "\n")
        _atomic_text(index_path, html)
    except BaseException:
        summary_path.unlink(missing_ok=True)
        index_path.unlink(missing_ok=True)
        workbook_path.unlink(missing_ok=True)
        shutil.rmtree(detail_dir, ignore_errors=True)
        raise


def _atomic_text(path: Path, text: str) -> None:
    fd, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as target:
            target.write(text)
            target.flush()
            os.fsync(target.fileno())
        os.replace(temp_name, path)
    except BaseException:
        Path(temp_name).unlink(missing_ok=True)
        raise


def _atomic_copy(source: Path, target: Path) -> None:
    fd, temp_name = tempfile.mkstemp(prefix=f".{target.name}.", dir=target.parent)
    os.close(fd)
    try:
        with open(source, "rb") as reader, open(temp_name, "wb") as writer:
            shutil.copyfileobj(reader, writer)
            # Flush and fsync so the copied artifact survives a crash before
            # the atomic replace.
            writer.flush()
            os.fsync(writer.fileno())
        os.replace(temp_name, target)
    except BaseException:
        Path(temp_name).unlink(missing_ok=True)
        raise
