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

TEMPLATE = Path(__file__).parents[1] / "resources"


def publish(
    output: Path,
    summary: dict[str, Any],
    details: dict[str, Path],
    previews: dict[str, list[dict[str, Any]]],
    preview_truncation: dict[str, dict[str, int]],
) -> None:
    output.mkdir(parents=True, exist_ok=True)
    detail_dir = output / "details"
    detail_dir.mkdir(exist_ok=True)
    links: dict[str, str] = {}
    for artifact_id, source in details.items():
        target = detail_dir / f"{artifact_id}.jsonl"
        _atomic_copy(source, target)
        links[artifact_id] = f"details/{artifact_id}.jsonl"
    summary["detail_links"] = links
    env = Environment(loader=FileSystemLoader(TEMPLATE), autoescape=select_autoescape(["html"]))
    html = env.get_template("report.html").render(
        summary=summary,
        previews=previews,
        links=links,
        preview_truncation=preview_truncation,
    )
    summary_path = output / "summary.json"
    index_path = output / "index.html"
    try:
        _atomic_text(summary_path, json.dumps(summary, ensure_ascii=False, indent=2) + "\n")
        _atomic_text(index_path, html)
    except BaseException:
        summary_path.unlink(missing_ok=True)
        index_path.unlink(missing_ok=True)
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
        shutil.copyfile(source, temp_name)
        os.replace(temp_name, target)
    except BaseException:
        Path(temp_name).unlink(missing_ok=True)
        raise
