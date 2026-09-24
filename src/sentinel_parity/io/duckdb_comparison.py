# pattern: Imperative Shell
"""Disk-backed duplicate-aware comparison and streamed detail output.

Only decoded values decide the result; declared column types are never
compared.
"""

from __future__ import annotations

import json
import uuid
from typing import TYPE_CHECKING, Any

import duckdb
import pyarrow.parquet as pq

from sentinel_parity.core.comparison_sql import quote_identifier

if TYPE_CHECKING:
    from pathlib import Path


def compare(
    left: dict[str, Any],
    right: dict[str, Any],
    work: Path,
    memory_limit: str,
    max_temp_size: str,
    dataset_id: str,
) -> dict[str, Any]:
    work.mkdir(parents=True, exist_ok=True)
    artifact_id = uuid.uuid4().hex
    spill = work / f"spill-{artifact_id}"
    spill.mkdir()
    connection = duckdb.connect(
        str(work / f"compare-{artifact_id}.duckdb"),
        config={
            "memory_limit": memory_limit,
            "max_temp_directory_size": max_temp_size,
            "temp_directory": str(spill),
            "preserve_insertion_order": "false",
            "threads": "1",
        },
    )
    try:
        for side, item in (("sas", left), ("python", right)):
            connection.execute(
                f"CREATE TABLE {side} AS SELECT * FROM read_parquet(?)", [str(item["path"])]
            )
        left_columns, right_columns = list(left["columns"]), list(right["columns"])
        for item in (left, right):
            for dtype in item["types"].values():
                if _unsupported(dtype):
                    raise TypeError("unsupported logical column type")
        if left_columns != right_columns:
            detail_path = _schema_failure(
                left, right, work / f"{artifact_id}.jsonl", dataset_id, "schema_columns_differ"
            )
            return {
                "status": "FAIL",
                "reason": "schema_columns_differ",
                "matched": 0,
                "sas_only": left["rows"],
                "python_only": right["rows"],
                "details_path": detail_path,
            }
        keycols = [f"k_{name}" for name in left_columns]
        keys = ", ".join(quote_identifier(name) for name in keycols)
        for side in ("sas", "python"):
            connection.execute(
                f"CREATE TABLE {side}_ranked AS SELECT *, row_number() OVER "
                f"(PARTITION BY {keys} ORDER BY ordinal) AS occurrence FROM {side}"
            )
        equality = " AND ".join(
            f"l.{quote_identifier(k)} IS NOT DISTINCT FROM r.{quote_identifier(k)}" for k in keycols
        )
        equality += " AND l.occurrence = r.occurrence"
        left_except = _except_count(connection, "sas", "python", keycols)
        right_except = _except_count(connection, "python", "sas", keycols)
        stats = connection.execute(
            "SELECT count(*) FILTER (WHERE l.ordinal IS NOT NULL AND r.ordinal IS NOT NULL), "
            "count(*) FILTER (WHERE r.ordinal IS NULL), "
            "count(*) FILTER (WHERE l.ordinal IS NULL) "
            f"FROM sas_ranked l FULL OUTER JOIN python_ranked r ON {equality}"
        ).fetchone()
        matched, sas_only, python_only = (int(value or 0) for value in (stats or (0, 0, 0)))
        if (left_except, right_except) != (sas_only, python_only):
            raise RuntimeError("independent multiset counts disagree")
        selected = ", ".join(
            f"l.{quote_identifier('v_' + n)} AS l_{i}, r.{quote_identifier('v_' + n)} AS r_{i}"
            for i, n in enumerate(left_columns)
        )
        query = (
            "SELECT l.ordinal AS l_ord, r.ordinal AS r_ord, "
            f"{selected} FROM sas_ranked l FULL OUTER JOIN python_ranked r ON {equality}"
        )
        detail_path = work / f"{artifact_id}.jsonl"
        with detail_path.open("w", encoding="utf-8") as output:
            cursor = connection.execute(query)
            while rows := cursor.fetchmany(1024):
                for row in rows:
                    l_ord, r_ord = row[0], row[1]
                    for side, ordinal, has_other, offset in (
                        ("sas", l_ord, r_ord is not None, 2),
                        ("python", r_ord, l_ord is not None, 3),
                    ):
                        if ordinal is not None:
                            values = {
                                name: json.loads(row[offset + i * 2])
                                for i, name in enumerate(left_columns)
                            }
                            output.write(
                                json.dumps(
                                    {
                                        "schema_version": 1,
                                        "dataset_id": dataset_id,
                                        "side": side,
                                        "staging_row_number": ordinal,
                                        "status": "PASS" if has_other else "FAIL",
                                        "reason": None
                                        if has_other
                                        else ("only_sas" if side == "sas" else "only_python"),
                                        "values": values,
                                    },
                                    ensure_ascii=False,
                                    separators=(",", ":"),
                                )
                                + "\n"
                            )
        return {
            "status": "PASS" if sas_only == 0 and python_only == 0 else "FAIL",
            "reason": None,
            "matched": matched,
            "sas_only": sas_only,
            "python_only": python_only,
            "details_path": detail_path,
        }
    finally:
        connection.close()


def _unsupported(value: str) -> bool:
    """True only for value shapes staging cannot decode at all."""
    text = value.casefold().strip()
    return (
        text.startswith(("list(", "array(", "struct(", "map(", "duration", "time("))
        or text == "time"
    )


def _except_count(
    connection: duckdb.DuckDBPyConnection, left: str, right: str, keycols: list[str]
) -> int:
    columns = ", ".join(quote_identifier(name) for name in keycols)
    query = (
        f"SELECT count(*) FROM (SELECT {columns} FROM {left} "
        f"EXCEPT ALL SELECT {columns} FROM {right})"
    )
    row = connection.execute(query).fetchone()
    return int(row[0] if row else 0)


def _schema_failure(
    left: dict[str, Any], right: dict[str, Any], path: Path, dataset_id: str, reason: str
) -> Path:
    names = sorted(set(left["columns"]) | set(right["columns"]))
    with path.open("w", encoding="utf-8") as output:
        for side, item in (("sas", left), ("python", right)):
            source = pq.ParquetFile(item["path"])  # type: ignore[no-untyped-call]
            source_columns = tuple(item["columns"])
            for batch in source.iter_batches(batch_size=1024):  # type: ignore[no-untyped-call]
                for row in batch.to_pylist():
                    values: dict[str, Any] = {}
                    for name in names:
                        if name not in source_columns:
                            values[name] = {"type": "absent_column"}
                            continue
                        encoded = row[f"v_{name}"]
                        values[name] = json.loads(encoded) if encoded is not None else None
                    output.write(
                        json.dumps(
                            {
                                "schema_version": 1,
                                "dataset_id": dataset_id,
                                "side": side,
                                "staging_row_number": int(row["ordinal"]),
                                "status": "FAIL",
                                "reason": reason,
                                "values": values,
                            },
                            separators=(",", ":"),
                        )
                        + "\n"
                    )
    return path
