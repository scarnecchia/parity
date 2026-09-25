# pattern: Imperative Shell
"""Disk-backed duplicate-aware comparison and streamed detail output.

Only decoded values decide the result; declared column types are never
compared.  Comparison always runs on the columns both sides share; column
deltas and value mismatches are collected as per-dataset conditions rather
than stopping at the first problem.
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
            "threads": "4",
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
        left_names, right_names = set(left_columns), set(right_columns)
        sas_only_columns = [name for name in left_columns if name not in right_names]
        python_only_columns = [name for name in right_columns if name not in left_names]
        shared = [name for name in left_columns if name in right_names]
        union = left_columns + [name for name in right_columns if name not in left_names]
        detail_path = work / f"{artifact_id}.jsonl"
        schema_conditions: list[dict[str, Any]] = []
        if sas_only_columns:
            schema_conditions.append(
                {"severity": "FAIL", "reason": "sas_only_columns", "columns": sas_only_columns}
            )
        if python_only_columns:
            schema_conditions.append(
                {
                    "severity": "WARN",
                    "reason": "python_only_columns",
                    "columns": python_only_columns,
                }
            )
        matched = sas_only = python_only = order_mismatches = 0
        crossed: list[str] = []
        value_conditions: list[dict[str, Any]] = []
        if not shared:
            # No column is comparable: every row is one-sided and every
            # schema delta already appears in the schema conditions.
            _absent_side_details(left, right, detail_path, dataset_id, union)
            sas_only, python_only = left["rows"], right["rows"]
        else:
            matched, sas_only, python_only, order_mismatches = _compare_shared(
                connection,
                left,
                right,
                shared,
                union,
                left_names,
                right_names,
                detail_path,
                dataset_id,
            )
            if sas_only > 0 or python_only > 0:
                crossed = [
                    name
                    for name in shared
                    if {_dtype_kind(left["types"][name]), _dtype_kind(right["types"][name])}
                    == {"number", "text"}
                ]
                crossed_keys = set(crossed)
                payload = _annotated_pair_diffs(connection, shared)
                type_explains = (
                    sas_only == python_only
                    and len(payload) == sas_only
                    and all(set(json.loads(diffs)) <= crossed_keys for _, _, _, diffs in payload)
                )
                if type_explains:
                    value_conditions.append(
                        {"severity": "WARN", "reason": "type_mismatch", "columns": crossed}
                    )
                else:
                    value_conditions.append({"severity": "FAIL", "reason": "value_mismatch"})
        conditions = schema_conditions + value_conditions
        conditions.sort(key=lambda item: item["severity"] != "FAIL")  # stable: FAILs lead
        failures = [item for item in conditions if item["severity"] == "FAIL"]
        primary = failures or conditions
        return {
            "status": "FAIL" if failures else ("WARN" if conditions else "PASS"),
            "reason": primary[0]["reason"] if primary else None,
            "matched": matched,
            "sas_only": sas_only,
            "python_only": python_only,
            "order_mismatches": order_mismatches,
            "type_mismatched_columns": crossed,
            "sas_only_columns": sas_only_columns,
            "python_only_columns": python_only_columns,
            "conditions": conditions,
            "details_path": detail_path,
        }
    finally:
        connection.close()


def _compare_shared(
    connection: duckdb.DuckDBPyConnection,
    left: dict[str, Any],
    right: dict[str, Any],
    shared: list[str],
    union: list[str],
    left_names: set[str],
    right_names: set[str],
    detail_path: Path,
    dataset_id: str,
) -> tuple[int, int, int, int]:
    """Compare both sides on the shared columns and stream row details."""
    keycols = [f"k_{name}" for name in shared]
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
    for side in ("sas", "python"):
        connection.execute(
            f"CREATE TABLE {side}_counts AS SELECT {keys}, count(*) AS n "
            f"FROM {side}_ranked GROUP BY ALL"
        )
    for side, other in (("sas", "python"), ("python", "sas")):
        on = " AND ".join(f"s.{quote_identifier(k)} = c.{quote_identifier(k)}" for k in keycols)
        connection.execute(
            f"CREATE TABLE {side}_excess AS SELECT s.* FROM {side}_ranked s "
            f"LEFT JOIN {other}_counts c ON {on} WHERE s.occurrence > coalesce(c.n, 0)"
        )
        connection.execute(
            f"CREATE TABLE {side}_pair AS SELECT *, row_number() OVER (ORDER BY ordinal) "
            f"AS pair_rank FROM {side}_excess"
        )
    connection.execute(
        "CREATE TABLE annotations("
        "s_ordinal BIGINT, p_ordinal BIGINT, pair_rank BIGINT, diffs VARCHAR)"
    )
    left_keys = ", ".join(f"l.{quote_identifier(k)} AS lk{i}" for i, k in enumerate(keycols))
    right_keys = ", ".join(f"r.{quote_identifier(k)} AS rk{i}" for i, k in enumerate(keycols))
    pair_rows = connection.execute(
        f"SELECT l.ordinal, r.ordinal, l.pair_rank, {left_keys}, {right_keys} "
        f"FROM sas_pair l FULL JOIN python_pair r ON l.pair_rank = r.pair_rank"
    ).fetchall()
    payload = []
    for prow in pair_rows:
        s_ord, p_ord, rank = prow[0], prow[1], prow[2]
        if s_ord is None or p_ord is None:
            continue
        diffs = [name for i, name in enumerate(shared) if prow[3 + i] != prow[3 + len(shared) + i]]
        if diffs:
            payload.append((s_ord, p_ord, rank, json.dumps(diffs)))
    if payload:
        connection.executemany("INSERT INTO annotations VALUES (?, ?, ?, ?)", payload)
    order_row = connection.execute(
        "SELECT count(*) FROM sas_ranked l JOIN python_ranked r ON "
        f"{equality} WHERE l.ordinal != r.ordinal"
    ).fetchone()
    order_mismatches = int(order_row[0]) if order_row else 0
    parts: list[str] = []
    for i, name in enumerate(union):
        if name in left_names:
            parts.append(f"l.{quote_identifier('v_' + name)} AS l_{i}")
        else:
            parts.append(f"CAST(NULL AS VARCHAR) AS l_{i}")
        if name in right_names:
            parts.append(f"r.{quote_identifier('v_' + name)} AS r_{i}")
        else:
            parts.append(f"CAST(NULL AS VARCHAR) AS r_{i}")
    selected = ", ".join(parts)
    query = (
        "SELECT l.ordinal AS l_ord, r.ordinal AS r_ord, "
        "sa.diffs AS s_diffs, sa.pair_rank AS s_pair, "
        "pa.diffs AS p_diffs, pa.pair_rank AS p_pair, "
        f"{selected} FROM sas_ranked l FULL OUTER JOIN python_ranked r ON {equality} "
        "LEFT JOIN annotations sa ON sa.s_ordinal = l.ordinal "
        "LEFT JOIN annotations pa ON pa.p_ordinal = r.ordinal"
    )
    with detail_path.open("w", encoding="utf-8") as output:
        cursor = connection.execute(query)
        while rows := cursor.fetchmany(1024):
            for row in rows:
                l_ord, r_ord = row[0], row[1]
                for side, ordinal, has_other, diffs, pair_id in (
                    ("sas", l_ord, r_ord is not None, row[2], row[3]),
                    ("python", r_ord, l_ord is not None, row[4], row[5]),
                ):
                    if ordinal is not None:
                        values: dict[str, Any] = {}
                        for i, name in enumerate(union):
                            offset = 6 + i * 2
                            raw = row[offset] if side == "sas" else row[offset + 1]
                            present = name in left_names if side == "sas" else name in right_names
                            values[name] = (
                                (json.loads(raw) if raw is not None else None)
                                if present
                                else {"type": "absent_column"}
                            )
                        record = {
                            "schema_version": 1,
                            "dataset_id": dataset_id,
                            "side": side,
                            "staging_row_number": ordinal,
                            "status": "PASS" if has_other else "FAIL",
                            "reason": None
                            if has_other
                            else ("only_sas" if side == "sas" else "only_python"),
                            "values": values,
                        }
                        if not has_other:
                            record["differing_columns"] = json.loads(diffs) if diffs else None
                            record["pair_id"] = pair_id
                        output.write(
                            json.dumps(
                                record,
                                ensure_ascii=False,
                                separators=(",", ":"),
                            )
                            + "\n"
                        )
    return matched, sas_only, python_only, order_mismatches


def _annotated_pair_diffs(
    connection: duckdb.DuckDBPyConnection, shared: list[str]
) -> list[tuple[int, int, int, str]]:
    """Re-read the annotations table as (sas ordinal, parquet ordinal, rank, diffs)."""
    return [
        (int(row[0]), int(row[1]), int(row[2]), str(row[3]))
        for row in connection.execute(
            "SELECT s_ordinal, p_ordinal, pair_rank, diffs FROM annotations"
        ).fetchall()
    ]


def _dtype_kind(value: str) -> str:
    """Coarse value-domain kind: number, text, or other."""
    text = value.casefold().strip()
    if text.startswith(("int", "uint", "float", "decimal")):
        return "number"
    if text.startswith(("string", "large_string", "utf8", "categorical", "enum")):
        return "text"
    return "other"


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


def _absent_side_details(
    left: dict[str, Any],
    right: dict[str, Any],
    path: Path,
    dataset_id: str,
    union: list[str],
) -> None:
    """Stream every row of both sides when no column is shared to compare."""
    with path.open("w", encoding="utf-8") as output:
        for side, item in (("sas", left), ("python", right)):
            source = pq.ParquetFile(item["path"])  # type: ignore[no-untyped-call]
            source_columns = tuple(item["columns"])
            for batch in source.iter_batches(batch_size=1024):  # type: ignore[no-untyped-call]
                for row in batch.to_pylist():
                    values: dict[str, Any] = {}
                    for name in union:
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
                                "reason": "only_sas" if side == "sas" else "only_python",
                                "values": values,
                            },
                            separators=(",", ":"),
                        )
                        + "\n"
                    )
