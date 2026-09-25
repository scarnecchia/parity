# pattern: Imperative Shell
"""Disk-backed duplicate-aware comparison and streamed detail output.

Only decoded values decide the result; declared column types are never
compared.  Comparison always runs on the columns both sides share; column
deltas and value mismatches are collected as per-dataset conditions rather
than stopping at the first problem.
"""

from __future__ import annotations

import json
import time
import uuid
from typing import TYPE_CHECKING, Any

import duckdb

from sentinel_parity.config import DEFAULT_THREADS
from sentinel_parity.core.comparison_sql import quote_identifier
from sentinel_parity.io import run_log

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path


def compare(
    left: dict[str, Any],
    right: dict[str, Any],
    work: Path,
    memory_limit: str,
    max_temp_size: str,
    dataset_id: str,
    *,
    threads: int = DEFAULT_THREADS,
) -> dict[str, Any]:
    work.mkdir(parents=True, exist_ok=True)
    artifact_id = uuid.uuid4().hex
    spill = work / f"spill-{artifact_id}"
    spill.mkdir()
    run_log.event(
        "compare_start",
        dataset=dataset_id,
        sas_rows=left["rows"],
        python_rows=right["rows"],
        threads=threads,
        memory_limit=memory_limit,
        max_temp_size=max_temp_size,
    )
    connection = duckdb.connect(
        str(work / f"compare-{artifact_id}.duckdb"),
        config={
            "memory_limit": memory_limit,
            "max_temp_directory_size": max_temp_size,
            "temp_directory": str(spill),
            "preserve_insertion_order": "false",
            "threads": str(threads),
        },
    )
    try:
        started = time.monotonic()
        for side, item in (("sas", left), ("python", right)):
            connection.execute(
                f"CREATE TABLE {side} AS SELECT * FROM read_parquet(?)", [str(item["path"])]
            )
        run_log.phase_done(dataset_id, "load_tables", started)
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
            _absent_side_details(connection, left, right, detail_path, dataset_id, union)
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
    started = time.monotonic()
    for side in ("sas", "python"):
        connection.execute(
            f"CREATE TABLE {side}_ranked AS SELECT *, row_number() OVER "
            f"(PARTITION BY {keys} ORDER BY ordinal) AS occurrence FROM {side}"
        )
        connection.execute(
            f"CREATE TABLE {side}_counts AS SELECT {keys}, count(*) AS n "
            f"FROM {side}_ranked GROUP BY ALL"
        )
    run_log.phase_done(dataset_id, "rank", started)
    equality = " AND ".join(
        f"l.{quote_identifier(k)} IS NOT DISTINCT FROM r.{quote_identifier(k)}" for k in keycols
    )
    equality += " AND l.occurrence = r.occurrence"
    started = time.monotonic()
    matched, sas_only, python_only = _join_stats(connection, equality)
    # The occurrence-aligned join yields the per-key multiset difference by
    # construction; the conservation identity is the independent cross-check.
    if matched + sas_only != left["rows"] or matched + python_only != right["rows"]:
        raise RuntimeError("row counts violate multiset conservation")
    run_log.phase_done(dataset_id, "multiset", started)
    started = time.monotonic()
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
    run_log.phase_done(dataset_id, "pairs", started)
    started = time.monotonic()
    order_row = connection.execute(
        "SELECT count(*) FROM sas_ranked l JOIN python_ranked r ON "
        f"{equality} WHERE l.ordinal != r.ordinal"
    ).fetchone()
    order_mismatches = int(order_row[0]) if order_row else 0
    dataset_ref = _sql_string(dataset_id)
    projections = [
        "l.ordinal AS " + quote_identifier("l_ord"),
        "r.ordinal AS " + quote_identifier("r_ord"),
        "sa.diffs AS " + quote_identifier("s_diffs"),
        "sa.pair_rank AS " + quote_identifier("s_pair"),
        "pa.diffs AS " + quote_identifier("p_diffs"),
        "pa.pair_rank AS " + quote_identifier("p_pair"),
    ]
    for name in union:
        if name in left_names:
            projections.append(
                f"l.{quote_identifier('v_' + name)} AS {quote_identifier('l_v_' + name)}"
            )
        if name in right_names:
            projections.append(
                f"r.{quote_identifier('v_' + name)} AS {quote_identifier('r_v_' + name)}"
            )
    sas_record = _side_record(
        "sas",
        quote_identifier("l_ord"),
        quote_identifier("r_ord"),
        _values_object(_present_cell("l_v_", left_names), union),
        quote_identifier("s_diffs"),
        quote_identifier("s_pair"),
        dataset_ref,
    )
    python_record = _side_record(
        "python",
        quote_identifier("r_ord"),
        quote_identifier("l_ord"),
        _values_object(_present_cell("r_v_", right_names), union),
        quote_identifier("p_diffs"),
        quote_identifier("p_pair"),
        dataset_ref,
    )
    # Two statements keep peak memory bounded: the join writes its result to
    # a disk-backed table first, releasing its hash tables before the record
    # expressions build wide JSON vectors per chunk.
    connection.execute(
        "CREATE TABLE detail_base AS SELECT "
        + ", ".join(projections)
        + f" FROM sas_ranked l FULL OUTER JOIN python_ranked r ON {equality} "
        "LEFT JOIN annotations sa ON sa.s_ordinal = l.ordinal "
        "LEFT JOIN annotations pa ON pa.p_ordinal = r.ordinal"
    )
    records_query = (
        "SELECT unnest([" + sas_record + ", " + python_record + "]) AS rec FROM detail_base"
    )
    connection.execute(
        f"COPY (SELECT rec FROM ({records_query}) WHERE rec IS NOT NULL) "
        f"TO {_sql_string(str(detail_path))} "
        "(FORMAT CSV, HEADER FALSE, DELIMITER '|', QUOTE '', ESCAPE '')"
    )
    run_log.phase_done(dataset_id, "details", started)
    return matched, sas_only, python_only, order_mismatches


_ABSENT_COLUMN_JSON = '{"type":"absent_column"}'


def _sql_string(value: str) -> str:
    """One single-quoted SQL string literal."""
    return "'" + value.replace("'", "''") + "'"


def _present_cell(prefix: str, names: set[str]) -> Callable[[str], str | None]:
    """Column expression for staged values the side carries, else None."""

    def cell(name: str) -> str | None:
        return quote_identifier(prefix + name) if name in names else None

    return cell


def _values_object(cell: Callable[[str], str | None], union: list[str]) -> str:
    """json_object call embedding staged v_* envelopes verbatim as JSON.

    `cell` maps a union column to its value expression, or None when the
    column is absent from this side (then the absent_column envelope is
    used).  Staged nulls become JSON null because json_object keeps the key
    with a null value.
    """
    arguments: list[str] = []
    for name in union:
        expression = cell(name)
        if expression is None:
            arguments.append(
                f"{_sql_string(name)}, CAST({_sql_string(_ABSENT_COLUMN_JSON)} AS JSON)"
            )
        else:
            arguments.append(f"{_sql_string(name)}, CAST({expression} AS JSON)")
    return "json_object(" + ", ".join(arguments) + ")"


def _side_record(
    side: str,
    own_ordinal: str,
    other_ordinal: str,
    values: str,
    diffs: str,
    pair_id: str,
    dataset_ref: str,
) -> str:
    """One JSON record expression per base row of `side`, or JSON NULL.

    Rows without an own ordinal produce nothing; matched rows get the PASS
    shape without differing_columns/pair_id keys, and one-sided rows get the
    FAIL shape carrying the annotation columns of the matching side.
    """
    passed = (
        "json_object('schema_version', 1, 'dataset_id', "
        f"{dataset_ref}, 'side', '{side}', 'staging_row_number', {own_ordinal}, "
        f"'status', 'PASS', 'reason', NULL, 'values', {values})"
    )
    failed = (
        "json_object('schema_version', 1, 'dataset_id', "
        f"{dataset_ref}, 'side', '{side}', 'staging_row_number', {own_ordinal}, "
        f"'status', 'FAIL', 'reason', 'only_{side}', 'values', {values}, "
        f"'differing_columns', CAST({diffs} AS JSON), "
        f"'pair_id', {pair_id})"
    )
    return (
        f"CASE WHEN {own_ordinal} IS NULL THEN CAST(NULL AS JSON) "
        f"WHEN {other_ordinal} IS NOT NULL THEN {passed} ELSE {failed} END"
    )


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


def _join_stats(connection: duckdb.DuckDBPyConnection, equality: str) -> tuple[int, int, int]:
    """Matched, sas-only, and python-only counts from the occurrence join.

    For each key the join matches min(occurrences) pairs; left rows beyond the
    right side's multiplicity stay unmatched, so the unmatched filters equal
    the EXCEPT ALL cardinalities without recomputing a window.
    """
    row = connection.execute(
        "SELECT count(*) FILTER (WHERE l.ordinal IS NOT NULL AND r.ordinal IS NOT NULL), "
        "count(*) FILTER (WHERE r.ordinal IS NULL), "
        "count(*) FILTER (WHERE l.ordinal IS NULL) "
        f"FROM sas_ranked l FULL OUTER JOIN python_ranked r ON {equality}"
    ).fetchone()
    matched, sas_only, python_only = (int(value or 0) for value in (row or (0, 0, 0)))
    return matched, sas_only, python_only


def _absent_side_details(
    connection: duckdb.DuckDBPyConnection,
    left: dict[str, Any],
    right: dict[str, Any],
    path: Path,
    dataset_id: str,
    union: list[str],
) -> None:
    """Export every row of both sides when no column is shared to compare."""
    dataset_ref = _sql_string(dataset_id)
    selects: list[str] = []
    for side, item in (("sas", left), ("python", right)):
        values = _values_object(_present_cell("v_", set(item["columns"])), union)
        selects.append(
            "SELECT json_object('schema_version', 1, 'dataset_id', "
            f"{dataset_ref}, 'side', '{side}', 'staging_row_number', source.ordinal, "
            f"'status', 'FAIL', 'reason', 'only_{side}', 'values', {values}) AS rec "
            f"FROM read_parquet({_sql_string(str(item['path']))}) AS source"
        )
    connection.execute(
        "COPY (SELECT rec FROM (" + " UNION ALL ".join(selects) + ")) "
        f"TO {_sql_string(str(path))} "
        "(FORMAT CSV, HEADER FALSE, DELIMITER '|', QUOTE '', ESCAPE '')"
    )
