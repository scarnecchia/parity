# pattern: Imperative Shell
"""Disk-backed duplicate-aware comparison and streamed difference output."""

from __future__ import annotations

import time
import uuid
from typing import TYPE_CHECKING, Any

import duckdb

if TYPE_CHECKING:
    from pathlib import Path

from sentinel_parity.config import DEFAULT_THREADS
from sentinel_parity.core.comparison_sql import quote_identifier
from sentinel_parity.io import run_log
from sentinel_parity.io.workbook_writer import WORKBOOK_COLUMNS

_COPY_NDJSON = "(FORMAT CSV, HEADER FALSE, DELIMITER '|', QUOTE '', ESCAPE '')"


def compare(
    left: dict[str, Any],
    right: dict[str, Any],
    work: Path,
    memory_limit: str,
    max_temp_size: str,
    dataset_id: str,
    *,
    threads: int = DEFAULT_THREADS,
    preview_rows: int = 100,
    preview_cell_chars: int = 512,
    workbook_stage_path: Path | None = None,
    workbook_remaining: dict[str, int] | None = None,
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
            if any(_unsupported(dtype) for dtype in item["types"].values()):
                raise TypeError("unsupported logical column type")
        left_names, right_names = set(left_columns), set(right_columns)
        sas_only_columns = [name for name in left_columns if name not in right_names]
        python_only_columns = [name for name in right_columns if name not in left_names]
        shared = [name for name in left_columns if name in right_names]
        union = left_columns + [name for name in right_columns if name not in left_names]
        detail_path = work / f"{artifact_id}.jsonl"
        conditions: list[dict[str, Any]] = []
        if sas_only_columns:
            conditions.append(
                {"severity": "FAIL", "reason": "sas_only_columns", "columns": sas_only_columns}
            )
        if python_only_columns:
            conditions.append(
                {
                    "severity": "WARN",
                    "reason": "python_only_columns",
                    "columns": python_only_columns,
                }
            )
        matched = sas_only = python_only = order_mismatches = 0
        counts: dict[str, int] = {}
        pair_count = 0
        crossed: list[str] = []
        if shared:
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
                preview_rows,
                preview_cell_chars,
            )
            if sas_only or python_only:
                crossed = [
                    name
                    for name in shared
                    if {_dtype_kind(left["types"][name]), _dtype_kind(right["types"][name])}
                    == {"number", "text"}
                ]
                counts = _difference_counts(connection)
                pair_count = _scalar_count(
                    connection,
                    "SELECT count(*) FROM pair_differences WHERE kind='paired_mismatch'",
                )
                type_explains = (
                    sas_only == python_only == pair_count
                    and _all_pair_differences_crossed(connection, crossed)
                )
                if type_explains:
                    conditions.append(
                        {"severity": "WARN", "reason": "type_mismatch", "columns": crossed}
                    )
                else:
                    conditions.append({"severity": "FAIL", "reason": "value_mismatch"})
        else:
            _one_sided_no_shared(connection, left, right, preview_rows, preview_cell_chars)
            sas_only, python_only = left["rows"], right["rows"]
            _export_pair_cells(connection, detail_path, dataset_id)
            counts = {}
            pair_count = 0
        difference_rows = _scalar_count(connection, "SELECT count(*) FROM pair_cells")
        details_bytes = detail_path.stat().st_size if detail_path.exists() else 0
        preview = _bounded_preview(connection, preview_rows, preview_cell_chars)
        # The aggregate scan is pure overhead when no workbook can be staged.
        workbook_measurements = (
            _workbook_measurements(connection)
            if workbook_stage_path is not None
            else _workbook_zero_measurements()
        )
        workbook_stage_reasons: list[str] = []
        if workbook_stage_path is not None and difference_rows:
            if workbook_remaining is None:
                raise ValueError("workbook staging requires remaining budgets")
            workbook_stage_reasons = _stage_reasons(
                workbook_measurements, workbook_remaining, dataset_id
            )
            if not workbook_stage_reasons:
                connection.execute(
                    "COPY (SELECT pair_rank AS pair_id,column_name AS column,"
                    f"{_workbook_cell_text('sas')} AS sas,"
                    f"{_workbook_cell_text('python')} AS python,"
                    "json_extract_string(sas,'$.type') AS sas_type,"
                    "json_extract_string(python,'$.type') AS python_type,s_ordinal AS sas_row,"
                    "p_ordinal AS python_row,kind FROM pair_cells "
                    "ORDER BY pair_rank,kind,s_ordinal,p_ordinal,column_name) TO ? "
                    "(FORMAT PARQUET)",
                    [str(workbook_stage_path)],
                )
        conditions.sort(key=lambda item: item["severity"] != "FAIL")
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
            "differing_column_counts": counts,
            "differing_pair_count": pair_count,
            "difference_row_count": difference_rows,
            "details_bytes": details_bytes,
            "preview_rows": preview,
            "workbook_measurements": workbook_measurements,
            "workbook_stage_path": workbook_stage_path
            if workbook_stage_path is not None and not workbook_stage_reasons and difference_rows
            else None,
            "workbook_stage_reasons": workbook_stage_reasons,
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
    preview_rows: int,
    cell_chars: int,
) -> tuple[int, int, int, int]:
    keys = ", ".join(quote_identifier("k_" + name) for name in shared)
    for side in ("sas", "python"):
        ranked_sql = (
            f"CREATE TABLE {side}_ranked AS SELECT *, "
            f"row_number() OVER (PARTITION BY {keys} ORDER BY ordinal) occurrence "
            f"FROM {side}"
        )
        counts_sql = (
            f"CREATE TABLE {side}_counts AS SELECT {keys}, count(*) n "
            f"FROM {side}_ranked GROUP BY ALL"
        )
        connection.execute(ranked_sql)
        connection.execute(counts_sql)
    equality = (
        " AND ".join(
            f"l.{quote_identifier('k_' + n)} IS NOT DISTINCT FROM r.{quote_identifier('k_' + n)}"
            for n in shared
        )
        + " AND l.occurrence=r.occurrence"
    )
    matched, sas_only, python_only = _join_stats(connection, equality)
    if matched + sas_only != left["rows"] or matched + python_only != right["rows"]:
        raise RuntimeError("row counts violate multiset conservation")
    for side, other in (("sas", "python"), ("python", "sas")):
        on = " AND ".join(
            f"s.{quote_identifier('k_' + n)}=c.{quote_identifier('k_' + n)}" for n in shared
        )
        excess_sql = (
            f"CREATE TABLE {side}_excess AS SELECT s.* FROM {side}_ranked s "
            f"LEFT JOIN {other}_counts c ON {on} "
            "WHERE s.occurrence > coalesce(c.n, 0)"
        )
        pair_sql = (
            f"CREATE TABLE {side}_pair AS SELECT *, "
            "row_number() OVER (ORDER BY "
            + ", ".join(f"{quote_identifier('k_' + name)} ASC NULLS FIRST" for name in shared)
            # Ordinal breaks ties between excess rows equal on every shared
            # column so identical reruns pair identically.
            + ", ordinal) pair_rank "
            + f"FROM {side}_excess"
        )
        connection.execute(excess_sql)
        connection.execute(pair_sql)
    pair_match = "l.pair_rank=r.pair_rank"
    tests = " OR ".join(
        f"l.{quote_identifier('k_' + n)} IS DISTINCT FROM r.{quote_identifier('k_' + n)}"
        for n in shared
    )
    pair_query = (
        "CREATE TABLE pair_differences AS SELECT "
        "l.ordinal AS s_ordinal,r.ordinal AS p_ordinal,"
        "coalesce(l.pair_rank,r.pair_rank) AS pair_rank,"
        "CASE WHEN l.ordinal IS NULL THEN 'only_python' "
        "WHEN r.ordinal IS NULL THEN 'only_sas' ELSE 'paired_mismatch' END AS kind "
        "FROM sas_pair l FULL JOIN python_pair r ON "
        + pair_match
        + " WHERE l.ordinal IS NULL OR r.ordinal IS NULL OR ("
        + tests
        + ")"
    )
    connection.execute(pair_query)
    cells = []
    for name in union:
        if name in left_names and name in right_names:
            cells.append(
                "SELECT d.s_ordinal,d.p_ordinal,d.pair_rank,d.kind,"
                f"{_sql_string(name)} AS column_name,"
                f"l.{quote_identifier('v_' + name)} AS sas,"
                f"r.{quote_identifier('v_' + name)} AS python "
                "FROM pair_differences d JOIN sas_pair l ON l.ordinal=d.s_ordinal "
                "JOIN python_pair r ON r.ordinal=d.p_ordinal "
                "WHERE d.kind='paired_mismatch' AND "
                f"l.{quote_identifier('k_' + name)} IS DISTINCT FROM "
                f"r.{quote_identifier('k_' + name)}"
            )
            cells.append(
                "SELECT d.s_ordinal,d.p_ordinal,d.pair_rank,d.kind,"
                f"{_sql_string(name)} AS column_name,"
                f"l.{quote_identifier('v_' + name)} AS sas,NULL::VARCHAR AS python "
                "FROM pair_differences d JOIN sas_pair l ON l.ordinal=d.s_ordinal "
                "WHERE d.kind='only_sas'"
            )
            cells.append(
                "SELECT d.s_ordinal,d.p_ordinal,d.pair_rank,d.kind,"
                f"{_sql_string(name)} AS column_name,"
                f"NULL::VARCHAR AS sas,r.{quote_identifier('v_' + name)} AS python "
                "FROM pair_differences d JOIN python_pair r ON r.ordinal=d.p_ordinal "
                "WHERE d.kind='only_python'"
            )
        elif name in left_names:
            cells.append(
                "SELECT d.s_ordinal,d.p_ordinal,d.pair_rank,d.kind,"
                f"{_sql_string(name)} AS column_name,"
                f"l.{quote_identifier('v_' + name)} AS sas,NULL::VARCHAR AS python "
                "FROM pair_differences d JOIN sas_pair l ON l.ordinal=d.s_ordinal "
                "WHERE d.kind='only_sas'"
            )
        else:
            cells.append(
                "SELECT d.s_ordinal,d.p_ordinal,d.pair_rank,d.kind,"
                f"{_sql_string(name)} AS column_name,"
                f"NULL::VARCHAR AS sas,r.{quote_identifier('v_' + name)} AS python "
                "FROM pair_differences d JOIN python_pair r ON r.ordinal=d.p_ordinal "
                "WHERE d.kind='only_python'"
            )
    connection.execute("CREATE TABLE pair_cells AS " + " UNION ALL ".join(cells))
    _export_pair_cells(connection, detail_path, dataset_id)
    connection.execute(
        "CREATE TEMP TABLE preview_source AS SELECT pair_rank AS pair_id,kind,column_name,"
        "json_extract_string(sas,'$.type') sas_type,"
        "json_extract_string(sas,'$.canonical') sas_canonical,"
        + _preview_projection("sas")
        + ",json_extract_string(python,'$.type') python_type,"
        "json_extract_string(python,'$.canonical') python_canonical,"
        + _preview_projection("python")
        + ",s_ordinal,p_ordinal FROM pair_cells "
        "ORDER BY pair_rank,kind,s_ordinal,p_ordinal,column_name "
        "LIMIT ?",
        [cell_chars, cell_chars, cell_chars, cell_chars, max(0, preview_rows)],
    )
    order_query = (
        "SELECT count(*) FROM sas_ranked l JOIN python_ranked r ON "
        + equality
        + " WHERE l.ordinal != r.ordinal"
    )
    order = connection.execute(order_query).fetchone()
    return matched, sas_only, python_only, int(order[0] if order else 0)


def _bounded_preview(
    connection: duckdb.DuckDBPyConnection, limit: int, cell_chars: int
) -> list[dict[str, Any]]:
    rows = connection.execute(
        "SELECT pair_id,kind,column_name,sas_type,sas_canonical,sas_value,"
        "python_type,python_canonical,python_value,s_ordinal,p_ordinal FROM preview_source "
        "ORDER BY pair_id,kind,s_ordinal,p_ordinal,column_name"
    ).fetchall()
    result: list[dict[str, Any]] = []
    for (
        pair,
        kind,
        column,
        sas_type,
        sas_canonical,
        sas_value,
        python_type,
        python_canonical,
        python_value,
        s_ord,
        p_ord,
    ) in rows:
        result.append(
            {
                "pair_id": pair,
                "kind": kind,
                "column": column,
                "sas": _preview_value(sas_type, sas_canonical, sas_value, s_ord is not None),
                "python": _preview_value(
                    python_type, python_canonical, python_value, p_ord is not None
                ),
                "sas_type": sas_type,
                "python_type": python_type,
                "sas_row": s_ord,
                "python_row": p_ord,
            }
        )
    return result


def _preview_projection(side: str) -> str:
    return (
        f"CASE WHEN {side} IS NULL THEN NULL ELSE "
        f"CASE WHEN length(coalesce(json_extract_string({side},'$.value'),{side}))>? "
        f"THEN left(coalesce(json_extract_string({side},'$.value'),{side}),?-1) || '…' "
        f"ELSE coalesce(json_extract_string({side},'$.value'),{side}) END END {side}_value"
    )


def _preview_value(
    kind: str | None, canonical: str | None, value: str | None, present: bool
) -> str:
    if not present:
        return "(no row)"
    if kind is None:
        return "(missing)"
    if value is None:
        return "(missing)"
    if canonical == "null" or (kind == "string" and value == ""):
        return f"{value} (compares as missing)"
    return value


def _one_sided_no_shared(
    connection: duckdb.DuckDBPyConnection,
    left: dict[str, Any],
    right: dict[str, Any],
    preview_rows: int,
    cell_chars: int,
) -> None:
    selects: list[str] = []
    for side, item in (("sas", left), ("python", right)):
        for column in item["columns"]:
            if side == "sas":
                expressions = (
                    f"ordinal AS s_ordinal,NULL::BIGINT AS p_ordinal,"
                    f"{_sql_string(column)} AS column_name,"
                    f"{quote_identifier('v_' + column)} AS sas,NULL::VARCHAR AS python"
                )
            else:
                expressions = (
                    f"NULL::BIGINT AS s_ordinal,ordinal AS p_ordinal,"
                    f"{_sql_string(column)} AS column_name,"
                    f"NULL::VARCHAR AS sas,{quote_identifier('v_' + column)} AS python"
                )
            selects.append(f"SELECT {expressions}, 'only_{side}' AS kind FROM {side}")
    connection.execute(
        "CREATE TABLE pair_cells AS SELECT s_ordinal,p_ordinal,NULL::BIGINT pair_rank,"
        "column_name, sas, python, kind FROM (" + " UNION ALL ".join(selects) + ") AS cells"
    )
    connection.execute(
        "CREATE TEMP TABLE preview_source AS SELECT NULL::BIGINT pair_id,kind,column_name,"
        "json_extract_string(sas,'$.type') sas_type,"
        "json_extract_string(sas,'$.canonical') sas_canonical,"
        + _preview_projection("sas")
        + ",json_extract_string(python,'$.type') python_type,"
        "json_extract_string(python,'$.canonical') python_canonical,"
        + _preview_projection("python")
        + ",s_ordinal,p_ordinal FROM pair_cells "
        "ORDER BY kind,s_ordinal,p_ordinal,column_name "
        "LIMIT ?",
        [cell_chars, cell_chars, cell_chars, cell_chars, max(0, preview_rows)],
    )


def _export_pair_cells(
    connection: duckdb.DuckDBPyConnection, detail_path: Path, dataset_id: str
) -> None:
    cell_records = (
        "SELECT json_object('schema_version',2,'dataset_id',"
        + _sql_string(dataset_id)
        + ",'pair_id',pair_rank,'kind',kind,'sas_row',s_ordinal,'python_row',p_ordinal,"
        "'differences',list(json_object('column',column_name,'sas',try_cast(sas AS JSON),"
        "'python',try_cast(python AS JSON)) ORDER BY column_name)) rec FROM pair_cells "
        "GROUP BY s_ordinal,p_ordinal,pair_rank,kind"
    )
    connection.execute(f"COPY ({cell_records}) TO {_sql_string(str(detail_path))} {_COPY_NDJSON}")


def _scalar_count(connection: duckdb.DuckDBPyConnection, sql: str) -> int:
    row = connection.execute(sql).fetchone()
    return int(row[0] or 0) if row is not None else 0


def _difference_counts(connection: duckdb.DuckDBPyConnection) -> dict[str, int]:
    rows = connection.execute(
        "SELECT column_name,count(*) FROM pair_cells "
        "WHERE kind='paired_mismatch' GROUP BY column_name"
    ).fetchall()
    return {str(name): int(count) for name, count in rows}


def _all_pair_differences_crossed(
    connection: duckdb.DuckDBPyConnection, crossed: list[str]
) -> bool:
    if not crossed:
        return False
    names = ",".join(_sql_string(name) for name in crossed)
    row = connection.execute(
        "SELECT count(*) FILTER (WHERE column_name NOT IN (" + names + ")) FROM pair_cells "
        "WHERE kind='paired_mismatch'"
    ).fetchone()
    return row is not None and int(row[0] or 0) == 0


def _workbook_cell_text(side: str) -> str:
    # The staged COPY and the workbook measurements render through this one
    # expression, so budgets always describe the exact cells written.
    absent = "only_python" if side == "sas" else "only_sas"
    return (
        f"CASE WHEN kind='{absent}' THEN '(no row)' "
        f"WHEN json_extract({side},'$.value') IS NULL THEN '(missing)' "
        f"WHEN json_extract_string({side},'$.canonical')='null' "
        f"OR (json_extract_string({side},'$.type')='string' "
        f"AND trim(coalesce(json_extract_string({side},'$.value'),''))='') "
        f"THEN coalesce(json_extract_string({side},'$.value'),'') || ' (compares as missing)' "
        f"ELSE json_extract_string({side},'$.value') END"
    )


def _workbook_zero_measurements() -> dict[str, int]:
    """Measurements for runs that can never stage workbook data."""
    return {
        "rows": 0,
        "cell_bytes": 0,
        "header_bytes": 0,
        "text_bytes": 0,
        "max_cell_chars": 0,
        "sheets": 1,
    }


def _workbook_measurements(connection: duckdb.DuckDBPyConnection) -> dict[str, int]:
    sas_text = _workbook_cell_text("sas")
    python_text = _workbook_cell_text("python")
    fields = (
        "CAST(pair_rank AS VARCHAR)",
        "column_name",
        sas_text,
        python_text,
        "coalesce(json_extract_string(sas,'$.type'),'')",
        "coalesce(json_extract_string(python,'$.type'),'')",
        "CAST(s_ordinal AS VARCHAR)",
        "CAST(p_ordinal AS VARCHAR)",
        "kind",
    )
    text_expr = " + ".join(
        f"octet_length(encode(coalesce(CAST({field} AS VARCHAR),'')))" for field in fields
    )
    char_expr = (
        "greatest("
        + ",".join(f"length(coalesce(CAST({field} AS VARCHAR),''))" for field in fields)
        + ")"
    )
    row = connection.execute(
        f"SELECT count(*),coalesce(sum({text_expr}),0),coalesce(max({char_expr}),0) FROM pair_cells"
    ).fetchone()
    values = row or (0, 0, 0)
    has_rows = int(values[0] or 0) > 0
    header_bytes = sum(len(label.encode("utf-8")) for label in WORKBOOK_COLUMNS) if has_rows else 0
    header_chars = max(map(len, WORKBOOK_COLUMNS), default=0) if has_rows else 0
    return {
        "rows": int(values[0] or 0),
        "cell_bytes": int(values[1] or 0),
        "header_bytes": header_bytes,
        "text_bytes": int(values[1] or 0) + header_bytes,
        "max_cell_chars": max(int(values[2] or 0), header_chars),
        "sheets": 2 if has_rows else 1,
    }


def _stage_reasons(
    current: dict[str, int], remaining: dict[str, int], dataset_id: str
) -> list[str]:
    reasons: list[str] = []
    if current["rows"] > remaining["rows"]:
        reasons.append("row_limit")
    if current["rows"] > remaining["rows_per_dataset"]:
        reasons.append(f"sheet_row_limit:{dataset_id}")
    if current["text_bytes"] > remaining["bytes"]:
        reasons.append("byte_limit")
    if max(0, current["sheets"] - 1) > remaining["sheets"]:
        reasons.append("sheet_limit")
    if current["max_cell_chars"] > 32_767:
        reasons.append("cell_limit")
    return reasons


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _dtype_kind(value: str) -> str:
    text = value.casefold().strip()
    if text.startswith(("int", "uint", "float", "decimal")):
        return "number"
    if text.startswith(("string", "large_string", "utf8", "categorical", "enum")):
        return "text"
    return "other"


def _unsupported(value: str) -> bool:
    text = value.casefold().strip()
    return (
        text.startswith(("list(", "array(", "struct(", "map(", "duration", "time("))
        or text == "time"
    )


def _join_stats(connection: duckdb.DuckDBPyConnection, equality: str) -> tuple[int, int, int]:
    query = (
        "SELECT count(*) FILTER (WHERE l.ordinal IS NOT NULL AND r.ordinal IS NOT NULL),"
        "count(*) FILTER (WHERE r.ordinal IS NULL),"
        "count(*) FILTER (WHERE l.ordinal IS NULL) "
        "FROM sas_ranked l FULL OUTER JOIN python_ranked r ON " + equality
    )
    row = connection.execute(query).fetchone()
    values = row or (0, 0, 0)
    return int(values[0] or 0), int(values[1] or 0), int(values[2] or 0)
