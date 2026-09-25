"""Deterministic adversarial dataset for the staging and detail parity harness.

Every value class the encoder must survive appears at least once: signed
zero, subnormals, extreme exponents, integers-as-floats, NaN and infinities,
Decimal-grammar text ("1_0", "Infinity", "1e999", Unicode digits), blank and
whitespace text, Unicode text, nulls, binary, decimal128, ns timestamps, and
tz-aware timestamps.  Variant "b" of the dataset derives from variant "a" by
a fixed permutation, eight key-changing value tweaks, one dropped row, and
one duplicated row, so a two-sided comparison exercises occurrence pairing,
order mismatches, FAIL annotations, and one-sided excess rows deterministically.
"""

from __future__ import annotations

import json
import math
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

ROW_COUNT = 21

# (Id, Flag, Amount, Code, Day, Instant, Moment, Tiny, Blob, Exact) per row.
_BASE_ROWS: tuple[tuple[Any, ...], ...] = (
    (
        0,
        True,
        16.0,
        " 12 ",
        date(2024, 1, 1),
        1_234_567_891_234_567_890,
        1_700_000_000_000_001,
        0,
        b"",
        Decimal("1.25"),
    ),
    (1, False, -0.0, "12", date(1970, 1, 1), 0, 0, 1, b"\x00\xff", Decimal("0")),
    (2, None, 0.0, "1_0", date(1969, 12, 31), -1, -1, 255, b"abc", None),
    (
        2**53,
        True,
        5e-324,
        "Infinity",
        date(9999, 12, 31),
        1,
        1_234,
        None,
        None,
        Decimal("-0.00001"),
    ),
    (
        2**53 + 1,
        None,
        1e308,
        "NaN",
        None,
        None,
        None,
        42,
        bytes(range(16)),
        Decimal("12345678901234567890.1234567890"),
    ),
    (
        -(2**53) - 1,
        False,
        -1e308,
        "nan",
        date(2024, 2, 29),
        86_399_999_999_999_999,
        None,
        0,
        b"\xff",
        None,
    ),
    (
        2**63 - 1,
        True,
        1e-308,
        "1e999",
        date(1900, 1, 1),
        -86_399_999_999_999_999,
        9_223_372_036_854_775,
        None,
        b"",
        Decimal("0.0000000001"),
    ),
    (
        -(2**63),
        None,
        math.inf,
        "\u0661\u0662",
        None,
        9_223_372_036_854_775_807,
        -9_223_372_036_854_775,
        7,
        None,
        Decimal("-0.0000000001"),
    ),
    (
        None,
        False,
        -math.inf,
        "",
        date(2024, 1, 15),
        -9_223_372_036_854_775_808,
        0,
        None,
        b"pad",
        Decimal("1E+20"),
    ),
    (-5, True, math.nan, "   ", date(2024, 12, 31), 1_000_000_000, 1_000_001, 100, b" ", None),
    (5, None, 0.1, "abc  ", None, None, 1_000_002, 2, b"\x00", Decimal("2.5")),
    (
        -7,
        True,
        -16.2,
        "caf\u00e9 \u2602",
        date(2000, 2, 29),
        1_000,
        1,
        3,
        b"\x00\xff\x00",
        Decimal("-2.5"),
    ),
    (16, False, float(2**53), "16.2", date(2024, 1, 2), 1_000_001, 2, 4, None, Decimal("16.2")),
    (None, None, -16.25, "48.32832", None, 1_000_002, 3, 5, b"Z", Decimal("48.32832")),
    (2**31, True, 2.5, "+7", date(2024, 6, 30), 1_000_003, 4, 6, b"\t\n", Decimal("7")),
    (-(2**31), False, -2.5, ".5", date(2024, 6, 30), 1_000_004, 5, 7, b"\n", Decimal("0.5")),
)

# Key-changing perturbations applied to distinct rows of variant "b", keyed by
# base row index and column position (see _BASE_ROWS tuple order).
_TWEAKS: dict[int, dict[int, Any]] = {
    3: {2: 1e-323},
    4: {3: "NAN!"},
    5: {8: b"\xfe"},
    6: {9: Decimal("0.0000000002")},
    7: {5: 9_223_372_036_854_775_806},
    8: {6: 5},
    9: {3: "  .  "},
    10: {2: 0.11},
}

_FRAME_SCHEMA: dict[str, pl.DataType] = {
    "Id": pl.Int64,
    "Flag": pl.Boolean,
    "Amount": pl.Float64,
    "Code": pl.String,
    "Day": pl.Date,
    "Instant": pl.Datetime("ns"),
    "Moment": pl.Datetime("us", "UTC"),
    "Tiny": pl.UInt8,
    "Blob": pl.Binary,
}


def _rows(variant: str) -> list[list[Any]]:
    rows = [list(row) for row in _BASE_ROWS]
    if variant == "empty":
        return []
    if variant in ("a", "b"):
        rows += [list(_BASE_ROWS[1])] * 5
    if variant == "b":
        for row_index, changes in _TWEAKS.items():
            for column_index, value in changes.items():
                rows[row_index][column_index] = value
        rows.reverse()
        drop = max(index for index, row in enumerate(rows) if row[0] == 1)
        del rows[drop]
        rows.append(list(next(row for row in rows if row[0] == 2)))
    assert variant != "empty" or not rows
    return rows


def adversarial_table(variant: str) -> pa.Table:
    """Build the adversarial Arrow table: "a", "b", or "empty"."""
    rows = _rows(variant)
    frame = pl.DataFrame(
        {name: [row[index] for row in rows] for index, name in enumerate(_FRAME_SCHEMA)},
        schema=_FRAME_SCHEMA,
    )
    table = frame.to_arrow()
    exact = pa.array([row[9] for row in rows], type=pa.decimal128(38, 10))
    table = table.append_column(pa.field("Exact", pa.decimal128(38, 10)), exact)
    for column, value_of in (("SasExtra", "sx{}"), ("PyExtra", "px{}")):
        wanted = ("a" if column == "SasExtra" else "b") == variant
        values = pa.array(
            [value_of.format(index) for index in range(len(rows))],
            type=pa.string(),
        )
        if wanted:
            table = table.append_column(pa.field(column, pa.string()), values)
    return table


def write_adversarial(path: Path, variant: str) -> None:
    pq.write_table(adversarial_table(variant), path)


def write_disjoint(path: Path) -> None:
    """A dataset sharing no column with the adversarial set."""
    pq.write_table(pa.table({"Unrelated": pa.array([1, 2, 3], type=pa.int64())}), path)


def stage_grid(
    source: Path,
    work: Path,
    kind: str = "python",
    round_digits: int | None = None,
    batch_size: int = 65536,
) -> dict[str, Any]:
    """Stage one file and read its canonical k_/v_ columns back verbatim."""
    from sentinel_parity.io.staging import stage

    work.mkdir(parents=True, exist_ok=True)
    info = stage(source, kind, work, batch_size=batch_size, round_digits=round_digits)
    names = list(info["columns"])
    table = pq.read_table(
        info["path"], columns=[f"{prefix}{name}" for name in names for prefix in ("k_", "v_")]
    )
    columns = {
        f"{prefix}{name}": table.column(f"{prefix}{name}").to_pylist()
        for name in names
        for prefix in ("k_", "v_")
    }
    rows = [
        [cell for name in names for cell in (columns[f"k_{name}"][i], columns[f"v_{name}"][i])]
        for i in range(info["rows"])
    ]
    return {"columns": names, "types": dict(info["types"]), "rows": rows}


def comparison_capture(left: Path, right: Path, dataset_id: str, work: Path) -> dict[str, Any]:
    """Stage both sides, compare, and return sorted detail records plus the result."""
    from sentinel_parity.io.duckdb_comparison import compare
    from sentinel_parity.io.staging import stage

    for part in ("left", "right"):
        (work / part).mkdir(parents=True, exist_ok=True)
    left_staged = stage(left, "python", work / "left")
    right_staged = stage(right, "python", work / "right")
    result = compare(left_staged, right_staged, work / "cmp", "1GB", "10GB", dataset_id)
    records = [
        json.loads(line)
        for line in Path(result["details_path"]).read_text(encoding="utf-8").splitlines()
    ]
    records.sort(key=lambda record: (record["side"], record["staging_row_number"]))
    return {"result": {k: v for k, v in result.items() if k != "details_path"}, "records": records}
