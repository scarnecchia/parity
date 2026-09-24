# pattern: Functional Core
"""Pure SQL fragments for exact-key duplicate-aware set comparisons."""

from __future__ import annotations


def quote_identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def row_number_sql(table: str, keys: list[str], ordinal: str = "ordinal") -> str:
    columns = ", ".join(quote_identifier(key) for key in keys)
    return (
        f"SELECT *, row_number() OVER (PARTITION BY {columns} "
        f"ORDER BY {quote_identifier(ordinal)}) AS occurrence FROM {quote_identifier(table)}"
    )
