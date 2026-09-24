# pattern: Functional Core
"""Pure dataset/column identity normalization."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class NormalizedName:
    original: str
    normalized: str


def normalize_name(name: str) -> str:
    return name.casefold()


def normalize_columns(names: list[str]) -> tuple[NormalizedName, ...]:
    values = tuple(NormalizedName(name, normalize_name(name)) for name in names)
    normalized = [value.normalized for value in values]
    if len(set(normalized)) != len(normalized):
        raise ValueError("duplicate normalized column names")
    return tuple(sorted(values, key=lambda value: value.normalized))
