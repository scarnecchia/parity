# pattern: Imperative Shell
"""Loading and validation for the optional per-table schema.toml file."""

from __future__ import annotations

import tomllib
from typing import TYPE_CHECKING

from sentinel_parity.core.discovery import normalize_identity_stem

if TYPE_CHECKING:
    from pathlib import Path

_ALLOWED_SECTION_KEYS = frozenset({"pair_keys"})


def load_schema(path: Path) -> dict[str, tuple[str, ...]]:
    """Parse schema.toml into normalized table stems and their pairing keys.

    Section names are table names: case-folded, with one leading rNN_ prefix
    ignored, so [r01_attrition] and [attrition] configure the same table.
    Raises ValueError on unreadable TOML, unknown section keys, malformed
    pair_keys, or sections that normalize to the same table.
    """
    try:
        values = tomllib.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, tomllib.TOMLDecodeError) as exc:
        raise ValueError(f"failed to read valid TOML schema file: {path}") from exc
    tables: dict[str, tuple[str, ...]] = {}
    for name, section in values.items():
        if not isinstance(section, dict):
            raise ValueError(f"schema section [{name}] must be a table")
        unknown = set(section) - _ALLOWED_SECTION_KEYS
        if unknown:
            raise ValueError(
                f"unknown key in schema section [{name}]: {', '.join(sorted(unknown))}. "
                'Sections support pair_keys = ["column", ...]'
            )
        keys = section.get("pair_keys", ())
        if not isinstance(keys, list) or any(not isinstance(key, str) or not key for key in keys):
            raise ValueError(
                f"schema section [{name}] pair_keys must be a list of nonempty column names"
            )
        folded = [key.casefold() for key in keys]
        if len(set(folded)) != len(folded):
            raise ValueError(f"schema section [{name}] pair_keys must not repeat a column")
        stem = normalize_identity_stem(name)
        if not stem:
            raise ValueError(f"schema section [{name}] normalizes to an empty table name")
        if stem in tables:
            raise ValueError(
                f"schema section [{name}] normalizes to table {stem!r}, already configured"
            )
        tables[stem] = tuple(keys)
    return tables
