# pattern: Imperative Shell
"""TOML loading and command-line/config precedence."""

from __future__ import annotations

import tomllib
from pathlib import Path
from typing import Any

from sentinel_parity.config import RunConfig

_ALLOWED = {
    "sas_root",
    "python_root",
    "output_dir",
    "id",
    "memory_limit",
    "temp_dir",
    "max_temp_size",
    "preview_rows",
    "round_digits",
    "threads",
}


def _path(value: str | Path, base: Path) -> Path:
    result = Path(value).expanduser()
    return (base / result).resolve() if not result.is_absolute() else result.resolve()


def load_config(config_path: Path | None, overrides: dict[str, Any]) -> RunConfig:
    values: dict[str, Any] = {}
    base = Path.cwd()
    if config_path is not None:
        source = config_path.expanduser().resolve()
        base = source.parent
        try:
            values = tomllib.loads(source.read_text(encoding="utf-8"))
        except (OSError, tomllib.TOMLDecodeError) as exc:
            raise ValueError("failed to read valid TOML configuration") from exc
        unknown = set(values) - _ALLOWED
        if unknown:
            raise ValueError("unknown configuration key")
    for key, value in overrides.items():
        if value is not None:
            values[key] = value
    if "sas_root" not in values or "python_root" not in values:
        raise ValueError("both sas_root and python_root are required")
    for key in ("sas_root", "python_root", "output_dir", "temp_dir"):
        if key in values and values[key] is not None:
            if not isinstance(values[key], (str, Path)):
                raise ValueError(f"{key} must be a string path")
            values[key] = _path(
                values[key], base if key not in overrides or overrides[key] is None else Path.cwd()
            )
    for key in ("memory_limit", "max_temp_size"):
        if key in values and not isinstance(values[key], str):
            raise ValueError(f"{key} must be a string")
    if "preview_rows" in values and (
        isinstance(values["preview_rows"], bool) or not isinstance(values["preview_rows"], int)
    ):
        raise ValueError("preview_rows must be an integer")
    if "round_digits" in values and (
        isinstance(values["round_digits"], bool) or not isinstance(values["round_digits"], int)
    ):
        raise ValueError("round_digits must be an integer")
    if "threads" in values and (
        isinstance(values["threads"], bool) or not isinstance(values["threads"], int)
    ):
        raise ValueError("threads must be an integer")
    return RunConfig(**values)
