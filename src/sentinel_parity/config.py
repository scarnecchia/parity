# pattern: Functional Core
"""Immutable validated run configuration."""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

DETAIL_DIR_NAME = "details"
_ID_PATTERN = re.compile(r"[A-Za-z0-9_]+")
_RESERVED_IDS = frozenset({DETAIL_DIR_NAME.casefold()})
_ID_MAX_LENGTH = 64
DEFAULT_THREADS = 4


@dataclass(frozen=True)
class RunConfig:
    sas_root: Path
    python_root: Path
    output_dir: Path = Path("parity-report")
    memory_limit: str = "1GB"
    temp_dir: Path | None = None
    max_temp_size: str = "10GB"
    preview_rows: int = 100
    batch_size: int = 65536
    round_digits: int | None = None
    id: str | None = None
    threads: int = DEFAULT_THREADS

    def __post_init__(self) -> None:
        if self.preview_rows < 0 or self.batch_size <= 0:
            raise ValueError("preview_rows must be nonnegative and batch_size must be positive")
        if self.round_digits is not None and (
            isinstance(self.round_digits, bool) or self.round_digits < 1
        ):
            raise ValueError("round_digits must be a positive integer")
        if not self.memory_limit or not self.max_temp_size:
            raise ValueError("memory_limit and max_temp_size must be nonempty")
        if not isinstance(self.threads, int) or isinstance(self.threads, bool) or self.threads < 1:
            raise ValueError("threads must be a positive integer")
        if self.id is not None:
            if not isinstance(self.id, str):
                raise ValueError("id must be a string")
            if _ID_PATTERN.fullmatch(self.id) is None:
                raise ValueError("id must contain only letters, numbers, and underscores")
            if len(self.id) > _ID_MAX_LENGTH:
                raise ValueError(f"id must be at most {_ID_MAX_LENGTH} characters")
            if self.id.casefold() in _RESERVED_IDS:
                raise ValueError("id must not be the reserved report name 'details'")

    @property
    def effective_output_dir(self) -> Path:
        """Report directory with the id subfolder applied; output_dir itself when id is unset."""
        return self.output_dir / self.id if self.id is not None else self.output_dir
