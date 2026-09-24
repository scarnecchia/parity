# pattern: Functional Core
"""Immutable validated run configuration."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


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

    def __post_init__(self) -> None:
        if self.preview_rows < 0 or self.batch_size <= 0:
            raise ValueError("preview_rows must be nonnegative and batch_size must be positive")
        if not self.memory_limit or not self.max_temp_size:
            raise ValueError("memory_limit and max_temp_size must be nonempty")
