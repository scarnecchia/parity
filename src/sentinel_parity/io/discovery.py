# pattern: Imperative Shell
"""Safe immediate-child dataset discovery."""

from __future__ import annotations

from typing import TYPE_CHECKING

from sentinel_parity.core.discovery import FileEntry

if TYPE_CHECKING:
    from pathlib import Path

    from sentinel_parity.config import RunConfig

EXTENSIONS = {".sas7bdat", ".parquet"}
DIRECTORIES = ("dplocal", "msoc")


def discover(root: Path, extension: str) -> list[FileEntry]:
    if not root.is_dir() or not root.exists():
        raise ValueError("input root must be an existing directory")
    result: list[FileEntry] = []
    for directory in DIRECTORIES:
        child = root / directory
        if not child.is_dir():
            raise ValueError("input root must contain readable dplocal and msoc directories")
        for path in child.iterdir():
            if path.suffix.casefold() != extension:
                continue
            if path.is_symlink():
                raise ValueError("matching-extension symlinks are unsupported")
            if not path.is_file():
                raise ValueError("matching-extension non-file entries are unsupported")
            result.append(FileEntry(directory, path.stem, path.name, str(path.resolve())))
    return result


def validate_roots_and_output(config: RunConfig) -> None:
    sas_root = config.sas_root.resolve()
    python_root = config.python_root.resolve()
    output = config.output_dir.resolve()
    temp = config.temp_dir.resolve() if config.temp_dir else None
    for candidate in (output, temp):
        if candidate and (
            candidate in (sas_root, python_root)
            or sas_root in candidate.parents
            or python_root in candidate.parents
        ):
            raise ValueError("output and temp directories must be outside input roots")
    if output.exists() and any(output.iterdir()):
        raise ValueError("output directory must be absent or empty")
