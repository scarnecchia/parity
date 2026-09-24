# pattern: Functional Core
"""Pure pairing and unmatched-file analysis."""

from __future__ import annotations

import re
from dataclasses import dataclass

_IDENTITY_PREFIX = re.compile(r"^r[0-9]{2}_", re.IGNORECASE)


@dataclass(frozen=True)
class FileEntry:
    directory: str
    stem: str
    filename: str
    path: str

    @property
    def key(self) -> tuple[str, str]:
        identity_stem = _IDENTITY_PREFIX.sub("", self.stem, count=1)
        return (self.directory.casefold(), identity_stem.casefold())


@dataclass(frozen=True)
class Pairing:
    matched: tuple[tuple[FileEntry, FileEntry], ...]
    sas_only: tuple[FileEntry, ...]
    python_only: tuple[FileEntry, ...]


def pair_files(sas_files: list[FileEntry], python_files: list[FileEntry]) -> Pairing:
    def mapping(files: list[FileEntry]) -> dict[tuple[str, str], FileEntry]:
        result: dict[tuple[str, str], FileEntry] = {}
        for file in files:
            if file.key in result:
                raise ValueError(f"ambiguous case-insensitive dataset identity: {file.filename}")
            result[file.key] = file
        return result

    left, right = mapping(sas_files), mapping(python_files)
    keys = sorted(left.keys() | right.keys())
    return Pairing(
        tuple((left[k], right[k]) for k in keys if k in left and k in right),
        tuple(left[k] for k in keys if k in left and k not in right),
        tuple(right[k] for k in keys if k in right and k not in left),
    )
