# pattern: Imperative Shell
"""Owned, same-run lowercase-path staging for case-sensitive SAS readers."""

from __future__ import annotations

import shutil
import uuid
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pathlib import Path


def stage_sas_input(source: Path, work: Path) -> Path:
    """Return an owned lowercase-extension hardlink/copy for SAS reader APIs."""
    work.mkdir(parents=True, exist_ok=True)
    destination = work / f"{uuid.uuid4().hex}.sas7bdat"
    try:
        destination.hardlink_to(source)
    except OSError:
        shutil.copyfile(source, destination)
    return destination
