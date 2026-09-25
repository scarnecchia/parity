# pattern: Imperative Shell
"""Structured JSONL run logging: file output always, stderr echo when verbose.

Every event is one JSON object carrying a wall-clock timestamp, elapsed
seconds, an event name, and semantic fields (counts, durations, paths,
identifiers). Exceptions are classified, never quoted: only the class name
and, for OSError, the errno and strerror reach the log, so cell values and
reader/database message text never enter logs or console output.
"""

from __future__ import annotations

import json
import sys
import time
from contextlib import suppress
from datetime import UTC, datetime
from typing import IO, TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pathlib import Path

_pending: list[str] = []
_handle: IO[str] | None = None
_verbose = False
_started = 0.0


def start() -> None:
    """Reset logging state and record the run's time origin."""
    global _pending, _handle, _started
    if _handle is not None:
        _handle.close()
    _pending = []
    _handle = None
    _started = time.monotonic()


def set_verbose(enabled: bool) -> None:
    """Echo every event line to stderr as it is produced."""
    global _verbose
    _verbose = enabled


def attach(destination: Path) -> None:
    """Write subsequent events to destination, draining buffered startup events."""
    global _handle, _pending
    if _handle is not None:
        _handle.close()
    destination.parent.mkdir(parents=True, exist_ok=True)
    handle = destination.open("w", encoding="utf-8")
    for line in _pending:
        handle.write(line)
    handle.flush()
    _pending = []
    _handle = handle


def event(event_name: str, **fields: Any) -> None:
    """Record one structured event; counts, durations, paths, and ids only."""
    record = {
        "ts": datetime.now(UTC).isoformat(timespec="milliseconds"),
        "elapsed_s": round(time.monotonic() - _started, 3),
        "event": event_name,
        **fields,
    }
    line = json.dumps(record, ensure_ascii=False) + "\n"
    if _verbose:
        _echo(line)
    if _handle is not None:
        _handle.write(line)
        _handle.flush()
    else:
        _pending.append(line)


def error(event_name: str, exc: BaseException, **fields: Any) -> None:
    """Record a failure event; class name plus OSError errno/strerror only."""
    details: dict[str, Any] = {"error": type(exc).__name__, **fields}
    if isinstance(exc, OSError):
        if exc.errno is not None:
            details["errno"] = exc.errno
        if exc.strerror is not None:
            details["strerror"] = exc.strerror
    event(event_name, **details)


def phase_done(dataset: str, phase: str, started: float) -> None:
    """Record the wall duration of a named comparison phase."""
    event(
        "compare_phase",
        dataset=dataset,
        phase=phase,
        duration_s=round(time.monotonic() - started, 3),
    )


def close() -> None:
    """Stop file logging; unechoed buffered events drain to stderr."""
    global _handle, _pending
    if _handle is not None:
        _handle.close()
        _handle = None
        return
    if not _verbose:
        for line in _pending:
            _echo(line)
    _pending = []


def _echo(line: str) -> None:
    """Write one line to stderr; a dead stderr must never abort the run."""
    with suppress(OSError):
        sys.stderr.write(line)
