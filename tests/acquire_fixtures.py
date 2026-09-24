# pattern: Imperative Shell
"""Explicitly download pinned development fixtures into the ignored cache."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
CACHE = ROOT / ".parity-fixtures"
MANIFEST = Path(__file__).with_name("fixtures_manifest.json")


def acquire() -> None:
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    CACHE.mkdir(parents=True, exist_ok=True)
    for name, fixture in manifest["fixtures"].items():
        expected = fixture["sha256"]
        if expected.startswith("RECORD_"):
            raise RuntimeError(f"record SHA-256 for {name} in tests/fixtures_manifest.json first")
        target = CACHE / name
        if target.exists() and hashlib.sha256(target.read_bytes()).hexdigest() == expected:
            continue
        request = Request(
            fixture["url"], headers={"User-Agent": "sentinel-parity-fixture-acquirer"}
        )
        with urlopen(request, timeout=60) as response:
            data = response.read()
        actual = hashlib.sha256(data).hexdigest()
        if actual != expected:
            raise RuntimeError(f"fixture integrity mismatch for {name}: got SHA-256 {actual}")
        target.write_bytes(data)
        print(f"acquired {name} sha256={actual}")


if __name__ == "__main__":
    acquire()
