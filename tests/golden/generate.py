"""Regenerate golden parity captures under .tmp/golden (never committed).

Run only with the implementation whose behavior should become the parity
definition; the output files are a local development reference for diffing
refactors (generated test data stays out of the repository).  The committed,
permanent gate is the builder-vs-scalar property tests plus the
independent-reference detail tests.

    .venv/bin/python tests/golden/generate.py

Pass --dev to additionally capture fixture-derived data (productsales,
datetime, dates_null staging grids and a full productsales detail stream);
those carry fixture data and stay out of the repository too.
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
import tempfile
from pathlib import Path

TESTS = Path(__file__).resolve().parents[1]
if str(TESTS) not in sys.path:
    sys.path.insert(0, str(TESTS))

import adversarial  # noqa: E402
import polars_readstat  # noqa: E402

from sentinel_parity.config import RunConfig  # noqa: E402
from sentinel_parity.runner import run  # noqa: E402

GOLDEN = TESTS.parent / ".tmp" / "golden"
FIXTURES = TESTS.parent / ".parity-fixtures"


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=1), encoding="utf-8")
    print(f"wrote {path} ({path.stat().st_size} bytes)")


def _golden_captures(work: Path) -> None:
    paths = {variant: work / f"adversarial_{variant}.parquet" for variant in ("a", "b")}
    for variant, path in paths.items():
        adversarial.write_adversarial(path, variant)
    empty_path = work / "adversarial_empty.parquet"
    adversarial.write_adversarial(empty_path, "empty")
    disjoint_path = work / "disjoint.parquet"
    adversarial.write_disjoint(disjoint_path)

    cases = {
        "base_default": adversarial.stage_grid(paths["a"], work / "grid-a"),
        "base_round2": adversarial.stage_grid(paths["a"], work / "grid-a-r2", round_digits=2),
        "base_multibatch": adversarial.stage_grid(paths["a"], work / "grid-a-multi", batch_size=7),
        "perturbed_default": adversarial.stage_grid(paths["b"], work / "grid-b"),
        "empty_default": adversarial.stage_grid(empty_path, work / "grid-empty"),
    }
    _write_json(GOLDEN / "adversarial_stage.json", {"version": 1, "cases": cases})

    captures = {
        "shared": adversarial.comparison_capture(
            paths["a"], paths["b"], "goldenadv", work / "cmp-shared"
        ),
        "absent": adversarial.comparison_capture(
            paths["a"], disjoint_path, "goldenabsent", work / "cmp-absent"
        ),
        "identical": adversarial.comparison_capture(
            paths["a"], paths["a"], "goldenident", work / "cmp-identical"
        ),
    }
    _write_json(GOLDEN / "adversarial_details.json", {"version": 1, **captures})


def _dev_captures(work: Path) -> None:
    destination = TESTS.parent / ".tmp" / "golden-dev"
    destination.mkdir(parents=True, exist_ok=True)
    grids = {}
    for name, stem, round_digits in (
        ("productsales_default", "productsales", None),
        ("productsales_round2", "productsales", 2),
        ("datetime_default", "datetime", None),
        ("dates_null_default", "dates_null", None),
    ):
        sas = FIXTURES / f"{stem}.sas7bdat"
        grids[name] = adversarial.stage_grid(
            sas, work / f"dev-{name}", kind="sas", round_digits=round_digits
        )
    _write_json(destination / "fixture_stage.json", {"version": 1, "cases": grids})

    sas_root, python_root = work / "dev-sas", work / "dev-python"
    for root in (sas_root, python_root):
        for sub in ("dplocal", "msoc"):
            (root / sub).mkdir(parents=True, exist_ok=True)
    shutil.copyfile(FIXTURES / "productsales.sas7bdat", sas_root / "dplocal" / "data.sas7bdat")
    polars_readstat.ScanReadstat(
        str(sas_root / "dplocal" / "data.sas7bdat")
    ).df.collect().write_parquet(python_root / "dplocal" / "data.parquet")
    output = work / "dev-out"
    assert run(RunConfig(sas_root, python_root, output)) == 0
    summary = json.loads((output / "summary.json").read_text())
    dataset = summary["datasets"][0]
    detail = output / summary["detail_links"][dataset["id"]]
    path = destination / "productsales_details.jsonl"
    path.write_text(detail.read_text(encoding="utf-8"), encoding="utf-8")
    print(f"wrote {path} ({path.stat().st_size} bytes, {dataset['matched_pairs']} matched pairs)")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dev", action="store_true", help="also capture fixture-derived data")
    args = parser.parse_args()
    with tempfile.TemporaryDirectory() as raw:
        work = Path(raw)
        _golden_captures(work)
        if args.dev:
            _dev_captures(work)


if __name__ == "__main__":
    main()
