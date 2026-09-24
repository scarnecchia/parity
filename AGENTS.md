# Sentinel parity

Last verified: 2026-09-24

## Stack and commands

- Python >=3.12.13; Typer, polars-readstat, Polars, PyArrow, DuckDB, Jinja2; setuptools/pip packaging.
- Create the environment with `python3 -m venv .venv`; install with `TMPDIR="$PWD/.tmp" .venv/bin/python -m pip install -e '.[dev]'` (this host's `/tmp` is a full tmpfs).
- Run `python -m pytest`, `python -m ruff check .`, `python -m ruff format --check .`, `python -m mypy src`, and `python -m build` inside `.venv`.
- Real external test inputs are explicitly downloaded with `python -m tests.acquire_fixtures`; tests never download. Cache stays in ignored `.parity-fixtures/` and must never enter wheel or sdist.

## Architecture and invariants

- `core/` contains pure normalization, typed value encoding, comparison SQL generation, and report modeling. It does not import filesystem/database readers.
- `io/` and `runner.py` are imperative shells for TOML/filesystem access, polars-readstat/Arrow staging, DuckDB comparison, and report output. Keep each runtime source file classified by its `# pattern:` line.
- SAS and Parquet rows are compared as exact, order-independent multisets. Canonical equality keys are per-column; never replace them with hash-only equality or delimiter concatenation.
- Comparison is value-only: declared column types are never a factor, and a type difference alone can never fail a dataset. Decoded values match iff equal in one value space — numbers by reduced rational value (16.0 = 16; 16.2 != 16) with numeric-looking text coerced to numbers; booleans as 1/0; temporals as exact instants (naive as UTC, dates as midnight); null, NaN, and blank or whitespace-only text as one missing value; other text exactly after trailing-whitespace trim (SAS blank padding). Keep Int64 ordinals/counts and stream rows in bounded batches.
- Unmatched rows are paired by occurrence rank and each detail record carries `differing_columns` so the failing value is attributable; pairs are diagnostics, not proof of row correspondence.
- Reports may carry sensitive data; never place cell values in logs or console errors. Publish generated artifacts atomically, keep output/temp outside inputs, and clean owned temp data on failure/interruption.

## Development guidance

- Use `coding-effectively`, `howto-functional-vs-imperative`, `howto-code-in-python`, `writing-code-comments`, and `writing-good-tests` for source/test changes.
- Read this file again against current behavior before changing cross-module contracts; update `Last verified` with the actual system date when those contracts change.
- Implementation model: `codex/gpt-6-luna(high)`. Independent review models required by the approved plan: `deepinfra/deepseek-v4.1-flash(high)` and `zai/glm-5.3-flash(high)`; the coordinator dispatches reviews.
- Deliver completed, verified work by committing and pushing directly to `origin`/`main` as part of normal delivery; no separate authorization needed. Keep commits atomic with green checks (pytest, ruff, mypy). Do not redistribute or package public test fixture binaries or CSV data.
