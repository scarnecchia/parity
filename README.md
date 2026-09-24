# Sentinel parity

`sentinel-parity` compares SAS7BDAT output with Parquet output in `dplocal` and `msoc` folders. It writes an offline HTML report, a JSON summary, and complete row-level JSONL details. It compares rows as duplicate-aware multisets; a detail `PASS` means one occurrence has an equal occurrence on the other side, not that original row numbers align.

## Install and run

Python 3.12.13 or newer is required. Use a virtual environment and pip:

```sh
python3 -m venv .venv
. .venv/bin/activate
python -m pip install sentinel-parity
parity run --sas_root /data/sas --python_root /data/parquet
```

The equivalent hyphenated options are `--sas-root` and `--python-root`. The package supports an explicit TOML file:

```toml
sas_root = "../sas-output"
python_root = "../parquet-output"
output_dir = "./parity-report"
memory_limit = "1GB"
max_temp_size = "10GB"
preview_rows = 100
```

Run it with `parity run --config config.toml`. There is no implicit config search. Relative paths in TOML resolve from that file's directory; relative command-line paths resolve from the current directory. Explicit CLI options override matching TOML values. `~` expands; environment variables are not interpolated.

Each input root must contain readable `dplocal` and `msoc` directories. Only immediate files are scanned. Dataset identity is the directory and case-folded filename stem; file extensions are case-insensitive. One-sided files fail the report. Existing nonempty output directories and output/temp locations inside an input root are rejected. Inputs are never modified.

The default output is `./parity-report`. Open `parity-report/index.html` directly; it has no network dependencies. For local preview, an optional server is `python -m http.server --bind 127.0.0.1 --directory parity-report`.

Exit status is 0 for all matched pairs passing, 1 for completed comparison failures, and 2 for invalid input, unsupported data, resource, or report errors. ERROR entries record only the exception class; the third-party message is omitted. Reader/database exception strings can include cell values, source paths, SQL fragments, or other implementation details, so exposing them could disclose data; use the exception class and dataset metadata to guide remediation. For `FileNotFoundError`/`PermissionError`, verify the input/temp paths and access; for `TypeError`, inspect schema/type support; for `OutOfMemoryException` or resource errors, raise configured limits or reduce workload; for `RuntimeError`, retain the redacted report and contact the maintainer. Never paste raw third-party exception text into shared logs or tickets.

## Equality and output details

Column names are case-folded and sorted before comparison; duplicate normalized names fail. Reader-decoded strings compare exactly, including any whitespace the reader preserves and empty strings. The pinned productsales SAS fixture yielded 1,440 COUNTRY values and none had trailing blanks, so it does not establish whether the reader trims source padding; a dedicated padded-string fixture was unavailable. Null is distinct from empty text and NaN. Finite int/float/Decimal values compare by exact reduced rational value, so `1` equals `1.0`, but an integer beyond binary64 exact range does not equal its rounded float. Binary values are represented in base64. Dates compare as days; naive timestamps and timezone-aware timestamps are distinct families. Reader precision limits still apply.

SAS numeric missing values `.`, `._`, and `.A` through `.Z` collapse to null according to polars-readstat's documented default behavior. The report records this policy. Equality is over reader-decoded values, not raw SAS bytes. This policy was verified with the pinned pyreadstat sample in this project’s development tests; production outputs are expected not to contain special missing values.

`summary.json` records file names, row counts, status, exact comparison counts, reader schema/metadata, DuckDB resource settings, package versions, and detail links. `details/*.jsonl` contains both sides' result for every readable row. Numeric envelopes preserve the original textual form and an exact canonical key. HTML previews are bounded by `preview_rows`; full results are only in JSONL. Reports may contain sensitive data: keep their directory protected and delete it using the data owner's retention procedure.

DuckDB memory and temp-space options constrain its execution limits, not total process RSS. Polars, Arrow, Python, and the OS also use memory/disk. No arbitrary dataset size is promised. Nested/list/struct/map types and some reader conversions are unsupported; use a clear ERROR rather than treating them as equal.

## Development and tests

External public test fixtures are acquired explicitly and cached under ignored `.parity-fixtures/`; they are excluded from distributions. The test runner never downloads them.

```sh
python3 -m venv .venv
TMPDIR="$PWD/.tmp" .venv/bin/python -m pip install -e '.[dev]'
.venv/bin/python -m tests.acquire_fixtures
.venv/bin/python -m pytest
.venv/bin/ruff check .
.venv/bin/python -m ruff format --check .
.venv/bin/mypy src
.venv/bin/python -m build
```

Tested development target: Python 3.14.7, Linux x86_64. Other operating systems and architectures are not validated.
