# parity

`parity` compares SAS7BDAT outputs with Parquet outputs under `dplocal` and `msoc` roots. It writes an offline HTML report, a JSON summary, and row-level JSONL details. Comparison is order-independent and duplicate-aware: a detail `PASS` means one row occurrence matched an equal occurrence on the other side, not that original row numbers align.

## Install

Requires Python >=3.12.13. Clone this repository and install in a virtual environment:

```sh
git clone https://github.com/scarnecchia/parity.git
cd parity
python3 -m venv .venv
. .venv/bin/activate
python -m pip install .
```

The distribution is named `sentinel-parity`; the console command is `parity`.

## Run

Provide both input roots directly:

```sh
parity run --sas-root /data/sas --python-root /data/parquet
```

`-s` and `-p` are short forms of `--sas-root` and `--python-root`; `-o` shortens `--output-dir`. Each root must contain `dplocal/` and `msoc/` directories.

Or copy [config.toml.example](config.toml.example) to `config.toml`, edit its paths, and run:

```sh
parity run --config config.toml
```

To override one setting from the file:

```sh
parity run --config config.toml --output-dir ./another-report
```

Open `parity-report/index.html` directly in a browser. The HTML report has no network dependencies.

## Options reference

| CLI option | TOML key | Type and meaning | Default |
| --- | --- | --- | --- |
| `--config` | None | Path to an explicit TOML configuration file | Unset; no config file loaded |
| `--sas-root`, `-s` | `sas_root` | String path to the SAS output root | Required; no default |
| `--python-root`, `-p` | `python_root` | String path to the Parquet output root | Required; no default |
| `--output-dir`, `-o` | `output_dir` | String path to the report directory | `./parity-report` |
| `--id` | `id` | Nonempty string of letters, numbers, and underscores, at most 64 characters; the reserved report name `details` is rejected; writes the report into an `<id>` subfolder of the report directory | Unset; report goes directly to the report directory |
| `--memory-limit` | `memory_limit` | Nonempty DuckDB memory-limit string, such as `"1GB"` or `"512MB"` | `"1GB"` |
| `--temp-dir` | `temp_dir` | String path to an existing base directory for the run's private temporary directory | Unset; Python's system temporary-directory selection, including `TMPDIR` |
| `--max-temp-size` | `max_temp_size` | Nonempty DuckDB maximum temporary-directory-size string, such as `"10GB"` | `"10GB"` |
| `--preview-rows` | `preview_rows` | Nonnegative integer: maximum mismatch preview rows per side per dataset in HTML; `0` hides preview rows | `100` |
| `--round` | `round_digits` | Nonnegative integer: round all numeric values — including coerced numeric text — to N digits after the decimal point (nearest, ties away from zero) before comparison | Omitted; raw values compare |
| `--threads` | `threads` | Positive integer: DuckDB worker threads for comparison | `4` |
| `--verbose` | None | Flag: echo structured run-log events to stderr as they happen | Not set |
| `--help` | None | Flag: show help and exit (`parity --help` or `parity run --help`) | Not set |

CLI values override TOML values **per key**. Required roots may come from either source. TOML keys are top-level; unknown keys are rejected.

Relative paths explicitly set in TOML resolve from the config file's directory. Relative CLI paths, including `--config`, resolve from the current working directory. If `output_dir` is omitted everywhere, its default resolves from the working directory, even when a config file is supplied. `~` expands in paths; environment variables are not interpolated. There is no implicit config discovery.

DuckDB validates the resource-limit strings. These settings limit DuckDB execution, not total process memory or all temporary disk use: Polars, Arrow, Python, and the OS use additional resources. The private run directory is cleaned up when the run finishes or unwinds after an error or handled interruption.

## Dataset discovery and pairing

- Both roots must contain readable directories named `dplocal` and `msoc`.
- Only immediate files are scanned, not nested directories. SAS inputs use `.sas7bdat`; Python inputs use `.parquet`. Extensions are case-insensitive.
- Dataset identity is the subdirectory plus the case-folded filename stem; one leading `r` plus two digits and an underscore is ignored for pairing on either side. For example, `dplocal/People.SAS7BDAT` pairs with `dplocal/people.parquet`, not `msoc/people.parquet`.
- Duplicate identities on either side are rejected. Matching-extension symlinks and non-file entries are rejected.
- A one-sided file produces a `FAIL` dataset if readable. If there are no matched dataset pairs anywhere across the two subdirectories, the run is a configuration error instead.
- Inputs are never modified. Output and configured temporary locations cannot equal or be inside either input root. The report directory — `output_dir`, or `output_dir`/`<id>` when `id` is set — must be absent or empty; existing reports are not overwritten. Concurrent runs writing the same report directory are unsupported.

## Interpret the results

### Exit codes

| Code | Meaning |
| --- | --- |
| `0` | All discovered datasets pass — including datasets that warned only on character-vs-numeric column differences. |
| `1` | Comparisons completed, but at least one dataset failed: one-sided file, schema/family mismatch, or unmatched row occurrences. |
| `2` | Configuration, input, unsupported-data, resource, report-writing, or interruption error. Dataset errors produce overall `ERROR` when a report can be written. |

Errors take precedence over comparison failures: `ERROR` / `2` overrides `FAIL` / `1`, which overrides `WARN` / `PASS` / `0`. A configuration or report-writing error may leave no complete report.

A completed comparison reports `WARN` instead of `FAIL` when every unmatched occurrence is explained by a character-vs-numeric column (one side numeric, the other text) — remaining differences like unparseable text in an otherwise numeric column. The dataset entry lists those columns in `type_mismatched_columns`, and `index.html` notes them at the top of the dataset section.

### Report files

```text
parity-report/
  index.html
  summary.json
  run.jsonl
  details/
    <id>.jsonl
```

With `id` set, these files are written into `parity-report/<id>/` so one report directory can hold several runs side by side.

`run.jsonl` is the structured run log: one JSON object per event with counts, durations, paths, and per-phase comparison timings; failures record the exception class and, for `OSError`, the errno and strerror. It never contains cell values, preview rows, or raw exception text. `--verbose` echoes the same lines to stderr as they happen, and a run that fails before the report directory exists drains its buffered events to stderr. A failed run deliberately keeps `run.jsonl`, so the report directory is then not empty: rerunning into it requires deleting the directory — including the log — by hand. The tool never removes the log; it is the forensic record of the failure.

`index.html` contains dataset summaries and a **mismatch-only, bounded preview**, with links to the complete JSONL details. The preview limit applies separately to each side of each dataset; truncation counts show how much was omitted. Passing rows are in JSONL, not the mismatch preview.

`summary.json` includes the schema version, overall status, the request id as `request_id` (`null` when `id` is unset), package versions, execution limits, dataset entries, preview truncation counts, and `detail_links` mapping dataset IDs to relative JSONL paths. Dataset entries include names, status, reason, `conditions`, row counts, `matched_pairs`, `sas_only`, `python_only`, `row_order_mismatches`, and `detail_complete`. `row_order_mismatches` counts matched rows whose file positions differ — the comparison is order-independent, so reordered files still pass, and `index.html` flags this at the top of the dataset section. Available metadata includes reader schemas and metadata, original filenames, and original/normalized describe output for successfully compared pairs. Fields and metadata available for one-sided or error entries differ from successful pairs.

`matched_pairs` counts equal occurrence pairs, not individual detail lines. Each matched pair contributes two `PASS` lines, one per side. `sas_only` and `python_only` count unmatched occurrences. For a completed comparison, each side's row count equals `matched_pairs` plus its unmatched count.

`missing_counterpart` marks every readable row as `FAIL` without attempting occurrence matching: the dataset exists on only one side.

A successfully compared dataset carries `conditions`: every problem found, not just the first. Comparison always runs on the columns both sides share, so a schema delta never stops the row checks.

| Condition | Severity | Meaning |
| --- | --- | --- |
| `sas_only_columns` | FAIL | Parquet lacks columns present in SAS, so no row can fully match. |
| `python_only_columns` | WARN | Parquet has extra columns SAS lacks; their values are not compared. |
| `value_mismatch` | FAIL | Unmatched rows differ in shared columns beyond character-vs-numeric coercion. |
| `type_mismatch` | WARN | Every difference is inside character-vs-numeric columns, so the dataset passes. |

A dataset FAILs when any condition fails, WARNs when only warnings remain (which does not fail the run), and PASSes with none. `reason` repeats the most severe condition's code (`null` when clean).

An `ERROR` dataset has `detail_complete: false`: do not interpret its zero counts as evidence of an empty input or expect complete JSONL details. Its reason is only the exception class, such as `TypeError` or `PermissionError`. Exception messages are redacted because reader/database errors can disclose cell values, paths, or SQL. Check type support, path permissions, and resource settings as appropriate; do not paste raw third-party errors into shared logs or tickets.

### JSONL details

For a dataset with `detail_complete: true`, JSONL contains one line per row on each available side, including passing rows. Each line has:

| Field | Meaning |
| --- | --- |
| `schema_version` | Detail format version, currently `1`. |
| `dataset_id` | Dataset ID used in the summary and detail links. |
| `side` | `"sas"` or `"python"`. |
| `staging_row_number` | Zero-based row ordinal on that side; not a cross-side row alignment. |
| `status` | `"PASS"` or `"FAIL"`. |
| `reason` | `null` for a matched occurrence; `only_sas` / `only_python` for unmatched occurrences, or an all-row reason above. |
| `values` | Map of case-folded column names to typed values. |

Non-null cells use envelopes with `type` and `value`. Numeric envelopes also carry an exact `canonical` key; numeric `value` is a string preserving the decoded value's textual form. Strings and Booleans retain their JSON value types, binary values use base64 strings, and dates/timestamps use ISO-formatted strings. A null cell is JSON `null`. A column that exists on one side only — schema deltas — renders as `{"type":"absent_column"}` on the other side, distinct from a present column containing null.

### Equality rules

Columns are case-folded and sorted before comparison; duplicate normalized column names are errors. Row order and original column order do not affect equality. Duplicate rows match by occurrence: three equal rows on one side and two on the other yield two matched pairs and one unmatched row.

Finite integers, floats, and decimals compare by exact reduced rational value, without a tolerance. Thus `1` equals `1.0`, but an integer does not equal a rounded float with a different exact value. Booleans are a separate family. Decoded text compares exactly, including preserved whitespace and empty strings; null, empty text, and NaN are distinct.

Dates are a separate family from timestamps. Naive and timezone-aware timestamps are distinct families; aware timestamps compare by UTC instant. Timestamp comparison preserves decoded nanosecond precision, subject to the reader's precision. Nested/list/struct/map types and other unsupported types or reader conversions produce `ERROR`, not equality.

## Protect report data

HTML previews and JSONL details can contain sensitive cell values; summaries can contain source metadata. Keep the entire report directory protected, share it only with authorized recipients, and delete it according to the data owner's retention procedure.

## Development and tests

From the repository root, create a virtual environment and install development dependencies. External fixtures must be acquired explicitly; tests never download them. Fixtures are cached in ignored `.parity-fixtures/` and excluded from distributions.

```sh
python3 -m venv .venv
mkdir -p .tmp
TMPDIR="$PWD/.tmp" .venv/bin/python -m pip install -e '.[dev]'
. .venv/bin/activate
export TMPDIR="$PWD/.tmp"
python -m tests.acquire_fixtures
python -m pytest
python -m ruff check .
python -m ruff format --check .
python -m mypy src
python -m build
```

Tested platform: Python 3.14.7, Linux x86_64. Nothing else has been validated.
