# parity

`parity` compares SAS7BDAT and Parquet outputs for users who need to check dataset agreement. It writes an offline HTML report, a JSON summary, complete differences-only JSONL details, and an Excel workbook when export limits permit. Comparison ignores row order and accounts for duplicate occurrences.

## Install and run

Use Python >=3.12.13 and Git. These commands use a POSIX shell. Validation covers Python 3.14.7 on Linux x86_64 only, not other platforms.

1. Open a terminal and download the source:

   ```sh
   git clone https://github.com/scarnecchia/parity.git
   cd parity
   ```

2. Create and activate a virtual environment, which keeps dependencies separate from other Python installations:

   ```sh
   python3 -m venv .venv
   . .venv/bin/activate
   ```

3. Create a local temporary directory and install the package:

   ```sh
   mkdir -p .tmp
   TMPDIR="$PWD/.tmp" .venv/bin/python -m pip install -e .
   ```

   The local directory avoids a full RAM-backed `/tmp` (`tmpfs`) during installation. Runtime dependencies include `XlsxWriter>=3.2,<4` for Excel export.

4. Run a comparison with your two input directories:

   ```sh
   parity run --sas-root /data/sas --python-root /data/parquet
   ```

   Replace `/data/sas` and `/data/parquet` with your paths. Each root must contain `dplocal/` and `msoc/` directories. At least one SAS/Parquet dataset pair must exist.

5. Open `parity-report/index.html` in a browser to inspect dataset statuses and differences.

   The command exits when the report is ready. Exit code `1` means the comparison completed but found failures. See [Exit codes](#exit-codes) for other outcomes.

The distribution is named `sentinel-parity`. The console command is `parity`. Use `-s`, `-p`, and `-o` as short forms of `--sas-root`, `--python-root`, and `--output-dir`.

Or copy [config.toml.example](config.toml.example) to `config.toml`, edit its paths, and run:

```sh
parity run --config config.toml
```

To override one setting from the file:

```sh
parity run --config config.toml --output-dir ./another-report
```

Open `parity-report/index.html` directly in a browser. The HTML report has no network dependencies.

## Developer setup

Use the source checkout and virtual environment from [Install and run](#install-and-run). Install the development extras from the repository root:

```sh
TMPDIR="$PWD/.tmp" .venv/bin/python -m pip install -e '.[dev]'
export TMPDIR="$PWD/.tmp"
```

Download external test fixtures explicitly before the full test suite:

```sh
python -m tests.acquire_fixtures
```

Tests never download fixtures automatically. The ignored `.parity-fixtures/` directory stores the cache. Distributions exclude these files.

Run the checks and build:

```sh
python -m pytest
python -m ruff check .
python -m ruff format --check .
python -m mypy src
python -m build
```

Use `parity run` from [Install and run](#install-and-run) to test local changes. No database service or web server is required.

The separate browser suite requires Node.js, Playwright, and Chromium. Install its dependencies explicitly:

```sh
npm install --no-save playwright
npx playwright install chromium
node tests/browser/test_report_narrow_viewport.mjs
```

It checks local HTML at 375px and 1280px widths without a server. Run the synthetic resource harness separately:

```sh
python -m tests.resource_harness
python -m tests.resource_harness --experiment
```

The default harness measures 10,000-row and 50,000-row pairs. The opt-in experiment measures 500,000 rows per side with 50,000 mismatches. It reports peak process memory, detail bytes, staged bytes, and elapsed time. These measurements are diagnostics, not portable resource guarantees. See [Development results](DEVELOPMENT-RESULTS.md) for recorded checks and limitations.

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
| `--preview-rows` | `preview_rows` | Nonnegative integer: maximum flat difference rows per dataset in HTML. `0` hides preview rows | `100` |
| `--preview-max-bytes` | `preview_max_bytes` | Positive integer: rendered preview-row bytes per dataset | `1048576` (1 MiB) |
| `--preview-total-max-bytes` | `preview_total_max_bytes` | Positive integer: rendered preview-row bytes per run | `10485760` (10 MiB) |
| `--preview-cell-chars` | `preview_cell_chars` | Positive integer: preview characters per cell | `512` |
| `--pair-keys` / `--no-pair-keys` | `pair_keys` | `--pair-keys`: comma-separated shared column names that order the diagnostic excess-row pairing first; remaining shared columns only break ties; an empty value falls back to TOML. `--no-pair-keys` overrides both the TOML value and `--pair-keys` for this run. Never changes pass/fail | Unset; excess rows pair by ascending value order across all shared columns |
| `--excel` / `--no-excel` | `excel` | Boolean: enable automatic workbook export | `true` |
| `--excel-max-sheets` | `excel_max_sheets` | Positive integer: workbook sheets, including Index | `100` |
| `--excel-max-rows` | `excel_max_rows` | Positive integer: flat difference rows per workbook | `100000` |
| `--excel-max-rows-per-sheet` | `excel_max_rows_per_sheet` | Positive integer: flat difference rows per dataset sheet | `25000` |
| `--excel-max-bytes` | `excel_max_bytes` | Positive integer: projected uncompressed cell text bytes, including value cells, type labels, headers, and Index | `104857600` (100 MiB) |
| `--round`, `--round-digits` | `round_digits` | Positive integer: round numeric values, including coerced numeric text, to N decimal places before comparison. Ties round away from zero | Omitted; raw values compare |
| `--threads` | `threads` | Positive integer: DuckDB worker threads for comparison | `4` |
| `--verbose` | None | Flag: echo structured run-log events to stderr as they happen | Not set |
| `--help` | None | Flag: show help and exit (`parity --help` or `parity run --help`) | Not set |

CLI values override TOML values **per key**. Required roots may come from either source. TOML keys are top-level; unknown keys are rejected.

Relative paths explicitly set in TOML resolve from the config file's directory. Relative CLI paths, including `--config`, resolve from the current working directory. If `output_dir` is omitted everywhere, its default resolves from the working directory, even when a config file is supplied. `~` expands in paths; environment variables are not interpolated. There is no implicit config discovery.

DuckDB validates the resource-limit strings. These settings limit DuckDB execution, not total process memory or all temporary disk use: Polars, Arrow, Python, and the OS use additional resources. The private run directory is cleaned up when the run finishes or unwinds after an error or handled interruption.

## Large runs

- Place the run temp on real disk: `--temp-dir /path/on/disk`. If the run temp resolves onto a `tmpfs` mount (RAM-backed, common for `/tmp`), the run logs a `temp_on_tmpfs` event and prints a warning, because DuckDB spill files then consume RAM instead of disk. The run continues either way.
- Size `memory_limit` to roughly 50–60% of machine RAM for large datasets (for example `"32GB"` on a 62 GB host); comparison joins spill to the temp directory beyond that. Raise `--max-temp-size` so the spill has room.
- `--threads` scales DuckDB comparison work; 4 is a conservative default, and 8–16 helps on many-core hosts with the memory to match.

## Dataset discovery and pairing

- Both roots must contain readable directories named `dplocal` and `msoc`.
- Only immediate files are scanned, not nested directories. SAS inputs use `.sas7bdat`; Python inputs use `.parquet`. Extensions are case-insensitive.
- Dataset identity is the subdirectory plus the case-folded filename stem; one leading `r` plus two digits and an underscore is ignored for pairing on either side. For example, `dplocal/People.SAS7BDAT` pairs with `dplocal/people.parquet`, not `msoc/people.parquet`.
- Duplicate identities on either side are rejected. Matching-extension symlinks and non-file entries are rejected.
- A one-sided file produces a `FAIL` dataset without opening the file or checking its contents. If neither subdirectory contains a matched dataset pair, the run is a configuration error instead.
- Inputs are never modified. Output and configured temporary locations cannot equal or be inside either input root. The report directory — `output_dir`, or `output_dir`/`<id>` when `id` is set — must be absent or empty; existing reports are not overwritten. Concurrent runs writing the same report directory are unsupported.

## Interpret the results

### Exit codes

| Code | Meaning |
| --- | --- |
| `0` | All discovered datasets have status `PASS` or `WARN`. Warnings include extra Parquet columns and character-vs-numeric differences. |
| `1` | Comparisons completed, but at least one dataset failed: missing counterpart, missing SAS columns in Parquet, or shared-value mismatches. |
| `2` | Configuration, input, unsupported-data, resource, report-writing, or interruption error. Dataset errors produce overall `ERROR` when a report can be written. |

Errors take precedence over comparison failures: `ERROR` / `2` overrides `FAIL` / `1`, which overrides `WARN` / `PASS` / `0`. A configuration or report-writing error may leave no complete report.

Shared-value differences produce a `type_mismatch` warning when unmatched counts are equal and every difference falls within character-vs-numeric columns. These columns have numeric values on one side and text on the other, such as unparseable text in an otherwise numeric column. Other conditions can still make the dataset fail. The dataset entry lists these columns in `type_mismatched_columns`. The HTML report identifies them at the top of the dataset section.

### Report files

```text
parity-report/
  index.html
  summary.json
  run.jsonl
  differences.xlsx  (when generated)
  details/
    <id>.jsonl
```

With `id` set, these files are written into `parity-report/<id>/` so one report directory can hold several runs side by side.

`run.jsonl` is the structured run log: one JSON object per event with counts, durations, paths, and per-phase comparison timings; failures record the exception class and, for `OSError`, the errno and strerror. It never contains cell values, preview rows, or raw exception text. `--verbose` echoes the same lines to stderr as they happen, and a run that fails before the report directory exists drains its buffered events to stderr. A failed run deliberately keeps `run.jsonl`, so the report directory is then not empty: rerunning into it requires deleting the directory — including the log — by hand. The tool never removes the log; it is the forensic record of the failure.

`index.html` flags files without an equivalent at the top. Each dataset has a flat, side-by-side difference table and a link to its complete JSONL details. Each table row represents one differing column, not one source row. The report shows preview counts, clipped-cell indicators, and reasons for omitted rows. It also links to the workbook or explains why no workbook was generated.

Preview row limits apply per dataset, not per side. SQL limits the fetched rows and clips cell text before Python receives it. Byte budgets apply during rendering. They count UTF-8 bytes in table-row fragments, including escaped text and markup, but exclude final indentation and the rest of the HTML. Cell clipping changes only the preview. Complete values remain in JSONL. Matching rows appear in neither the preview nor JSONL.

The preview and workbook mark blank or whitespace-only text with `(compares as missing)`. This uses the same whitespace rule as comparison: Python `str.rstrip()` leaves an empty string. The test uses the full value, so preview clipping cannot change its meaning.

`summary.json` includes the schema version, overall status, the request id as `request_id` (`null` when `id` is unset), package versions, execution limits, dataset entries, preview truncation counts, and `detail_links` mapping dataset IDs to relative JSONL paths. Dataset entries include names, status, reason, `conditions`, row counts, `matched_pairs`, `sas_only`, `python_only`, `row_order_mismatches`, the effective `pair_keys` used for diagnostic pairing, and `detail_complete`. `row_order_mismatches` counts matched rows whose file positions differ — the comparison is order-independent, so reordered files still pass, and `index.html` flags this at the top of the dataset section. Available metadata includes reader schemas and metadata, original filenames, and original/normalized describe output for successfully compared pairs. One-sided entries never open their file, so the existing side's row counts are `null` and they carry no detail link, preview, or reader metadata; error entries differ from successful pairs.

**Breaking format change:** `summary.json` now has `schema_version: 3` and `details_schema_version: 2`. Update consumers of the previous full-row JSONL format.

Summary fields include:

| Field | Meaning |
| --- | --- |
| `difference_row_count` | Number of entries across all JSONL `differences` arrays, not the number of JSONL records. Available per dataset and per run. |
| `details_bytes` | Complete detail-file bytes, per dataset and per run. |
| `preview_truncation` | Per-dataset `{shown_rows, omitted_rows, rendered_bytes, reasons}`. Reasons include `row_limit`, `dataset_byte_limit`, and `run_byte_limit`. |
| `excel` | Run-level `{status, path, reasons, limits, measurements}`. Status is `generated`, `disabled`, `no_differences`, or `omitted`. `limits` records configured budgets. `measurements` contains `rows`, `text_bytes`, and `max_cell_chars`. |

Excel row measurements cover datasets staged while the workbook was still eligible, even if the runner later deletes their staging files. Text-byte and maximum-cell measurements also include Index text for every dataset. Datasets compared after omission report zero `workbook_measurements`. These measurements do not describe all differences when the workbook is omitted. The summary does not expose temporary `workbook_stage_path` values.

For complete details, `shown_rows + omitted_rows` equals `difference_row_count`. The top-level `preview_truncation` maps dataset IDs to these measurements. Each dataset entry also contains its measurements.

`matched_pairs` counts equal occurrence pairs. Matched pairs produce no JSONL records. `sas_only` and `python_only` count unmatched occurrences before diagnostic pairing. For a completed comparison, each side's row count equals `matched_pairs` plus its unmatched count.

### Excel workbook

Excel export is automatic. Disable it with `excel = false` in TOML or `--no-excel` on the command line.

`differences.xlsx` contains an Index sheet with every dataset, its status, conditions, and dataset-to-sheet mapping. Each dataset with cell differences gets a separate sheet. Schema-only conditions and missing counterparts can produce an Index-only workbook. A clean run with no differences needs no workbook.

The workbook is all-or-nothing. Any exceeded sheet, row, byte, or Excel cell limit omits the entire workbook. Complete JSONL remains available. The summary records explicit omission reasons as `{code, dataset_id}` objects, and `index.html` states each reason in plain language — including the raising option and its configured value — naming the dataset for per-dataset breaches. Run-level reasons use a null dataset ID. The workbook path is null unless its status is `generated`.

The byte budget measures projected uncompressed cell text: staged value strings, type labels, and headers, plus Index text measured in the runner. It does not predict compressed XLSX size or total temporary disk use. Hard caps apply even when configured budgets are larger: 100 sheets including Index, and 1,048,575 difference rows per workbook or dataset sheet. Cells over 32,767 characters cause omission rather than truncation.

The runner stages per-dataset Parquet files only while the workbook remains eligible. When it becomes ineligible, the runner deletes those files and skips further workbook staging. XlsxWriter writes eligible workbooks in constant-memory mode. The writer finalizes the workbook before atomic publication.

Source values and Int64 pair/row identifiers remain text to prevent numeric precision loss. Source text cannot become formulas or hyperlinks. Workbook temporary XML can exceed the final compressed file size. DuckDB memory and spill limits do not cap workbook temporary files or total process memory.

### Dataset conditions

`missing_counterpart` marks a file with no equivalent on the other side. The file is never opened — no row counts, reader metadata, or JSONL details — and `index.html` lists it under **Files without an equivalent** at the top of the report.

A compared dataset carries `conditions`: every problem found, not just the first. Comparison always runs on the columns both sides share, so a schema delta never stops the row checks; a one-sided entry carries exactly one condition, `missing_counterpart`.

| Condition | Severity | Meaning |
| --- | --- | --- |
| `missing_counterpart` | FAIL | The file has no equivalent on the other side; the file is never read. |
| `sas_only_columns` | FAIL | Parquet lacks columns present in SAS. Shared-column values can still match. |
| `python_only_columns` | WARN | Parquet has extra columns SAS lacks; their values are not compared. |
| `value_mismatch` | FAIL | Unmatched counts differ, or paired rows differ outside character-vs-numeric columns. |
| `type_mismatch` | WARN | Unmatched counts are equal, and every shared-value difference is inside character-vs-numeric columns. Other conditions can still make the dataset fail. When declared `pair_keys` are set and their pairing also pairs rows differing outside those columns, the condition carries `pairing_note` restating that the pass verdict comes from the automatic pairing. |

A dataset FAILs when any condition fails, WARNs when only warnings remain (which does not fail the run), and PASSes with none. `reason` repeats the most severe condition's code (`null` when clean).

An `ERROR` dataset has `detail_complete: false`: do not interpret its zero counts as evidence of an empty input or expect complete JSONL details. Its reason is only the exception class, such as `TypeError` or `PermissionError`. The run log records the same class name — plus errno and strerror for `OSError` — and never raw exception text, because reader and database errors can disclose cell values or paths. Dataset entries in `summary.json` and `index.html` do include the exception message as `error_message`; reports may contain sensitive data, so store and share them accordingly. Check type support, path permissions, and resource settings as appropriate; do not paste raw third-party errors into shared logs or tickets.

### JSONL details

JSONL v2 contains nested paired differences only. It has no full-row match records and no `PASS` records. A complete empty file is valid for a passing dataset or a dataset with schema-only differences.

| Field | Meaning |
| --- | --- |
| `schema_version` | Detail format version, `2`. |
| `dataset_id` | Dataset ID used in the summary and detail links. |
| `pair_id` | Diagnostic excess-row rank ordered by canonical values across all shared columns. Null when the datasets have no shared columns. |
| `kind` | `paired_mismatch`, `only_sas`, or `only_python`. |
| `sas_row`, `python_row` | Original zero-based staging ordinals. Null means that row is absent. |
| `differences` | Array of `{column, sas, python}` entries with original typed cell payloads, ordered by column name. This array identifies which columns differ. |

A paired record includes only differing shared columns. A one-sided excess row includes each available column once, with null on the absent side. Datasets with no shared columns produce separate one-sided records, not fabricated pairs. Schema differences remain in summary and report metadata. JSONL v2 has no `absent_column` envelopes.

Excess rows receive diagnostic pair ranks by ascending, null-safe canonical values — declared `pair_keys` first when set, then the remaining shared columns — while `sas_row` and `python_row` retain original staging ordinals. Pairs diagnose differences; they do not establish business-key correspondence between source rows: when a pairing key's value itself differs (a flipped `group`, for example), the rows cannot align and the pair spans different entities, which the report outlines. Declare stable identity columns rather than columns whose values may be corrupted for the most meaningful pairing. JSONL record order is nondeterministic. Consumers must not rely on file order.

Non-null cells use typed envelopes. Numeric envelopes include an exact `canonical` key and preserve the decoded value as text. Binary values use base64, and dates/timestamps use ISO-formatted text. NaN and blank-text payloads retain their original representation even when comparison treats them as missing.

A present null cell is JSON `null`. Top-level `sas_row` or `python_row` null means an absent row instead. Use these row fields and `kind` to distinguish absence from a null cell. Both cell entries can be null for a one-sided null cell.

To flatten records, emit one row for each `differences` entry. Copy the record's dataset, pair, kind, and row fields into each emitted row. For example, a record with differences for `amount` and `date` produces two flat rows. This is how HTML and Excel count difference rows.

### Equality rules

Columns are case-folded and sorted before comparison; duplicate normalized column names are errors. Row order and original column order do not affect equality. Duplicate rows match by occurrence: three equal rows on one side and two on the other yield two matched pairs and one unmatched row.

Comparison uses decoded values, not declared column types. Finite numbers compare by exact reduced rational value, without a tolerance. Numeric-looking text compares as numbers, and booleans compare as `1` or `0`. Thus `16.0` equals `16`, but `16.2` does not equal `16`. `--round N` rounds numeric values before comparison, with ties away from zero.

Null, NaN, and blank or whitespace-only text share one missing-value meaning. Other text compares exactly after trailing-whitespace removal for SAS blank padding.

Dates and timestamps compare as exact instants. Dates mean midnight, and naive timestamps use UTC. Timestamp comparison preserves decoded nanosecond precision, subject to the reader's precision. Nested/list/struct/map types and other unsupported types or reader conversions produce `ERROR`, not equality.

## Protect report data

HTML previews, JSONL details, and Excel workbooks can contain sensitive cell values. Summaries can contain source metadata. Protect the entire report directory. Share it only with authorized recipients. Delete it according to the data owner's retention procedure.
