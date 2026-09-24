# Development results

Status: **all requested acceptance evidence except the operator-waived original special-missing fixture is implemented and verified on the tested platform**. Final full gates and targeted acceptance tests pass. No Git initialization, commit, push, or PR was performed.

## Environment and dependency versions

Validated only on Python **3.14.7**, Linux **x86_64** (`Linux-7.1.8-arch1-3-x86_64`, glibc 2.44). This does not establish support for other architectures or operating systems. Active venv dependency versions: polars-readstat 0.21.1, Polars 1.44.2, PyArrow 24.0.0, DuckDB 1.5.5, Typer 0.27.2, Jinja2 3.1.6, pytest 8.4.2, Ruff 0.16.8, mypy 1.20.2. Build used isolated setuptools 81.0.0 and wheel 0.48.0.

External test fixture binaries and CSVs are development-only, hash-checked, ignored under `.parity-fixtures/`, and were not found in either distribution. The original upstream `missing_values.sas7bdat` with an independently pinned expected-value oracle was not obtainable. Following the operator's waiver, special-missing behavior is exercised best-effort with pyreadstat's generated `missing_test.sas7bdat`, including informative-null reason codes; this does not satisfy the original fixture provenance/oracle requirement. No production fixture redistribution is authorized.

## Final automated command results

Commands were run with pip/venv (not uv); project temp files use `TMPDIR="$PWD/.tmp"` because the host `/tmp` tmpfs is full.

| Command | Result |
|---|---|
| `TMPDIR="$PWD/.tmp" .venv/bin/python -m pytest -q` | **PASS** — 38 passed in 18.62s, no skips |
| `.venv/bin/python -m ruff check .` | **PASS** — All checks passed |
| `.venv/bin/python -m ruff format --check .` | **PASS** — 22 files already formatted |
| `.venv/bin/python -m mypy src` | **PASS** — no issues in 15 source files |
| `TMPDIR="$PWD/.tmp" .venv/bin/python -m build --sdist --wheel` | **PASS** — built `sentinel_parity-0.1.0.tar.gz` and `sentinel_parity-0.1.0-py3-none-any.whl` |
| pip install wheel into `.tmp/wheel-test-env`, run `parity --help` and `parity run --help` from `.tmp` | **PASS** — console script and both invocation paths work outside checkout |
| pip install sdist from `dist/sentinel_parity-0.1.0.tar.gz` | **PASS** — pip rebuilt a wheel and installed it |

The first attempted isolated wheel CLI check used `pip install --no-deps` in a venv without the declared dependencies and failed with `ModuleNotFoundError: typer`. This was corrected by installing declared wheel dependencies; subsequent installed-wheel CLI checks passed. It was environment setup, not a packaging defect.

Artifact inspection: wheel contains `sentinel_parity/resources/report.html`; sdist contains the same resource. Both contained zero external fixture members (no `.sas7bdat`, `.parity-fixtures`, or productsales CSV files). The direct artifact inventory reported 21 wheel members and 36 sdist members.

## Workload and resource behavior

- `test_disk_backed_comparison_smoke`: **PASS** — 250,000 duplicate-heavy Parquet rows per side, shuffled opposite-side order, staged in 4,096-row batches, DuckDB configured at 128MB memory and 2GB max temp, exact PASS with 250,000 matched pairs, no side-only rows, and 500,000 detail JSONL records. A prior measured test run completed the test in 5.61s; process child max RSS was 751,876 KiB. That RSS includes pytest/Polars/Python and is **not DuckDB-only memory**. DuckDB's configured limit does not cap whole-process RSS.
- `test_resource_exhaustion_is_error`: **PASS** — deliberate 250,000-row high-cardinality reversal with 1MB DuckDB memory and 1KB temp allowance raised DuckDB `OutOfMemoryException`; it did not masquerade as a value mismatch.
- Targeted final integration run for workload/resource/signal/escaping: 5 passed in 8.50s before the final expanded installed-wheel/report-failure tests; full suite subsequently passed with those tests included.

## Mandatory test list audit (AC.1–AC.9)

All names below are present across `tests/test_acceptance.py` and `tests/test_core.py` except where explicitly marked. A passing test count alone is not evidence that every plan assertion is met; the “Evidence/status” column identifies known partial tests.

| Mandatory test | Evidence/status |
|---|---|
| `test_installed_wheel_cli` | **Strengthened and PASS** — builds wheel, pip-installs to target, calls module `--help` and `run --help` outside checkout. Dependencies are not isolated inside this test; they are exercised via active interpreter environment. |
| `test_config_cli_equivalence` | **Strengthened and PASS** — invokes direct CLI and TOML-configured CLI against the same real SAS/Parquet data; both exit 0 and report 1,440 rows per side with 1,440 matches. |
| `test_config_precedence_and_relative_paths` | PASS at loader level for config-relative roots/output and CLI overrides; no isolated process invocation. |
| `test_invalid_config` | Partial — unknown TOML key rejected; not a matrix of malformed TOML/types/missing roots. |
| `test_discovery_matrix` | Partial — uppercase extension and matching-extension symlink rejection covered; full collision/pairing matrix is not asserted in this single named test. |
| `test_schema_name_alignment` | PASS for core name alignment behavior, but no full comparison all-row details asserted here. |
| `test_missing_files_and_columns` | **Partial** — current case combines corrupt SAS and a Python one-sided file and expects error 2; it does not independently verify missing-column all-row FAIL output. |
| `test_empty_scope` | PASS — actual runner rejects empty file scope. |
| `test_external_fixture_integrity` | PASS — SHA256s checked against fixture manifest. |
| `test_real_sas_reader_oracle` | **PASS** — full 1,440-row multiset comparison against pinned `productsales.csv`; CSV numerics parsed as exact Decimal/Fraction rationals, reader binary floats use exact float ratios, text remains verbatim, and MONTH uses ISO date semantics. No differing cells or rows were found. |
| `test_real_sas_special_missing_policy` | **Operator-waived deviation** — generated pyreadstat fixture and informative null reasons; original Shotwell fixture/oracle unavailable. |
| `test_real_sas_temporal_policy` | Partial — checks reader schema/metadata and row count; not a complete independent CSV decoded-value oracle. |
| `test_numeric_exactness_boundaries` | PASS for core boundary examples, not a SQL integration sweep of every plan-listed numeric pair. |
| `test_null_nan_text_policy` | PASS for pure policy examples. |
| `test_temporal_precision_and_families` | PASS for core temporal cases; Arrow nanosecond end-to-end detail serialization is not independently proven. |
| `test_unsupported_types` | PASS for core unsupported-type classification. |
| `test_value_encoding_roundtrip` | PASS for core typed value roundtrip. |
| `test_multiset_comparison_oracle` | Partial — real fixture exact match and count; independent Counter oracle, permutation invariance, and swap symmetry not all exercised. |
| `test_duplicate_conservation` | PASS for a one-row imbalance, record count, and one FAIL; not a broad randomized conservation oracle. |
| `test_row_details_complete` | Partial — asserts 2,880 output records and schema version, not every per-occurrence value/status. |
| `test_all_rows_fail_for_schema_or_missing_file` | **PASS** — schema mismatch, missing counterpart, incompatible family; exact all-row reason/status, readable-side counts, completeness, union normalized names, typed payloads and absent-column-vs-null distinction are asserted. |
| `test_html_report_contract` | Partial — checks title/details/links/offline string; no full HTML parser validation of all links/statuses/preview bounds. |
| `test_html_escaping` | PASS — real mismatch report with malicious cell; asserts escaped JSON embedding and no literal executable image tag. |
| `test_jsonl_typed_values` | Partial — scans up to 200 actual detail records for canonical year; not a comprehensive large-number/null/empty/binary roundtrip through CLI. |
| `test_summary_detail_reconciliation` | Partial — verifies detail-link record count equals twice matched pairs for a passing real-reader case. |
| `test_exit_code_precedence` | Partial — subprocess-like Typer runner path with corrupt Parquet confirms code 2; cell-value non-disclosure assertion checks filename sentinel, not a known cell sentinel across all logs/streams. |
| `test_input_and_output_safety` | **PASS** — rejects output/temp inside both roots, leaves a pre-existing nonempty output untouched, reports matching-extension symlink as error, and checks SHA-256 inputs unchanged. |
| `test_corrupt_dataset_continues` | PASS — corrupt dataset ERROR and independent good dataset PASS are both in report. |
| `test_report_write_failure` | **Strengthened and PASS** — real SAS/Parquet staging and DuckDB comparison; injects failure at atomic `index.html` write and asserts no index or temp HTML sibling. It is in-process injection, not a subprocess filesystem-permission failure. |
| `test_interruption_cleanup` | **PASS for active staging interruption** — SIGINT and SIGTERM are sent only after the subprocess reports 1,440 staged rows and the actual staged artifact exists, then blocks before runner advances; both exit nonzero and owned temp is cleaned. This specifically proves active staging, not the later DuckDB query phase. |
| `test_bounded_batch_pipeline` | **PASS** — actual Arrow batches counted at the owned staging boundary for real SAS and Parquet sources; observed maximum asserted positive and no greater than configured 17-row limit. |
| `test_disk_backed_comparison_smoke` | PASS — workload and metrics above. |
| `test_resource_exhaustion_is_error` | PASS — deliberate DuckDB OOM exception at very low configured resource caps. |
| `test_distribution_contents` | **Partial test itself** — asserts source template and fixture ignore rule; separate final wheel/sdist inspection verified actual contents/exclusions. |
| `test_readme_workflow` | **PASS** — creates fresh venv, builds/installs wheel with dependencies, stages copied external fixture and matching Parquet into fixture roots, executes both README invocations and verifies PASS summary, 1,440 matches, HTML and complete 2,880-line JSONL. |

## AC summary

| AC | Status | Evidence / remaining qualification |
|---|---|---|
| AC.1 | PASS for requested CLI/package acceptance | Built/installed wheel CLI paths, end-to-end config equivalence, final fresh-venv workflow pass; invalid-config matrix remains narrower than all possible malformed types. |
| AC.2 | PASS for discovery safety and detail scenarios | Discovery errors, empty scope, case and symlink behavior plus schema/missing/incompatible full detail outcomes pass; not every combinatorial filename collision is separately parameterized. |
| AC.3 | PASS except operator-waived original fixture | Full 1,440-row exact multiset oracle against pinned productsales.csv passes with Decimal/Fraction numeric keys, binary-float exact ratios, verbatim strings, ISO dates. Original Shotwell special-missing fixture remains waived; generated pyreadstat fixture is documented as best effort. |
| AC.4 | PASS for implemented value policy tests | Core numeric/null/text/temporal/unsupported/roundtrip tests pass; platform validation remains Python 3.14.7 Linux x86_64 only. |
| AC.5 | PASS | Duplicate conservation and full-row details pass; schema mismatch, missing counterpart, and incompatible-family outputs assert exact all-row records, counts, reasons, completeness, union names and absent-vs-null shape. |
| AC.6 | PASS for report output checks | Offline HTML, escaping, typed JSONL, summary reconciliation and complete detail cases pass. |
| AC.7 | PASS for requested safety/interruption checks | Both-root output/temp rejection, untouched nonempty output, symlink error, input hashes, atomic report-write failure, and SIGINT/SIGTERM during active staging with owned-temp cleanup pass. The signal test proves active staging work (artifact plus progress), not execution inside DuckDB query. |
| AC.8 | PASS | Actual batch sizes at owned staging boundary assert <= configured 17; 250k workload passes at 128MB/2GB; deliberate tiny-limit exhaustion raises DuckDB OutOfMemoryException. |
| AC.9 | PASS | Fresh venv + built wheel + dependency install executes both README run forms with staged fixture data and verifies summary/HTML/JSONL; wheel/sdist include report resource and exclude fixtures. |

## Review round 1 findings ledger

| Finding | Resolution and evidence |
|---|---|
| 1. Multi-dataset artifact collisions | Each dataset has its own `dataset-<id>` work directory; comparisons use unique DuckDB DB, spill and detail artifacts. Regression: `test_multiple_matched_pairs_have_isolated_details` verifies two passing pairs and two distinct published detail links. |
| 2. Nanosecond timestamps | Staging casts Arrow timestamp arrays to int64 before Python conversion; keys preserve epoch ns and payload ISO text carries nine fractional digits. Regression: `test_timestamp_ns_parquet_compares_to_sas_with_submicrosecond_digits`. |
| 3. SIGTERM/SIGHUP cleanup | `cli.main()` installs SIGTERM and SIGHUP handlers that raise `KeyboardInterrupt`; runner `finally` cleanup is exercised through the actual product entry point. `test_interruption_cleanup` no longer injects child handlers and checks owned temp cleanup and nonzero exit. |
| 4. Corrupt one-sided file | One-sided reads/details are inside per-entry exception boundaries; failures become ERROR with `detail_complete:false`, while processing continues. `test_corrupt_one_sided_dataset_is_reported_and_continues` asserts exit 2 and report. |
| 5. Exit/publish contract | CLI sanitizes OSError/TypeError and exits 2; TOML path types are validated while Typer Path overrides remain supported. Summary/index are removed if either publish operation fails. `test_report_write_failure` asserts exit 2 and no summary/index. |
| 6. One-sided JSONL typing | One-sided values use `_encode_scalar`/`typed_value`, retain `dataset_id`, reject normalized-name collisions, and do not stringify unsupported values. `test_one_sided_detail_values_use_typed_envelopes` checks NaN, date and binary envelopes and JSON validity. |
| 7. Aware vs naive timestamps | Family classifier distinguishes aware and naive timestamp dtype descriptions; mismatch is an all-row `incompatible_column_family` FAIL. Regression: `test_aware_naive_timestamps_are_schema_family_failure`. |
| 8. Independent multiset check | Independent directional counts use literal `EXCEPT ALL` over canonical key columns before the join and are compared to join-derived side-only counts. The DuckDB comparison connection uses one thread, allowing the mandated check to fit the existing resource gate. `test_disk_backed_comparison_smoke` passes at 250k rows. |
| 9. DuckDB DESCRIBE and previews | Staging records `DESCRIBE` output per canonical relation; summary includes preview truncation counts and HTML shows them. Multi-pair and one-sided preview tests assert metadata and the visible omitted count. |
| 10. Empty intersection | Runner raises a configuration error when no pairs match even if one-sided files exist. `test_empty_intersection_with_one_sided_files_is_configuration_error` asserts CLI exit 2 and no summary. |
| 11. Unsupported families | Explicit dtype-family parsing rejects nested/list/array/struct/map, Time, Duration and unknown families as dataset ERRORs, including all-null nested columns. `test_all_null_nested_column_is_dataset_error` asserts exit 2 and incomplete details. |
| 12. Root readability | Replaced the no-op mode test with `os.access(root, os.R_OK)` in runner validation. |
| 13. Error anchors and naming | HTML renders detail anchors only when a link exists; error dataset names include their directory prefix. Covered in report and error-continuation regressions. |
| 14. Reader padding behavior | Probe of pinned `productsales.sas7bdat` observed 1,440 COUNTRY values and zero trailing-blank values. This fixture cannot establish whether padded source fields are trimmed; README now limits the claim to reader-decoded strings and records this evidence/limitation. No dedicated padded fixture was available. |
| 15. Detail ordering | Not implemented: comparison emits rows in DuckDB join stream order, not guaranteed side/ordinal order. Sorting a full detail result would require additional buffering/materialization and is unnecessary for correctness; no ordering guarantee is claimed. |

## Latest verification

After round-one fixes and `MANIFEST.in`: `.venv/bin/python -m pytest -q` passed **45 tests, no skips**; Ruff lint and format checks passed; `.venv/bin/mypy --strict src` passed; `python -m build --sdist --wheel` passed. The 250k workload gate was rerun and passed with 128MB DuckDB memory and 2GB temp settings. Final sdist inventory includes test support modules and manifest while excluding fixture binaries/CSVs.

## Review round 2 findings ledger

| Item | Resolution and regression evidence |
|---|---|
| B1 uppercase `.SAS7BDAT` | Added `stage_sas_input`, creating an owned lowercase `.sas7bdat` hardlink with copy fallback. Matched staging and SAS-only row count, metadata, and detail readers use the owned path; originals remain unchanged. `test_uppercase_sas_extension_reads_real_fixture_pair` reads the real productsales fixture through the CLI and checks row counts, details, and input hash. |
| B2 dataset IDs in matched/schema JSONL | `dataset_id` is passed into `compare` and `_schema_failure` and written on every matched and schema-failure row. `test_multiple_matched_pairs_have_isolated_details` and `test_all_rows_fail_for_schema_or_missing_file` assert every line's ID matches the summary dataset ID. |
| B3 named timezone classification | Staging records timezone awareness from structural Polars `Datetime.time_zone` metadata, not zone-name substring detection. `test_aware_naive_timestamps_are_schema_family_failure` checks naive vs `America/New_York` FAIL; `test_timezone_named_zone_compares_equivalent_utc_instants` checks equivalent UTC/New_York epochs PASS. |
| B4 one-sided ns precision | One-sided details now consume Arrow record batches and cast timestamp arrays to int64 before formatting. `test_one_sided_detail_values_use_typed_envelopes` checks two ns-distinct values with exact 9-digit text; the matched ns test checks four exact strings by staging ordinal. |
| B5 unsupported types before schema branch | `compare` validates type maps for both inputs before checking column-name sets; one-sided writer validates schema dtypes before rows. `test_all_null_nested_column_is_dataset_error` and `test_all_null_struct_extra_column_is_dataset_error` use all-null extra List/Struct columns with differing column sets and assert ERROR/incomplete details. |
| B6 zero previews | `RunConfig` accepts `preview_rows=0`; HTML renders omitted counts independently of preview rows and omits the table when empty. `test_preview_zero_still_shows_mismatch_counts` asserts truncation counts and no table. |
| S1 multi-pair attribution | Second pair now differs: 1,440-row PASS and 7-row unique-value mismatch. Test checks summary counts, unique links, every dataset ID, and per-dataset values/statuses. |
| S2 exact ns evidence | Exact expected strings are checked by staging ordinal for four matched Parquet timestamps, including adjacent sub-microsecond values `.234567890` through `.234567893`. |
| S3 paths/config | `test_nonexistent_temp_dir_exits_two` reaches temp creation after a real matched fixture pair. `test_invalid_toml_path_type_exits_two` covers `sas_root=123`. |
| S4 owned EXCEPT boundary | `test_except_all_disagreement_is_dataset_error` calls the real owned `_except_count` helper, injects a disagreement at that owned boundary, and asserts dataset ERROR/no detail link. |
| S5 collision/readability | `test_one_sided_casefold_collision_is_error` uses a real Arrow Parquet schema with `Foo`/`foo`. `test_unreadable_input_root_exits_two` deterministically makes the owned `os.access` boundary deny the input root and checks sanitized exit 2. |
| S6 original and canonical DESCRIBE | `stage` captures `DESCRIBE` for raw/original and canonical relations; summary exposes both per side. The multi-pair test requires all four metadata entries. |
| S7 output and occurrence quality | `test_exit_code_precedence` now reads `capsys` once, checks both captured streams and CliRunner output, and proves a sensitive cell sentinel exists in JSONL but not console output. `test_row_details_complete` and `test_duplicate_conservation` assert IDs, keys/statuses by side and staging ordinal. |
| S8 ERROR policy | README documents that only the exception class is recorded and the third-party message is omitted, why third-party text is withheld (possible cell/path/SQL disclosure), and remediation classes. |

## Round 2 final verification

- `TMPDIR="$PWD/.tmp" .venv/bin/python -m pytest -q`: **54 passed in 22.35s, no skips** (after final source/test edits).
- `.venv/bin/python -m ruff check .`: **PASS** — all checks passed. The first final run caught a type-only `Path` import and a long test line; both were corrected before this pass.
- `.venv/bin/python -m ruff format --check .`: **PASS** — 23 files already formatted.
- `.venv/bin/mypy --strict src`: **PASS** — no issues in 16 source files.
- `TMPDIR="$PWD/.tmp" .venv/bin/python -m pytest -q tests/test_acceptance.py::test_disk_backed_comparison_smoke`: **PASS** — 1 passed in 5.82s; 250,000-row workload gate.
- `TMPDIR="$PWD/.tmp" .venv/bin/python -m build --sdist --wheel`: **PASS** — built `sentinel_parity-0.1.0.tar.gz` and `sentinel_parity-0.1.0-py3-none-any.whl` from the final source tree.
- Distribution inventory: wheel **22 members**, sdist **43 members**; both contain `resources/report.html`; neither contains SAS fixture binaries, `.parity-fixtures`, or productsales CSV fixture data.

## Review round 3 findings ledger

| Item | Correction and regression evidence |
|---|---|
| B3 naive/aware datetime regression | Reworked `test_aware_naive_timestamps_are_schema_family_failure` to use the real `datetime.sas7bdat` `DateTime` timestamp on both sides; verifies the SAS dtype is naive and the Parquet dtype is `America/New_York` aware. Asserts exit 1, schema FAIL with `incompatible_column_family`, and all eight detail rows FAIL with complete details. Existing `test_timezone_named_zone_compares_equivalent_utc_instants` remains unchanged. |
| S2 nanosecond exactness | Replaced permissive outcome in `test_timestamp_ns_keys_and_adjacent_ns_mismatch_are_exact`. It reads staged `k_instant` values and asserts distinct `dt-naive-ns` keys for consecutive ns values, then checks mismatched SAS-vs-Parquet instants yield exit 1, 0 matched/4 `sas_only`/4 `python_only`, detail reasons `only_sas` and `only_python`, and exact nine-digit fractional payloads. |
| README ERROR wording | README states ERROR entries record only the exception class and omit the third-party message; it no longer claims a redacted message field exists. |

## Round 3 final verification

- `TMPDIR="$PWD/.tmp" .venv/bin/python -m pytest -q`: **54 passed in 22.33s, no skips**.
- `.venv/bin/python -m ruff check .`: **PASS** — all checks passed.
- `.venv/bin/python -m ruff format --check .`: **PASS** — 23 files already formatted.
- `.venv/bin/mypy --strict src`: **PASS** — no issues in 16 source files.
- `TMPDIR="$PWD/.tmp" .venv/bin/python -m pytest -q tests/test_acceptance.py::test_disk_backed_comparison_smoke`: **PASS** — 1 passed; 250,000-row workload gate.
- `TMPDIR="$PWD/.tmp" .venv/bin/python -m build --sdist --wheel`: **PASS** — built wheel and sdist.
- Distribution inventory: wheel **22 members**, sdist **43 members**; both include the report resource and exclude fixture payloads.

## Scope qualifications

- The original upstream Shotwell `missing_values.sas7bdat` fixture and its independent expected-value oracle remain unavailable. Per explicit operator direction, that fixture requirement is waived; the checked-in development test uses the pinned pyreadstat-generated sample only as best-effort behavioral evidence. This is the sole fixture waiver and is not represented as original-fixture parity.
- Validation is limited to Python 3.14.7 on Linux x86_64. No claim is made for other architectures or operating systems.
- The interruption test proves signal delivery during an active staging operation using the owned adapter's completed-row heartbeat and artifact presence; it does not assert the signal lands during the subsequent DuckDB SQL phase.
- Other tests may not enumerate every theoretical malformed config, collision, or type combination; the requested acceptance behaviors were exercised with real artifacts and exact assertions as recorded above.
