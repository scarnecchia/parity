import csv
import hashlib
import json
import shutil
import subprocess
import sys
from collections import Counter
from decimal import Decimal
from fractions import Fraction
from pathlib import Path

import polars as pl
import polars_readstat
import pytest
from typer.testing import CliRunner

from sentinel_parity.cli import app
from sentinel_parity.config import RunConfig
from sentinel_parity.io.config_loader import load_config
from sentinel_parity.io.discovery import discover
from sentinel_parity.io.staging import stage
from sentinel_parity.runner import run

runner = CliRunner()


def _roots(base: Path) -> tuple[Path, Path]:
    sas, python = base / "sas", base / "python"
    for root in (sas, python):
        (root / "dplocal").mkdir(parents=True, exist_ok=True)
        (root / "msoc").mkdir(parents=True, exist_ok=True)
    return sas, python


def _write_parquet(path: Path, data: dict[str, list[object]]) -> None:
    pl.DataFrame(data).write_parquet(path)


def test_external_fixture_integrity(fixtures: Path) -> None:
    manifest = json.loads((Path(__file__).with_name("fixtures_manifest.json")).read_text())
    for name, item in manifest["fixtures"].items():
        assert hashlib.sha256((fixtures / name).read_bytes()).hexdigest() == item["sha256"]


def test_real_sas_reader_oracle(fixtures: Path, tmp_path: Path) -> None:
    data = (
        polars_readstat.ScanReadstat(str(fixtures / "productsales.sas7bdat"))
        .df.collect()
        .to_dicts()
    )
    with (fixtures / "productsales.csv").open(encoding="utf-8-sig", newline="") as source:
        oracle = list(csv.DictReader(source))
    expected_columns = {
        "ACTUAL",
        "PREDICT",
        "COUNTRY",
        "REGION",
        "DIVISION",
        "PRODTYPE",
        "PRODUCT",
        "QUARTER",
        "YEAR",
        "MONTH",
    }
    assert len(data) == len(oracle) == 1440
    assert set(data[0]) == expected_columns

    def exact(value: object) -> tuple[str, object]:
        if isinstance(value, float):
            return ("numeric", value.as_integer_ratio())
        if isinstance(value, (int, Decimal)) and not isinstance(value, bool):
            numerator, denominator = Fraction(value).as_integer_ratio()
            return ("numeric", (numerator, denominator))
        if isinstance(value, str):
            return ("text", value)
        if value is None:
            return ("null", None)
        if hasattr(value, "isoformat"):
            return ("temporal", value.isoformat())
        return (type(value).__name__, value)

    columns = sorted(expected_columns)
    sas_rows = [tuple(exact(row[name]) for name in columns) for row in data]
    csv_rows: list[tuple[tuple[str, object], ...]] = []
    for row in oracle:
        cells: list[tuple[str, object]] = []
        for name in columns:
            value = row[name]
            if name in {"ACTUAL", "PREDICT", "QUARTER", "YEAR"}:
                cells.append(exact(Decimal(value)))
            elif name == "MONTH":
                from datetime import date

                cells.append(exact(date.fromisoformat(value)))
            else:
                cells.append(exact(value))
        csv_rows.append(tuple(cells))
    reader_counts, oracle_counts = Counter(sas_rows), Counter(csv_rows)
    if reader_counts != oracle_counts:
        sas_remaining, csv_remaining = reader_counts.copy(), oracle_counts.copy()
        sas_remaining.subtract(oracle_counts)
        csv_remaining.subtract(reader_counts)
        differences: list[dict[str, object]] = []
        for side, counts in (("reader_only", sas_remaining), ("csv_only", csv_remaining)):
            for row, count in counts.items():
                if count <= 0:
                    continue
                for column, value in zip(columns, row, strict=True):
                    differences.append(
                        {"side": side, "column": column, "value": value, "multiplicity": count}
                    )
        difference_path = tmp_path / "productsales-oracle-differences.json"
        difference_path.write_text(json.dumps(differences, indent=2), encoding="utf-8")
        raise AssertionError(f"productsales full-row oracle mismatch; details: {difference_path}")
    assert polars_readstat.ScanReadstat(str(fixtures / "productsales.sas7bdat")).metadata[
        "row_count"
    ] == len(oracle)


def test_real_sas_temporal_reader_schema(fixtures: Path) -> None:
    reader = polars_readstat.ScanReadstat(str(fixtures / "datetime.sas7bdat"))
    assert reader.metadata["row_count"] == 4
    assert reader.schema["Date1"] == pl.Date
    assert str(reader.schema["DateTime"]).startswith("Datetime(time_unit='us'")
    assert set(reader.metadata["columns"][2]["format"] for _ in [0]) == {"DATETIME"}


def test_timestamp_ns_keys_and_adjacent_ns_mismatch_are_exact(
    fixtures: Path, tmp_path: Path
) -> None:
    import duckdb
    import pyarrow as pa
    import pyarrow.parquet as pq

    from sentinel_parity.io.staging import stage

    # Read canonical keys from the actual staged comparison boundary.
    adjacent_path = tmp_path / "adjacent.parquet"
    pq.write_table(
        pa.table(
            {
                "instant": pa.array(
                    [1_234_567_891_234_567_890, 1_234_567_891_234_567_891],
                    type=pa.timestamp("ns"),
                )
            }
        ),
        adjacent_path,
    )
    stage_work = tmp_path / "adjacent-stage"
    stage_work.mkdir()
    staged = stage(adjacent_path, "python", stage_work)
    connection = duckdb.connect()
    try:
        adjacent_keys = connection.execute(
            'SELECT "k_instant" FROM read_parquet(?) ORDER BY ordinal', [str(staged["path"])]
        ).fetchall()
    finally:
        connection.close()
    assert adjacent_keys == [
        ("dt-ns:1234567891234567890",),
        ("dt-ns:1234567891234567891",),
    ]
    assert adjacent_keys[0] != adjacent_keys[1]

    # Shift every Parquet instant by one ns, so no SAS timestamp can match.
    sas, python = _roots(tmp_path)
    shutil.copyfile(fixtures / "datetime.sas7bdat", sas / "dplocal" / "temporal.sas7bdat")
    sas_frame = polars_readstat.ScanReadstat(
        str(sas / "dplocal" / "temporal.sas7bdat")
    ).df.collect()
    sas_datetime = sas_frame["DateTime"].to_arrow()
    assert pa.types.is_timestamp(sas_datetime.type)
    unit_to_ns = {"s": 1_000_000_000, "ms": 1_000_000, "us": 1_000, "ns": 1}
    sas_time_ns = [
        None if value is None else value * unit_to_ns[sas_datetime.type.unit]
        for value in sas_datetime.cast(pa.int64()).to_pylist()
    ]
    mismatched_values = pa.array(
        [None if value is None else value + 1 for value in sas_time_ns], type=pa.timestamp("ns")
    )
    table = pa.table({name: sas_frame[name].to_arrow() for name in sas_frame.columns})
    table = table.set_column(
        table.schema.get_field_index("DateTime"), "DateTime", mismatched_values
    )
    pq.write_table(table, python / "dplocal" / "temporal.parquet")

    assert run(RunConfig(sas, python, tmp_path / "out")) == 1
    summary = json.loads((tmp_path / "out" / "summary.json").read_text())
    dataset = summary["datasets"][0]
    assert dataset["status"] == "FAIL"
    assert (dataset["sas_rows"], dataset["python_rows"], dataset["matched_pairs"]) == (4, 4, 0)
    assert (dataset["sas_only"], dataset["python_only"]) == (4, 4)
    details = [
        json.loads(line)
        for line in (tmp_path / "out" / summary["detail_links"][dataset["id"]])
        .read_text()
        .splitlines()
    ]
    assert len(details) == 8
    assert {(row["side"], row["status"], row["reason"]) for row in details} == {
        ("sas", "FAIL", "only_sas"),
        ("python", "FAIL", "only_python"),
    }
    from datetime import UTC, datetime

    def exact_iso_ns(epoch_ns: int) -> str:
        seconds, fraction = divmod(epoch_ns, 1_000_000_000)
        base = datetime.fromtimestamp(seconds, tz=UTC).strftime("%Y-%m-%dT%H:%M:%S")
        return f"{base}.{fraction:09d}"

    expected_python_values = sorted(
        exact_iso_ns(value)
        for value in mismatched_values.cast(pa.int64()).to_pylist()
        if value is not None
    )
    python_values = sorted(
        row["values"]["datetime"]["value"] for row in details if row["side"] == "python"
    )
    assert python_values == expected_python_values
    assert all(len(value.rsplit(".", 1)[1]) == 9 for value in python_values)


def test_timezone_named_zone_compares_equivalent_utc_instants(tmp_path: Path) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    from sentinel_parity.io.staging import stage

    sas, python = _roots(tmp_path)
    zone = "America/New_York"
    values_utc = pa.array(
        [1_600_000_000_000_000_001, 1_600_000_000_000_000_002], type=pa.timestamp("ns", tz="UTC")
    )
    values_ny = pa.array(
        [1_600_000_000_000_000_001, 1_600_000_000_000_000_002], type=pa.timestamp("ns", tz=zone)
    )
    left_path, right_path = tmp_path / "utc.parquet", tmp_path / "ny.parquet"
    pq.write_table(pa.table({"instant": values_utc}), left_path)
    pq.write_table(pa.table({"instant": values_ny}), right_path)
    work = tmp_path / "work"
    work.mkdir()
    left, right = stage(left_path, "python", work), stage(right_path, "python", work)
    from sentinel_parity.io.duckdb_comparison import compare

    result = compare(left, right, work, "128MB", "2GB", "timezone-test")
    assert result["status"] == "PASS"
    records = [json.loads(line) for line in Path(result["details_path"]).read_text().splitlines()]
    assert len(records) == 4
    assert all(record["status"] == "PASS" for record in records)
    assert all(record["dataset_id"] == "timezone-test" for record in records)


def test_naive_and_aware_timestamps_match_on_values(fixtures: Path, tmp_path: Path) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    sas, python = _roots(tmp_path)
    shutil.copyfile(fixtures / "datetime.sas7bdat", sas / "dplocal" / "time.sas7bdat")
    sas_path = sas / "dplocal" / "time.sas7bdat"
    sas_reader = polars_readstat.ScanReadstat(str(sas_path))
    assert isinstance(sas_reader.schema["DateTime"], pl.Datetime)
    assert sas_reader.schema["DateTime"].time_zone is None
    frame = sas_reader.df.collect()
    fields = {name: frame[name].to_arrow() for name in frame.columns}
    naive_arrow = fields["DateTime"]
    assert pa.types.is_timestamp(naive_arrow.type) and naive_arrow.type.tz is None
    fields["DateTime"] = pa.array(
        naive_arrow.cast(pa.int64()).to_pylist(),
        type=pa.timestamp("us", tz="America/New_York"),
    )
    pq.write_table(pa.table(fields), python / "dplocal" / "time.parquet")

    # Same instants under different declared types: values match, so pass.
    assert run(RunConfig(sas, python, tmp_path / "out")) == 0
    summary = json.loads((tmp_path / "out" / "summary.json").read_text())
    dataset = summary["datasets"][0]
    assert dataset["status"] == "PASS"
    assert dataset["reason"] is None
    assert (dataset["matched_pairs"], dataset["sas_only"], dataset["python_only"]) == (4, 0, 0)
    details = [
        json.loads(line)
        for line in (tmp_path / "out" / summary["detail_links"][dataset["id"]])
        .read_text()
        .splitlines()
    ]
    assert len(details) == 8
    assert all(row["status"] == "PASS" for row in details)


def test_config_cli_equivalence(tmp_path: Path) -> None:
    sas, python = _roots(tmp_path)
    shutil.copyfile(
        Path(__file__).resolve().parents[1] / ".parity-fixtures" / "productsales.sas7bdat",
        sas / "dplocal" / "data.sas7bdat",
    )
    polars_readstat.ScanReadstat(str(sas / "dplocal" / "data.sas7bdat")).df.collect().write_parquet(
        python / "dplocal" / "data.parquet"
    )
    direct_output = tmp_path / "direct"
    config_output = tmp_path / "configured"
    config = tmp_path / "config.toml"
    config.write_text(
        f'sas_root = "{sas}"\npython_root = "{python}"\noutput_dir = "{config_output}"\n'
    )
    direct = runner.invoke(
        app,
        [
            "run",
            "--sas-root",
            str(sas),
            "--python-root",
            str(python),
            "--output-dir",
            str(direct_output),
        ],
    )
    configured = runner.invoke(app, ["run", "--config", str(config)])
    assert direct.exit_code == configured.exit_code == 0
    direct_summary = json.loads((direct_output / "summary.json").read_text())
    config_summary = json.loads((config_output / "summary.json").read_text())
    for summary in (direct_summary, config_summary):
        assert summary["status"] == "PASS"
        dataset = summary["datasets"][0]
        assert (dataset["sas_rows"], dataset["python_rows"], dataset["matched_pairs"]) == (
            1440,
            1440,
            1440,
        )


def test_config_precedence_and_relative_paths(tmp_path: Path) -> None:
    config = tmp_path / "run.toml"
    config.write_text(
        'sas_root="sas"\npython_root="python"\npreview_rows=4\noutput_dir="reports"\n'
    )
    override = tmp_path / "other"
    result = load_config(
        config,
        {
            "sas_root": override,
            "python_root": None,
            "output_dir": None,
            "memory_limit": None,
            "temp_dir": None,
            "max_temp_size": None,
            "preview_rows": 9,
        },
    )
    assert result.sas_root == override.resolve()
    assert result.python_root == (tmp_path / "python").resolve()
    assert result.output_dir == (tmp_path / "reports").resolve()
    assert result.preview_rows == 9


def test_invalid_config(tmp_path: Path) -> None:
    config = tmp_path / "bad.toml"
    config.write_text('sas_root="x"\npython_root="y"\nunknown=1\n')
    with pytest.raises(ValueError):
        load_config(config, {})


def test_invalid_toml_path_type_exits_two(tmp_path: Path) -> None:
    config = tmp_path / "bad-path.toml"
    config.write_text('sas_root = 123\npython_root = "python"\n')
    result = runner.invoke(app, ["run", "--config", str(config)])
    assert result.exit_code == 2
    assert "sas_root must be a string path" in result.stderr


def test_nonexistent_temp_dir_exits_two(tmp_path: Path) -> None:
    sas, python = _roots(tmp_path)
    fixture = Path(__file__).resolve().parents[1] / ".parity-fixtures" / "productsales.sas7bdat"
    shutil.copyfile(fixture, sas / "dplocal" / "matched.sas7bdat")
    polars_readstat.ScanReadstat(
        str(sas / "dplocal" / "matched.sas7bdat")
    ).df.collect().write_parquet(python / "dplocal" / "matched.parquet")
    result = runner.invoke(
        app,
        [
            "run",
            "--sas-root",
            str(sas),
            "--python-root",
            str(python),
            "--output-dir",
            str(tmp_path / "out"),
            "--temp-dir",
            str(tmp_path / "missing-temp"),
        ],
    )
    assert result.exit_code == 2
    assert "FileNotFoundError" in result.stderr or "operation failed" in result.stderr


def test_unreadable_input_root_exits_two(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import sentinel_parity.runner as runner_module

    sas, python = _roots(tmp_path)
    sas_file = sas / "dplocal" / "secret.sas7bdat"
    sas_file.touch()
    original_access = runner_module.os.access
    monkeypatch.setattr(
        runner_module.os,
        "access",
        lambda path, mode: False if Path(path) == sas else original_access(path, mode),
    )
    result = runner.invoke(
        app,
        [
            "run",
            "--sas-root",
            str(sas),
            "--python-root",
            str(python),
            "--output-dir",
            str(tmp_path / "out"),
        ],
    )
    assert result.exit_code == 2
    normalized = " ".join(result.stderr.replace("│", " ").split()).casefold()
    assert "readable" in normalized
    assert "secret" not in result.stdout + result.stderr


def test_one_sided_casefold_collision_is_error(tmp_path: Path) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    sas, python = _roots(tmp_path)
    collision = pa.table({"Foo": pa.array([1]), "foo": pa.array([2])})
    pq.write_table(collision, python / "dplocal" / "only.parquet")
    fixture = Path(__file__).resolve().parents[1] / ".parity-fixtures" / "productsales.sas7bdat"
    shutil.copyfile(fixture, sas / "msoc" / "matched.sas7bdat")
    polars_readstat.ScanReadstat(str(sas / "msoc" / "matched.sas7bdat")).df.collect().write_parquet(
        python / "msoc" / "matched.parquet"
    )
    assert run(RunConfig(sas, python, tmp_path / "out")) == 2
    summary = json.loads((tmp_path / "out" / "summary.json").read_text())
    collision_dataset = next(
        item for item in summary["datasets"] if item["name"] == "dplocal/only.parquet"
    )
    assert collision_dataset["status"] == "ERROR"
    assert collision_dataset["detail_complete"] is False
    assert collision_dataset["reason"] == "ValueError"


def test_installed_wheel_cli(tmp_path: Path) -> None:
    project = Path(__file__).resolve().parents[1]
    wheel_dir = tmp_path / "wheels"
    wheel_dir.mkdir()
    build = subprocess.run(
        [sys.executable, "-m", "build", "--wheel", "--outdir", str(wheel_dir)],
        cwd=project,
        capture_output=True,
        text=True,
        check=False,
    )
    assert build.returncode == 0, build.stderr
    target = tmp_path / "installed"
    wheel = next(wheel_dir.glob("*.whl"))
    install = subprocess.run(
        [sys.executable, "-m", "pip", "install", "--no-deps", "--target", str(target), str(wheel)],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
    )
    assert install.returncode == 0, install.stderr
    env = {**__import__("os").environ, "PYTHONPATH": str(target)}
    for args, expected in ((["--help"], "run"), (["run", "--help"], "--sas_root")):
        result = subprocess.run(
            [sys.executable, "-m", "sentinel_parity.cli", *args],
            cwd=tmp_path,
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )
        assert result.returncode == 0, result.stderr
        assert expected in result.stdout


def test_discovery_matrix(tmp_path: Path) -> None:
    sas, _ = _roots(tmp_path)
    (sas / "dplocal" / "MiX.SAS7BDAT").touch()
    (sas / "msoc" / "unrelated.txt").touch()
    assert [entry.filename for entry in discover(sas, ".sas7bdat")] == ["MiX.SAS7BDAT"]
    (sas / "dplocal" / "link.sas7bdat").symlink_to(sas / "dplocal" / "MiX.SAS7BDAT")
    with pytest.raises(ValueError):
        discover(sas, ".sas7bdat")


def test_missing_files_and_columns(tmp_path: Path) -> None:
    sas, python = _roots(tmp_path)
    _write_parquet(python / "dplocal" / "only.parquet", {"x": [1]})
    (sas / "dplocal" / "only.sas7bdat").write_bytes(b"bad")
    code = run(RunConfig(sas, python, tmp_path / "out"))
    assert code == 2


def test_empty_scope(tmp_path: Path) -> None:
    sas, python = _roots(tmp_path)
    with pytest.raises(ValueError, match="no matched dataset pairs"):
        run(RunConfig(sas, python, tmp_path / "out"))


def test_empty_intersection_with_one_sided_files_is_configuration_error(tmp_path: Path) -> None:
    sas, python = _roots(tmp_path)
    shutil.copyfile(
        Path(__file__).resolve().parents[1] / ".parity-fixtures" / "productsales.sas7bdat",
        sas / "dplocal" / "only.sas7bdat",
    )
    _write_parquet(python / "msoc" / "other.parquet", {"value": [1]})
    result = runner.invoke(
        app,
        [
            "run",
            "--sas-root",
            str(sas),
            "--python-root",
            str(python),
            "--output-dir",
            str(tmp_path / "out"),
        ],
    )
    assert result.exit_code == 2
    assert "no matched dataset pairs" in result.stderr
    assert not (tmp_path / "out" / "summary.json").exists()


def test_prefixed_sas_filename_pairs_and_compares_end_to_end(tmp_path: Path) -> None:
    fixture = Path(__file__).resolve().parents[1] / ".parity-fixtures" / "productsales.sas7bdat"
    sas, python = _roots(tmp_path)
    sas_file = sas / "dplocal" / "r01_products.sas7bdat"
    shutil.copyfile(fixture, sas_file)
    polars_readstat.ScanReadstat(str(sas_file)).df.collect().write_parquet(
        python / "dplocal" / "products.parquet"
    )

    assert run(RunConfig(sas, python, tmp_path / "out")) == 0
    dataset = json.loads((tmp_path / "out" / "summary.json").read_text())["datasets"][0]
    assert dataset["status"] == "PASS"
    assert dataset["name"] == "dplocal/r01_products.sas7bdat"
    assert (dataset["sas_rows"], dataset["python_rows"], dataset["matched_pairs"]) == (
        1440,
        1440,
        1440,
    )


def test_prefixed_parquet_filename_pairs_and_compares_end_to_end(tmp_path: Path) -> None:
    fixture = Path(__file__).resolve().parents[1] / ".parity-fixtures" / "productsales.sas7bdat"
    sas, python = _roots(tmp_path)
    sas_file = sas / "dplocal" / "products.sas7bdat"
    shutil.copyfile(fixture, sas_file)
    polars_readstat.ScanReadstat(str(sas_file)).df.collect().write_parquet(
        python / "dplocal" / "R01_products.parquet"
    )

    assert run(RunConfig(sas, python, tmp_path / "out")) == 0
    dataset = json.loads((tmp_path / "out" / "summary.json").read_text())["datasets"][0]
    assert dataset["status"] == "PASS"
    assert dataset["name"] == "dplocal/products.sas7bdat"
    assert (dataset["sas_rows"], dataset["python_rows"], dataset["matched_pairs"]) == (
        1440,
        1440,
        1440,
    )


def test_uppercase_sas_extension_reads_real_fixture_pair(tmp_path: Path) -> None:
    fixture = Path(__file__).resolve().parents[1] / ".parity-fixtures" / "productsales.sas7bdat"
    sas, python = _roots(tmp_path)
    sas_file = sas / "dplocal" / "Uppercase.SAS7BDAT"
    shutil.copyfile(fixture, sas_file)
    input_hash = hashlib.sha256(sas_file.read_bytes()).hexdigest()
    frame = polars_readstat.ScanReadstat(str(fixture)).df.collect()
    frame.write_parquet(python / "dplocal" / "uppercase.parquet")
    assert run(RunConfig(sas, python, tmp_path / "out")) == 0
    summary = json.loads((tmp_path / "out" / "summary.json").read_text())
    dataset = summary["datasets"][0]
    assert dataset["status"] == "PASS"
    assert dataset["sas_rows"] == dataset["python_rows"] == 1440
    assert hashlib.sha256(sas_file.read_bytes()).hexdigest() == input_hash
    details = [
        json.loads(line)
        for line in (tmp_path / "out" / summary["detail_links"][dataset["id"]])
        .read_text()
        .splitlines()
    ]
    assert len(details) == 2880
    assert all(record["dataset_id"] == dataset["id"] for record in details)


def test_multiset_comparison_oracle(tmp_path: Path) -> None:
    sas, python = _roots(tmp_path)
    # Build a valid SAS sample and match it exactly as Parquet.
    sas_file = sas / "dplocal" / "sample.sas7bdat"
    shutil.copyfile(
        Path(__file__).resolve().parents[1] / ".parity-fixtures" / "productsales.sas7bdat", sas_file
    )
    frame = polars_readstat.ScanReadstat(str(sas_file)).df.collect()
    frame.write_parquet(python / "dplocal" / "sample.parquet")
    assert run(RunConfig(sas, python, tmp_path / "out")) == 0
    summary = json.loads((tmp_path / "out" / "summary.json").read_text())
    assert summary["datasets"][0]["matched_pairs"] == 1440


def test_multiple_matched_pairs_have_isolated_details(tmp_path: Path) -> None:
    sas, python = _roots(tmp_path)
    fixture = Path(__file__).resolve().parents[1] / ".parity-fixtures" / "productsales.sas7bdat"
    first_file = sas / "dplocal" / "first.sas7bdat"
    second_file = sas / "msoc" / "second.sas7bdat"
    shutil.copyfile(fixture, first_file)
    shutil.copyfile(fixture, second_file)
    first = polars_readstat.ScanReadstat(str(first_file)).df.collect()
    second = (
        polars_readstat.ScanReadstat(str(second_file))
        .df.collect()
        .head(7)
        .with_columns(pl.lit("pair-two-only").alias("COUNTRY"))
    )
    first.write_parquet(python / "dplocal" / "first.parquet")
    second.write_parquet(python / "msoc" / "second.parquet")
    assert run(RunConfig(sas, python, tmp_path / "out")) == 1
    summary = json.loads((tmp_path / "out" / "summary.json").read_text())
    assert len(summary["datasets"]) == 2
    by_name = {dataset["name"]: dataset for dataset in summary["datasets"]}
    assert by_name["dplocal/first.sas7bdat"]["status"] == "PASS"
    second_dataset = by_name["msoc/second.sas7bdat"]
    assert second_dataset["status"] == "FAIL"
    assert second_dataset["sas_rows"] == 1440 and second_dataset["python_rows"] == 7
    assert all(
        dataset["sas_original_describe"]
        and dataset["sas_describe"]
        and dataset["python_original_describe"]
        and dataset["python_describe"]
        for dataset in summary["datasets"]
    )
    links = summary["detail_links"]
    assert len(links) == 2 and len(set(links.values())) == 2
    for dataset in summary["datasets"]:
        path = tmp_path / "out" / links[dataset["id"]]
        records = [json.loads(line) for line in path.read_text().splitlines()]
        assert all(record["dataset_id"] == dataset["id"] for record in records)
        assert len(records) == dataset["sas_rows"] + dataset["python_rows"]
        if dataset["name"] == "dplocal/first.sas7bdat":
            assert all(record["status"] == "PASS" for record in records)
        else:
            python_rows = [record for record in records if record["side"] == "python"]
            assert len(python_rows) == 7
            assert all(row["values"]["country"]["value"] == "pair-two-only" for row in python_rows)
            sas_failures = [
                row for row in records if row["side"] == "sas" and row["status"] == "FAIL"
            ]
            assert len(sas_failures) == 1440


def test_duplicate_conservation(tmp_path: Path) -> None:
    sas, python = _roots(tmp_path)
    shutil.copyfile(
        Path(__file__).resolve().parents[1] / ".parity-fixtures" / "productsales.sas7bdat",
        sas / "dplocal" / "sample.sas7bdat",
    )
    frame = polars_readstat.ScanReadstat(str(sas / "dplocal" / "sample.sas7bdat")).df.collect()
    frame.head(frame.height - 1).write_parquet(python / "dplocal" / "sample.parquet")
    assert run(RunConfig(sas, python, tmp_path / "out")) == 1
    summary = json.loads((tmp_path / "out" / "summary.json").read_text())
    dataset = summary["datasets"][0]
    details = [
        json.loads(line)
        for line in (tmp_path / "out" / summary["detail_links"][dataset["id"]])
        .read_text()
        .splitlines()
    ]
    assert len(details) == 2879
    assert all(record["dataset_id"] == dataset["id"] for record in details)
    sas_rows = {
        record["staging_row_number"]: record for record in details if record["side"] == "sas"
    }
    python_rows = {
        record["staging_row_number"]: record for record in details if record["side"] == "python"
    }
    assert set(sas_rows) == set(range(frame.height))
    assert set(python_rows) == set(range(frame.height - 1))
    failed_sas = [record for record in sas_rows.values() if record["status"] == "FAIL"]
    assert len(failed_sas) == 1
    excess_ordinal = frame.height - 1
    assert failed_sas[0]["staging_row_number"] == excess_ordinal
    expected_country = frame["COUNTRY"][excess_ordinal]
    assert failed_sas[0]["values"]["country"] == {"type": "string", "value": expected_country}
    assert all(record["status"] == "PASS" for record in python_rows.values())


def test_row_details_complete(tmp_path: Path) -> None:
    sas, python = _roots(tmp_path)
    shutil.copyfile(
        Path(__file__).resolve().parents[1] / ".parity-fixtures" / "productsales.sas7bdat",
        sas / "dplocal" / "sample.sas7bdat",
    )
    polars_readstat.ScanReadstat(
        str(sas / "dplocal" / "sample.sas7bdat")
    ).df.collect().write_parquet(python / "dplocal" / "sample.parquet")
    assert run(RunConfig(sas, python, tmp_path / "out")) == 0
    summary = json.loads((tmp_path / "out" / "summary.json").read_text())
    dataset = summary["datasets"][0]
    records = [
        json.loads(line)
        for line in (tmp_path / "out" / summary["detail_links"][dataset["id"]])
        .read_text()
        .splitlines()
    ]
    assert len(records) == 2880
    assert all(record["schema_version"] == 1 for record in records)
    assert all(record["dataset_id"] == dataset["id"] for record in records)
    expected = polars_readstat.ScanReadstat(str(sas / "dplocal" / "sample.sas7bdat")).df.collect()
    by_occurrence = {(record["side"], record["staging_row_number"]): record for record in records}
    assert len(by_occurrence) == 2880
    for side in ("sas", "python"):
        for ordinal in range(expected.height):
            record = by_occurrence[(side, ordinal)]
            assert record["status"] == "PASS"
            assert record["values"]["country"] == {
                "type": "string",
                "value": expected["COUNTRY"][ordinal],
            }


def test_all_rows_fail_for_schema_or_missing_file(tmp_path: Path) -> None:
    fixture = Path(__file__).resolve().parents[1] / ".parity-fixtures" / "productsales.sas7bdat"

    def parse_details(
        output: Path, reason: str
    ) -> tuple[dict[str, object], list[dict[str, object]]]:
        summary = json.loads((output / "summary.json").read_text())
        dataset = next(item for item in summary["datasets"] if item["reason"] == reason)
        detail_path = output / summary["detail_links"][dataset["id"]]
        records = [json.loads(line) for line in detail_path.read_text().splitlines()]
        return dataset, records

    sas, python = _roots(tmp_path / "schema")
    shutil.copyfile(fixture, sas / "dplocal" / "schema.sas7bdat")
    source = polars_readstat.ScanReadstat(str(sas / "dplocal" / "schema.sas7bdat")).df.collect()
    _write_parquet(python / "dplocal" / "schema.parquet", {"different": [None, "x"]})
    assert run(RunConfig(sas, python, tmp_path / "schema-out")) == 1
    dataset, records = parse_details(tmp_path / "schema-out", "schema_columns_differ")
    assert dataset["reason"] == "schema_columns_differ"
    assert dataset["sas_rows"] == source.height and dataset["python_rows"] == 2
    assert dataset["detail_complete"] is True
    assert len(records) == source.height + 2
    expected_names = {name.casefold() for name in source.columns} | {"different"}
    assert {record["side"] for record in records} == {"sas", "python"}
    assert all(record["status"] == "FAIL" for record in records)
    assert all(record["reason"] == "schema_columns_differ" for record in records)
    assert all(record["dataset_id"] == dataset["id"] for record in records)
    sas_records = [record for record in records if record["side"] == "sas"]
    python_records = [record for record in records if record["side"] == "python"]
    assert len(sas_records) == source.height and len(python_records) == 2
    assert all(set(record["values"]) == expected_names for record in records)
    assert all(record["values"]["different"] == {"type": "absent_column"} for record in sas_records)
    assert python_records[0]["values"]["different"] is None
    assert python_records[1]["values"]["different"] == {"type": "string", "value": "x"}
    assert all(record["values"]["actual"] == {"type": "absent_column"} for record in python_records)

    sas2, python2 = _roots(tmp_path / "missing")
    shutil.copyfile(fixture, sas2 / "dplocal" / "only.sas7bdat")
    readable = polars_readstat.ScanReadstat(str(sas2 / "dplocal" / "only.sas7bdat")).df.collect()
    shutil.copyfile(fixture, sas2 / "msoc" / "matched.sas7bdat")
    polars_readstat.ScanReadstat(
        str(sas2 / "msoc" / "matched.sas7bdat")
    ).df.collect().write_parquet(python2 / "msoc" / "matched.parquet")
    assert run(RunConfig(sas2, python2, tmp_path / "missing-out")) == 1
    dataset, records = parse_details(tmp_path / "missing-out", "missing_counterpart")
    assert dataset["reason"] == "missing_counterpart"
    assert dataset["sas_rows"] == readable.height and dataset["python_rows"] == 0
    assert dataset["detail_complete"] is True
    assert len(records) == readable.height
    assert all(
        record["side"] == "sas" and record["reason"] == "missing_counterpart" for record in records
    )
    assert all(record["dataset_id"] == dataset["id"] for record in records)
    assert all(
        set(record["values"]) == {name.casefold() for name in readable.columns}
        for record in records
    )

    sas3, python3 = _roots(tmp_path / "family")
    shutil.copyfile(fixture, sas3 / "dplocal" / "family.sas7bdat")
    _write_parquet(
        python3 / "dplocal" / "family.parquet",
        {
            "actual": ["text"] * 1440,
            "predict": [1.0] * 1440,
            "country": ["x"] * 1440,
            "region": ["x"] * 1440,
            "division": ["x"] * 1440,
            "prodtype": ["x"] * 1440,
            "product": ["x"] * 1440,
            "quarter": [1] * 1440,
            "year": [1] * 1440,
            "month": [1] * 1440,
        },
    )
    # Declared types differ (numeric vs text), but comparison runs on values:
    # no row matches, so every row fails as only_sas/only_python, not schema.
    assert run(RunConfig(sas3, python3, tmp_path / "family-out")) == 1
    summary3 = json.loads((tmp_path / "family-out" / "summary.json").read_text())
    dataset = summary3["datasets"][0]
    assert dataset["status"] == "FAIL"
    assert dataset["reason"] is None
    assert dataset["sas_rows"] == 1440 and dataset["python_rows"] == 1440
    assert (dataset["matched_pairs"], dataset["sas_only"], dataset["python_only"]) == (
        0,
        1440,
        1440,
    )
    detail_path = tmp_path / "family-out" / summary3["detail_links"][dataset["id"]]
    records = [json.loads(line) for line in detail_path.read_text().splitlines()]
    assert len(records) == 2880
    assert all(record["status"] == "FAIL" for record in records)
    assert {record["reason"] for record in records} == {"only_sas", "only_python"}


def test_all_null_nested_column_is_dataset_error(tmp_path: Path) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    sas, python = _roots(tmp_path)
    fixture = Path(__file__).resolve().parents[1] / ".parity-fixtures" / "productsales.sas7bdat"
    sas_file = sas / "dplocal" / "nested.sas7bdat"
    shutil.copyfile(fixture, sas_file)
    frame = polars_readstat.ScanReadstat(str(sas_file)).df.collect()
    table = pa.table({name: frame[name].to_arrow() for name in frame.columns if name != "COUNTRY"})
    nested = pa.array([None] * frame.height, type=pa.list_(pa.int64()))
    table = table.append_column("EXTRA", nested)
    pq.write_table(table, python / "dplocal" / "nested.parquet")
    assert run(RunConfig(sas, python, tmp_path / "out")) == 2
    summary = json.loads((tmp_path / "out" / "summary.json").read_text())
    dataset = summary["datasets"][0]
    assert dataset["status"] == "ERROR"
    assert dataset["detail_complete"] is False


def test_all_null_struct_extra_column_is_dataset_error(tmp_path: Path) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    sas, python = _roots(tmp_path)
    fixture = Path(__file__).resolve().parents[1] / ".parity-fixtures" / "productsales.sas7bdat"
    sas_file = sas / "dplocal" / "struct.sas7bdat"
    shutil.copyfile(fixture, sas_file)
    frame = polars_readstat.ScanReadstat(str(sas_file)).df.collect()
    table = pa.table({name: frame[name].to_arrow() for name in frame.columns if name != "COUNTRY"})
    struct_type = pa.struct([pa.field("child", pa.int64())])
    table = table.append_column("EXTRA", pa.array([None] * frame.height, type=struct_type))
    pq.write_table(table, python / "dplocal" / "struct.parquet")
    assert run(RunConfig(sas, python, tmp_path / "out")) == 2
    dataset = json.loads((tmp_path / "out" / "summary.json").read_text())["datasets"][0]
    assert dataset["status"] == "ERROR"
    assert dataset["detail_complete"] is False


def test_one_sided_detail_values_use_typed_envelopes(tmp_path: Path) -> None:
    from datetime import date

    import pyarrow as pa
    import pyarrow.parquet as pq

    sas, python = _roots(tmp_path)
    one_sided_path = python / "dplocal" / "only.parquet"
    _write_parquet(
        one_sided_path,
        {
            "amount": [float("nan"), 2.0],
            "when": [date(2025, 1, 2), date(2025, 1, 3)],
            "blob": [bytes([0, 255]), bytes([1, 2])],
        },
    )
    original_table = pq.read_table(one_sided_path)
    ns_values = pa.array(
        [1_234_567_891_234_567_890, 1_234_567_891_234_567_891], type=pa.timestamp("ns")
    )
    pq.write_table(original_table.append_column("instant", ns_values), one_sided_path)
    fixture = Path(__file__).resolve().parents[1] / ".parity-fixtures" / "productsales.sas7bdat"
    shutil.copyfile(fixture, sas / "msoc" / "matched.sas7bdat")
    polars_readstat.ScanReadstat(str(sas / "msoc" / "matched.sas7bdat")).df.collect().write_parquet(
        python / "msoc" / "matched.parquet"
    )
    assert run(RunConfig(sas, python, tmp_path / "out", preview_rows=1)) == 1
    summary = json.loads((tmp_path / "out" / "summary.json").read_text())
    dataset = next(item for item in summary["datasets"] if item["reason"] == "missing_counterpart")
    records = [
        json.loads(line)
        for line in (tmp_path / "out" / summary["detail_links"][dataset["id"]])
        .read_text()
        .splitlines()
    ]
    value = records[0]["values"]
    assert records[0]["dataset_id"] == dataset["id"]
    assert value["amount"] == {"type": "float", "value": "nan", "canonical": "null"}
    assert value["when"] == {"type": "date", "value": "2025-01-02"}
    assert value["blob"] == {"type": "binary", "value": "AP8="}
    by_ordinal = {
        record["staging_row_number"]: record["values"]["instant"]["value"] for record in records
    }
    assert by_ordinal == {
        0: "2009-02-13T23:31:31.234567890",
        1: "2009-02-13T23:31:31.234567891",
    }
    assert summary["preview_truncation"][dataset["id"]]["python"] == 1
    html = (tmp_path / "out" / "index.html").read_text()
    assert "omitted 0 SAS and 1 Parquet mismatches" in html
    assert "NaN" not in (tmp_path / "out" / summary["detail_links"][dataset["id"]]).read_text()


def test_jsonl_typed_values(tmp_path: Path) -> None:
    sas, python = _roots(tmp_path)
    shutil.copyfile(
        Path(__file__).resolve().parents[1] / ".parity-fixtures" / "productsales.sas7bdat",
        sas / "dplocal" / "data.sas7bdat",
    )
    polars_readstat.ScanReadstat(str(sas / "dplocal" / "data.sas7bdat")).df.collect().write_parquet(
        python / "dplocal" / "data.parquet"
    )
    run(RunConfig(sas, python, tmp_path / "out"))
    lines = next((tmp_path / "out" / "details").glob("*.jsonl")).read_text().splitlines()
    rows = (json.loads(line) for line in lines[:200])
    assert any(row["values"]["year"]["canonical"] == "n:1993/1" for row in rows)


def test_html_report_contract(tmp_path: Path) -> None:
    sas, python = _roots(tmp_path)
    shutil.copyfile(
        Path(__file__).resolve().parents[1] / ".parity-fixtures" / "productsales.sas7bdat",
        sas / "dplocal" / "data.sas7bdat",
    )
    polars_readstat.ScanReadstat(str(sas / "dplocal" / "data.sas7bdat")).df.collect().write_parquet(
        python / "dplocal" / "data.parquet"
    )
    run(RunConfig(sas, python, tmp_path / "out"))
    html = (tmp_path / "out" / "index.html").read_text()
    assert "Sentinel parity report" in html and "<details>" in html and "details/" in html
    assert "https://" not in html


def test_preview_zero_still_shows_mismatch_counts(tmp_path: Path) -> None:
    sas, python = _roots(tmp_path)
    _write_parquet(python / "dplocal" / "only.parquet", {"value": [1, 2]})
    fixture = Path(__file__).resolve().parents[1] / ".parity-fixtures" / "productsales.sas7bdat"
    shutil.copyfile(fixture, sas / "msoc" / "matched.sas7bdat")
    polars_readstat.ScanReadstat(str(sas / "msoc" / "matched.sas7bdat")).df.collect().write_parquet(
        python / "msoc" / "matched.parquet"
    )
    assert run(RunConfig(sas, python, tmp_path / "out", preview_rows=0)) == 1
    summary = json.loads((tmp_path / "out" / "summary.json").read_text())
    one_sided = next(
        item for item in summary["datasets"] if item["reason"] == "missing_counterpart"
    )
    assert summary["preview_truncation"][one_sided["id"]]["python"] == 2
    html = (tmp_path / "out" / "index.html").read_text()
    assert "omitted 0 SAS and 2 Parquet mismatches" in html
    assert "<table" not in html


def test_html_escaping(tmp_path: Path) -> None:
    sas, python = _roots(tmp_path)
    shutil.copyfile(
        Path(__file__).resolve().parents[1] / ".parity-fixtures" / "productsales.sas7bdat",
        sas / "dplocal" / "fake.sas7bdat",
    )
    sas_frame = polars_readstat.ScanReadstat(str(sas / "dplocal" / "fake.sas7bdat")).df.collect()
    python_frame = sas_frame.with_columns(pl.lit("<img src=x>").alias("COUNTRY"))
    python_frame.write_parquet(python / "dplocal" / "fake.parquet")
    assert run(RunConfig(sas, python, tmp_path / "out")) == 1
    html = (tmp_path / "out" / "index.html").read_text()
    assert r"\u003cimg src=x\u003e" in html
    assert "<img src=x>" not in html


def test_summary_detail_reconciliation(tmp_path: Path) -> None:
    sas, python = _roots(tmp_path)
    shutil.copyfile(
        Path(__file__).resolve().parents[1] / ".parity-fixtures" / "productsales.sas7bdat",
        sas / "dplocal" / "data.sas7bdat",
    )
    polars_readstat.ScanReadstat(str(sas / "dplocal" / "data.sas7bdat")).df.collect().write_parquet(
        python / "dplocal" / "data.parquet"
    )
    run(RunConfig(sas, python, tmp_path / "out"))
    summary = json.loads((tmp_path / "out" / "summary.json").read_text())
    dataset = summary["datasets"][0]
    assert dataset["matched_pairs"] * 2 == len(
        Path(tmp_path / "out" / summary["detail_links"][dataset["id"]]).read_text().splitlines()
    )


def test_exit_code_precedence(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    sas, python = _roots(tmp_path)
    shutil.copyfile(
        Path(__file__).resolve().parents[1] / ".parity-fixtures" / "productsales.sas7bdat",
        sas / "dplocal" / "secret.sas7bdat",
    )
    (python / "dplocal" / "secret.parquet").write_bytes(b"corrupt")
    shutil.copyfile(
        Path(__file__).resolve().parents[1] / ".parity-fixtures" / "productsales.sas7bdat",
        sas / "msoc" / "cell-sentinel.sas7bdat",
    )
    sentinel_frame = (
        polars_readstat.ScanReadstat(str(sas / "msoc" / "cell-sentinel.sas7bdat"))
        .df.collect()
        .with_columns(pl.lit("sensitive-cell-sentinel").alias("COUNTRY"))
    )
    sentinel_frame.write_parquet(python / "msoc" / "cell-sentinel.parquet")
    cli_result = CliRunner().invoke(
        app,
        [
            "run",
            "--sas-root",
            str(sas),
            "--python-root",
            str(python),
            "--output-dir",
            str(tmp_path / "out"),
        ],
    )
    assert cli_result.exit_code == 2
    captured = capsys.readouterr()
    output = captured.out + captured.err + cli_result.stdout + cli_result.stderr
    assert "secret" not in output
    assert "sensitive-cell-sentinel" not in output
    summary = json.loads((tmp_path / "out" / "summary.json").read_text())
    sentinel_dataset = next(
        dataset
        for dataset in summary["datasets"]
        if dataset["name"] == "msoc/cell-sentinel.sas7bdat"
    )
    details_path = tmp_path / "out" / summary["detail_links"][sentinel_dataset["id"]]
    assert "sensitive-cell-sentinel" in details_path.read_text()


def test_input_and_output_safety(tmp_path: Path) -> None:
    sas, python = _roots(tmp_path)
    source = Path(__file__).resolve().parents[1] / ".parity-fixtures" / "productsales.sas7bdat"
    sas_file = sas / "dplocal" / "data.sas7bdat"
    shutil.copyfile(source, sas_file)
    py_file = python / "dplocal" / "data.parquet"
    polars_readstat.ScanReadstat(str(sas_file)).df.collect().write_parquet(py_file)
    initial_hashes = {
        str(path): hashlib.sha256(path.read_bytes()).hexdigest() for path in (sas_file, py_file)
    }
    for input_root in (sas, python):
        with pytest.raises(ValueError, match="outside input roots"):
            run(RunConfig(sas, python, input_root / "nested-report"))
        with pytest.raises(ValueError, match="outside input roots"):
            run(
                RunConfig(
                    sas, python, tmp_path / f"out-{input_root.name}", temp_dir=input_root / "tmp"
                )
            )

    nonempty = tmp_path / "nonempty-output"
    nonempty.mkdir()
    sentinel = nonempty / "keep.txt"
    sentinel.write_bytes(b"keep exactly")
    before = sentinel.read_bytes()
    with pytest.raises(ValueError, match="empty"):
        run(RunConfig(sas, python, nonempty))
    assert sentinel.read_bytes() == before
    symlink = sas / "dplocal" / "linked.sas7bdat"
    symlink.symlink_to(sas_file)
    with pytest.raises(ValueError, match="symlinks"):
        discover(sas, ".sas7bdat")
    assert {
        str(path): hashlib.sha256(path.read_bytes()).hexdigest() for path in (sas_file, py_file)
    } == initial_hashes


def test_corrupt_dataset_continues(tmp_path: Path) -> None:
    sas, python = _roots(tmp_path)
    (sas / "dplocal" / "bad.sas7bdat").write_bytes(b"bad")
    _write_parquet(python / "dplocal" / "bad.parquet", {"x": [1]})
    shutil.copyfile(
        Path(__file__).resolve().parents[1] / ".parity-fixtures" / "productsales.sas7bdat",
        sas / "msoc" / "good.sas7bdat",
    )
    polars_readstat.ScanReadstat(str(sas / "msoc" / "good.sas7bdat")).df.collect().write_parquet(
        python / "msoc" / "good.parquet"
    )
    assert run(RunConfig(sas, python, tmp_path / "out")) == 2
    summary = json.loads((tmp_path / "out" / "summary.json").read_text())
    assert {dataset["status"] for dataset in summary["datasets"]} == {"ERROR", "PASS"}


def test_corrupt_one_sided_dataset_is_reported_and_continues(tmp_path: Path) -> None:
    sas, python = _roots(tmp_path)
    (sas / "dplocal" / "bad.sas7bdat").write_bytes(b"not a SAS file")
    fixture = Path(__file__).resolve().parents[1] / ".parity-fixtures" / "productsales.sas7bdat"
    shutil.copyfile(fixture, sas / "msoc" / "healthy.sas7bdat")
    _write_parquet(python / "msoc" / "healthy.parquet", {"different": [1]})
    code = run(RunConfig(sas, python, tmp_path / "out"))
    assert code == 2
    summary = json.loads((tmp_path / "out" / "summary.json").read_text())
    bad = next(
        dataset for dataset in summary["datasets"] if dataset["name"] == "dplocal/bad.sas7bdat"
    )
    healthy = next(
        dataset for dataset in summary["datasets"] if dataset["name"] == "msoc/healthy.sas7bdat"
    )
    assert bad["status"] == "ERROR" and bad["detail_complete"] is False
    assert healthy["status"] == "FAIL" and healthy["reason"] == "schema_columns_differ"
    assert healthy["id"] in summary["detail_links"]


def test_report_write_failure(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    sas, python = _roots(tmp_path)
    shutil.copyfile(
        Path(__file__).resolve().parents[1] / ".parity-fixtures" / "productsales.sas7bdat",
        sas / "dplocal" / "data.sas7bdat",
    )
    polars_readstat.ScanReadstat(str(sas / "dplocal" / "data.sas7bdat")).df.collect().write_parquet(
        python / "dplocal" / "data.parquet"
    )
    out = tmp_path / "out"
    import sentinel_parity.io.report_writer as report_writer

    def fail_html_write(path: Path, text: str) -> None:
        if path.name == "index.html":
            raise OSError("injected atomic report write failure")
        original_atomic_text(path, text)

    original_atomic_text = report_writer._atomic_text
    monkeypatch.setattr(report_writer, "_atomic_text", fail_html_write)
    result = runner.invoke(
        app,
        [
            "run",
            "--sas-root",
            str(sas),
            "--python-root",
            str(python),
            "--output-dir",
            str(out),
        ],
    )
    assert result.exit_code == 2
    assert "operation failed (OSError)" in result.stderr
    assert not (out / "index.html").exists()
    assert not (out / "summary.json").exists()
    assert not list(out.glob(".index.html.*"))


@pytest.mark.parametrize("signal_name", ["SIGINT", "SIGTERM"])
def test_interruption_cleanup(tmp_path: Path, signal_name: str) -> None:
    sas, python = _roots(tmp_path)
    shutil.copyfile(
        Path(__file__).resolve().parents[1] / ".parity-fixtures" / "productsales.sas7bdat",
        sas / "dplocal" / "signal.sas7bdat",
    )
    polars_readstat.ScanReadstat(
        str(sas / "dplocal" / "signal.sas7bdat")
    ).df.collect().write_parquet(python / "dplocal" / "signal.parquet")
    owned_temp = tmp_path / "owned-temp"
    owned_temp.mkdir()
    progress = tmp_path / "progress.json"
    script = f"""\
import json, sys, time
from pathlib import Path
import sentinel_parity.runner as runner_module
from typer.testing import CliRunner
from sentinel_parity.cli import main
original_stage = runner_module.stage
progress = Path({str(progress)!r})
def stage_with_progress(*args, **kwargs):
    result = original_stage(*args, **kwargs)
    progress.write_text(json.dumps({{"path": str(result["path"]), "rows": result["rows"]}}))
    time.sleep(30)
    return result
runner_module.stage = stage_with_progress
sys.argv = ["parity", "run", "--sas-root", {str(sas)!r}, "--python-root", {str(python)!r},
    "--output-dir", {str(tmp_path / "out")!r}, "--temp-dir", {str(owned_temp)!r}]
main()
"""
    process = subprocess.Popen(
        [sys.executable, "-c", script], cwd=Path(__file__).resolve().parents[1]
    )
    import signal
    import time

    deadline = time.monotonic() + 30
    while not progress.exists() and process.poll() is None and time.monotonic() < deadline:
        time.sleep(0.01)
    assert progress.exists()
    progress_data = json.loads(progress.read_text())
    assert progress_data["rows"] == 1440
    assert Path(progress_data["path"]).is_file()
    process.send_signal(getattr(signal, signal_name))
    process.wait(timeout=30)
    assert process.returncode != 0
    assert list(owned_temp.iterdir()) == []


def test_bounded_batch_pipeline(tmp_path: Path) -> None:
    sas, python = _roots(tmp_path)
    sas_file = Path(__file__).resolve().parents[1] / ".parity-fixtures" / "productsales.sas7bdat"
    result = stage(sas_file, "sas", tmp_path, 17)
    assert result["rows"] == 1440
    assert result["path"].is_file()
    assert 0 < result["observed_max_batch_size"] <= 17

    parquet_source = tmp_path / "many.parquet"
    pl.DataFrame({"value": list(range(103))}).write_parquet(parquet_source)
    parquet_result = stage(parquet_source, "python", tmp_path, 17)
    assert parquet_result["rows"] == 103
    assert 0 < parquet_result["observed_max_batch_size"] <= 17


@pytest.mark.slow
def test_disk_backed_comparison_smoke(tmp_path: Path) -> None:
    rows = 250_000
    frame = pl.DataFrame(
        {"id": [i % 97 for i in range(rows)], "payload": [f"v{i % 13}" for i in range(rows)]}
    )
    left_file = tmp_path / "left.parquet"
    right_file = tmp_path / "right.parquet"
    frame.write_parquet(left_file)
    frame.sample(fraction=1.0, shuffle=True, seed=42).write_parquet(right_file)
    work = tmp_path / "work"
    work.mkdir()
    left = stage(left_file, "python", work, batch_size=4096)
    right = stage(right_file, "python", work, batch_size=4096)
    from sentinel_parity.io.duckdb_comparison import compare

    result = compare(left, right, work, "128MB", "2GB", "workload-dataset")
    assert result["status"] == "PASS"
    assert left["describe"] and right["describe"]
    assert result["matched"] == rows
    assert result["sas_only"] == result["python_only"] == 0
    assert len(Path(result["details_path"]).read_text().splitlines()) == 2 * rows
    spills = list(work.glob("spill-*"))
    assert len(spills) == 1 and spills[0].is_dir()
    assert sum(path.stat().st_size for path in work.rglob("*") if path.is_file()) > 0


def test_except_all_disagreement_is_dataset_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import sentinel_parity.io.duckdb_comparison as comparison

    sas, python = _roots(tmp_path)
    fixture = Path(__file__).resolve().parents[1] / ".parity-fixtures" / "productsales.sas7bdat"
    shutil.copyfile(fixture, sas / "dplocal" / "sample.sas7bdat")
    polars_readstat.ScanReadstat(
        str(sas / "dplocal" / "sample.sas7bdat")
    ).df.collect().write_parquet(python / "dplocal" / "sample.parquet")
    original_count = comparison._except_count
    calls = 0

    def disagree(connection: object, left: str, right: str, keycols: list[str]) -> int:
        nonlocal calls
        calls += 1
        actual = original_count(connection, left, right, keycols)  # type: ignore[arg-type]
        return actual + (1 if calls == 1 else 0)

    monkeypatch.setattr(comparison, "_except_count", disagree)
    assert run(RunConfig(sas, python, tmp_path / "out")) == 2
    summary = json.loads((tmp_path / "out" / "summary.json").read_text())
    dataset = summary["datasets"][0]
    assert dataset["status"] == "ERROR"
    assert dataset["reason"] == "RuntimeError"
    assert dataset["detail_complete"] is False
    assert dataset["id"] not in summary["detail_links"]


def test_resource_exhaustion_is_error(tmp_path: Path) -> None:
    rows = 250_000
    frame = pl.DataFrame(
        {"key": [f"{i:08d}" for i in range(rows)], "blob": ["x" * 128 for _ in range(rows)]}
    )
    left_file, right_file = tmp_path / "left.parquet", tmp_path / "right.parquet"
    frame.write_parquet(left_file)
    frame.with_columns(pl.col("key").reverse()).write_parquet(right_file)
    work = tmp_path / "work"
    work.mkdir()
    left = stage(left_file, "python", work, batch_size=1024)
    right = stage(right_file, "python", work, batch_size=1024)
    from duckdb import OutOfMemoryException

    from sentinel_parity.io.duckdb_comparison import compare

    with pytest.raises(OutOfMemoryException):
        compare(left, right, work, "1MB", "1KB", "resource-dataset")


def test_distribution_contents() -> None:
    assert (
        Path(__file__).resolve().parents[1] / "src/sentinel_parity/resources/report.html"
    ).is_file()
    assert ".parity-fixtures" in (Path(__file__).resolve().parents[1] / ".gitignore").read_text()


def test_numeric_values_match_across_widths_but_exact_value_decides(tmp_path: Path) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    from sentinel_parity.io.duckdb_comparison import compare
    from sentinel_parity.io.staging import stage

    left_path, right_path = tmp_path / "left.parquet", tmp_path / "right.parquet"
    pq.write_table(pa.table({"n": pa.array([16.0, 16.2], type=pa.float64())}), left_path)
    pq.write_table(pa.table({"n": pa.array([16, 16.0], type=pa.int64())}), right_path)
    work = tmp_path / "work"
    work.mkdir()
    result = compare(
        stage(left_path, "python", work),
        stage(right_path, "python", work),
        work,
        "128MB",
        "2GB",
        "numeric-value-test",
    )
    # 16.0 = 16 matches; 16.2 != 16 decides, regardless of declared width.
    assert (result["status"], result["matched"]) == ("FAIL", 1)
    assert (result["sas_only"], result["python_only"]) == (1, 1)


def test_boolean_matches_numeric_values_across_types(tmp_path: Path) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    from sentinel_parity.io.duckdb_comparison import compare
    from sentinel_parity.io.staging import stage

    numeric_path = tmp_path / "numeric.parquet"
    pq.write_table(pa.table({"flag": pa.array([1, 0, 1, 0], type=pa.int64())}), numeric_path)

    def boolean_parquet(name: str, values: list[bool]) -> Path:
        path = tmp_path / f"{name}.parquet"
        pq.write_table(pa.table({"flag": pa.array(values, type=pa.bool_())}), path)
        return path

    work = tmp_path / "work"
    work.mkdir()
    staged_numeric = stage(numeric_path, "python", work)
    matching = compare(
        staged_numeric,
        stage(boolean_parquet("matching", [True, False, True, False]), "python", work),
        work,
        "128MB",
        "2GB",
        "bool-matching",
    )
    assert (matching["status"], matching["matched"]) == ("PASS", 4)
    mismatching = compare(
        staged_numeric,
        stage(boolean_parquet("mismatching", [True, False, True, True]), "python", work),
        work,
        "128MB",
        "2GB",
        "bool-mismatching",
    )
    assert (mismatching["status"], mismatching["matched"]) == ("FAIL", 3)
    assert (mismatching["sas_only"], mismatching["python_only"]) == (1, 1)


def test_sas_trailing_blank_padding_matches(tmp_path: Path) -> None:
    sas, python = _roots(tmp_path)
    fixture = Path(__file__).resolve().parents[1] / ".parity-fixtures" / "productsales.sas7bdat"
    shutil.copyfile(fixture, sas / "dplocal" / "padded.sas7bdat")
    frame = polars_readstat.ScanReadstat(str(sas / "dplocal" / "padded.sas7bdat")).df.collect()
    padded = frame.with_columns(pl.concat_str(pl.col("COUNTRY"), pl.lit("  ")).alias("COUNTRY"))
    padded.write_parquet(python / "dplocal" / "padded.parquet")
    assert run(RunConfig(sas, python, tmp_path / "out")) == 0
    dataset = json.loads((tmp_path / "out" / "summary.json").read_text())["datasets"][0]
    assert dataset["status"] == "PASS"


def test_round_digits_option_changes_comparison(tmp_path: Path) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    from sentinel_parity.io.duckdb_comparison import compare
    from sentinel_parity.io.staging import stage

    left_path, right_path = tmp_path / "left.parquet", tmp_path / "right.parquet"
    pq.write_table(pa.table({"n": pa.array([16.24681, 5.0], type=pa.float64())}), left_path)
    pq.write_table(pa.table({"n": pa.array([16.2468, 5.0], type=pa.float64())}), right_path)
    work = tmp_path / "work"
    work.mkdir()
    raw = compare(
        stage(left_path, "python", work),
        stage(right_path, "python", work),
        work,
        "128MB",
        "2GB",
        "round-raw",
    )
    assert (raw["status"], raw["matched"]) == ("FAIL", 1)
    rounded = compare(
        stage(left_path, "python", work, round_digits=4),
        stage(right_path, "python", work, round_digits=4),
        work,
        "128MB",
        "2GB",
        "round-4",
    )
    # 16.24681 and 16.2468 agree at 4 decimal places.
    assert (rounded["status"], rounded["matched"]) == ("PASS", 2)


def test_datasets_are_reported_in_name_order(tmp_path: Path) -> None:
    sas, python = _roots(tmp_path)
    fixture = Path(__file__).resolve().parents[1] / ".parity-fixtures" / "productsales.sas7bdat"
    shutil.copyfile(fixture, sas / "dplocal" / "matched.sas7bdat")
    polars_readstat.ScanReadstat(
        str(sas / "dplocal" / "matched.sas7bdat")
    ).df.collect().write_parquet(python / "dplocal" / "matched.parquet")
    shutil.copyfile(fixture, sas / "msoc" / "lonely.sas7bdat")
    assert run(RunConfig(sas, python, tmp_path / "out")) == 1
    names = [
        item["name"]
        for item in json.loads((tmp_path / "out" / "summary.json").read_text())["datasets"]
    ]
    assert names == sorted(names, key=str.casefold)
    assert names[0].startswith("dplocal/")


def test_round_digits_config_and_cli(tmp_path: Path) -> None:
    config = tmp_path / "run.toml"
    config.write_text('sas_root="sas"\npython_root="python"\nround_digits=4\noutput_dir="out"\n')
    assert load_config(config, {}).round_digits == 4
    bad = tmp_path / "bad.toml"
    bad.write_text('sas_root="sas"\npython_root="python"\nround_digits=0\noutput_dir="out"\n')
    with pytest.raises(ValueError):
        load_config(bad, {})

    sas, python = _roots(tmp_path / "run")
    fixture = Path(__file__).resolve().parents[1] / ".parity-fixtures" / "productsales.sas7bdat"
    shutil.copyfile(fixture, sas / "dplocal" / "data.sas7bdat")
    polars_readstat.ScanReadstat(str(sas / "dplocal" / "data.sas7bdat")).df.collect().write_parquet(
        python / "dplocal" / "data.parquet"
    )
    result = runner.invoke(
        app,
        [
            "run",
            "--sas-root",
            str(sas),
            "--python-root",
            str(python),
            "--output-dir",
            str(tmp_path / "cli-out"),
            "--round",
            "2",
        ],
    )
    assert result.exit_code == 0
    summary = json.loads((tmp_path / "cli-out" / "summary.json").read_text())
    assert summary["limits"]["round_digits"] == 2


def test_row_order_mismatch_is_flagged(tmp_path: Path) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    from sentinel_parity.io.duckdb_comparison import compare
    from sentinel_parity.io.staging import stage

    left_path, right_path = tmp_path / "left.parquet", tmp_path / "right.parquet"
    pq.write_table(pa.table({"a": [1, 2, 3], "b": [10, 20, 30]}), left_path)
    pq.write_table(pa.table({"a": [3, 1, 2], "b": [30, 10, 20]}), right_path)
    work = tmp_path / "work"
    work.mkdir()
    reordered = compare(
        stage(left_path, "python", work),
        stage(right_path, "python", work),
        work,
        "128MB",
        "2GB",
        "order-differs",
    )
    # Same multiset, different file order: PASS, but the flag fires.
    assert (reordered["status"], reordered["matched"]) == ("PASS", 3)
    assert reordered["order_mismatches"] == 3
    ordered = compare(
        stage(left_path, "python", work),
        stage(left_path, "python", work),
        work,
        "128MB",
        "2GB",
        "order-same",
    )
    assert ordered["order_mismatches"] == 0


def test_row_order_flag_in_report(tmp_path: Path) -> None:
    sas, python = _roots(tmp_path)
    fixture = Path(__file__).resolve().parents[1] / ".parity-fixtures" / "productsales.sas7bdat"
    shutil.copyfile(fixture, sas / "dplocal" / "reversed.sas7bdat")
    frame = polars_readstat.ScanReadstat(str(sas / "dplocal" / "reversed.sas7bdat")).df.collect()
    frame.reverse().write_parquet(python / "dplocal" / "reversed.parquet")
    assert run(RunConfig(sas, python, tmp_path / "out")) == 0
    dataset = json.loads((tmp_path / "out" / "summary.json").read_text())["datasets"][0]
    assert dataset["status"] == "PASS"
    assert dataset["row_order_mismatches"] == 1440
    html = (tmp_path / "out" / "index.html").read_text()
    assert "Row order differs from the Parquet file" in html


def test_type_crossed_columns_warn_instead_of_fail(tmp_path: Path) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    from sentinel_parity.io.duckdb_comparison import compare
    from sentinel_parity.io.staging import stage

    left_path, right_path = tmp_path / "left.parquet", tmp_path / "right.parquet"
    pq.write_table(pa.table({"n": [1, 2], "keep": ["a", "b"]}), left_path)
    pq.write_table(pa.table({"n": ["1", "x"], "keep": ["a", "b"]}), right_path)
    work = tmp_path / "work"
    work.mkdir()
    result = compare(
        stage(left_path, "python", work),
        stage(right_path, "python", work),
        work,
        "128MB",
        "2GB",
        "warn-test",
    )
    # The only mismatch is text "x" vs numeric 2 inside a character-vs-
    # numeric column; the same-typed column matches fully.
    assert result["status"] == "WARN"
    assert (result["matched"], result["sas_only"], result["python_only"]) == (1, 1, 1)
    assert result["type_mismatched_columns"] == ["n"]

    pq.write_table(pa.table({"n": [1, 2], "keep": ["a", "BAD"]}), left_path)
    mixed = compare(
        stage(left_path, "python", work),
        stage(right_path, "python", work),
        work,
        "128MB",
        "2GB",
        "mixed-test",
    )
    # A same-typed value mismatch is still a FAIL.
    assert mixed["status"] == "FAIL"


def test_type_crossed_warn_run(tmp_path: Path) -> None:
    sas, python = _roots(tmp_path)
    fixture = Path(__file__).resolve().parents[1] / ".parity-fixtures" / "productsales.sas7bdat"
    shutil.copyfile(fixture, sas / "dplocal" / "warned.sas7bdat")
    frame = polars_readstat.ScanReadstat(str(sas / "dplocal" / "warned.sas7bdat")).df.collect()
    altered = frame.with_columns(
        pl.when(pl.int_range(0, pl.len()) == 0)
        .then(pl.lit("unknown"))
        .otherwise(pl.col("YEAR").cast(pl.String))
        .alias("YEAR")
    )
    altered.write_parquet(python / "dplocal" / "warned.parquet")
    assert run(RunConfig(sas, python, tmp_path / "out")) == 0
    summary = json.loads((tmp_path / "out" / "summary.json").read_text())
    dataset = summary["datasets"][0]
    assert dataset["status"] == "WARN"
    assert summary["status"] == "WARN"
    assert dataset["type_mismatched_columns"] == ["year"]
    html = (tmp_path / "out" / "index.html").read_text()
    assert "Character vs numeric columns: year" in html
    assert "WARN" in html


def test_missing_and_text_forms_match_across_types(tmp_path: Path) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    from sentinel_parity.io.duckdb_comparison import compare
    from sentinel_parity.io.staging import stage

    left_path, right_path = tmp_path / "left.parquet", tmp_path / "right.parquet"
    pq.write_table(
        pa.table(
            {
                "t": pa.array(["CANADA ", "zz"]),
                "n": pa.array([8.0, 1.5], type=pa.float64()),
                "m": pa.array([float("nan"), None], type=pa.float64()),
            }
        ),
        left_path,
    )
    pq.write_table(
        pa.table(
            {
                "t": pa.array(["CANADA", "zz "]),
                "n": pa.array(["8", "1.5"], type=pa.large_string()),
                "m": pa.array([None, None], type=pa.float64()),
            }
        ),
        right_path,
    )
    work = tmp_path / "work"
    work.mkdir()
    result = compare(
        stage(left_path, "python", work),
        stage(right_path, "python", work),
        work,
        "128MB",
        "2GB",
        "text-missing-forms-test",
    )
    # SAS trailing blank padding, blank-vs-null, NaN-vs-null, and numeric
    # text all compare as equal values.
    assert (result["status"], result["matched"], result["sas_only"], result["python_only"]) == (
        "PASS",
        2,
        0,
        0,
    )


def test_unmatched_rows_carry_differing_columns(tmp_path: Path) -> None:
    import json

    import pyarrow as pa
    import pyarrow.parquet as pq

    from sentinel_parity.io.duckdb_comparison import compare
    from sentinel_parity.io.staging import stage

    left_path, right_path = tmp_path / "left.parquet", tmp_path / "right.parquet"
    pq.write_table(
        pa.table({"a": [1, 2], "b": [10, 20], "c": [5, 6]}),
        left_path,
    )
    pq.write_table(
        pa.table({"a": [1, 2], "b": [10, 99], "c": [5, 6]}),
        right_path,
    )
    work = tmp_path / "work"
    work.mkdir()
    result = compare(
        stage(left_path, "python", work),
        stage(right_path, "python", work),
        work,
        "128MB",
        "2GB",
        "diff-attribution-test",
    )
    assert result["status"] == "FAIL"
    records = [json.loads(line) for line in Path(result["details_path"]).read_text().splitlines()]
    failures = [record for record in records if record["status"] == "FAIL"]
    assert len(failures) == 2
    assert all(record["differing_columns"] == ["b"] for record in failures)
    assert {record["side"] for record in failures} == {"sas", "python"}
    pair_ids = {record["pair_id"] for record in failures}
    assert len(pair_ids) == 1 and None not in pair_ids
