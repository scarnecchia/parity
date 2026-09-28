import json
import shutil
from pathlib import Path

import polars as pl
import pytest

from sentinel_parity.config import RunConfig
from sentinel_parity.io.schema_config import load_schema
from sentinel_parity.runner import run

FIXTURE = Path(__file__).resolve().parents[1] / ".parity-fixtures" / "productsales.sas7bdat"


def _write_schema(path: Path, content: str) -> Path:
    path.write_text(content, encoding="utf-8")
    return path


def test_load_schema_normalizes_stems(tmp_path: Path) -> None:
    schema = _write_schema(
        tmp_path / "schema.toml",
        '[r01_attrition]\npair_keys = ["group", "level"]\n\n[attrition2]\npair_keys = []\n',
    )

    assert load_schema(schema) == {"attrition": ("group", "level"), "attrition2": ()}


def test_load_schema_rejects_bad_sections(tmp_path: Path) -> None:
    cases = (
        "[t]\nunknown = 1\n",
        "[t]\npair_keys = [1]\n",
        '[t]\npair_keys = [""]\n',
        "[t]\nnot_a_table = 3\n",
        "[r01_t]\npair_keys = []\n\n[t]\npair_keys = []\n",
    )
    for index, content in enumerate(cases):
        schema = _write_schema(tmp_path / f"schema-{index}.toml", content)
        with pytest.raises(ValueError):
            load_schema(schema)


def _setup_tables(tmp_path: Path) -> tuple[Path, Path]:
    if not FIXTURE.is_file():
        pytest.fail(
            "external fixtures unavailable; run `python -m tests.acquire_fixtures` explicitly"
        )
    sas, python = tmp_path / "sas", tmp_path / "python"
    (sas / "dplocal").mkdir(parents=True)
    (sas / "msoc").mkdir(parents=True)
    (python / "dplocal").mkdir(parents=True)
    (python / "msoc").mkdir(parents=True)
    shutil.copyfile(FIXTURE, sas / "dplocal" / "r01_people.sas7bdat")
    shutil.copyfile(FIXTURE, sas / "dplocal" / "plain.sas7bdat")
    if Path(".parity-fixtures/productsales.parquet").is_file():
        frame = pl.read_parquet(".parity-fixtures/productsales.parquet")
    else:
        import polars_readstat

        frame = polars_readstat.ScanReadstat(str(FIXTURE)).df.collect()
    differing = frame.with_columns((pl.col("ACTUAL") + 1).alias("ACTUAL"))
    differing.write_parquet(python / "dplocal" / "people.parquet")
    frame.write_parquet(python / "dplocal" / "plain.parquet")
    return sas, python


def test_schema_toml_assigns_pair_keys_per_table(tmp_path: Path) -> None:
    sas, python = _setup_tables(tmp_path)
    schema = _write_schema(tmp_path / "schema.toml", '[r01_people]\npair_keys = ["ACTUAL"]\n')
    out = tmp_path / "out"

    assert run(RunConfig(sas, python, out, schema=schema)) == 1

    keys = {
        item["name"]: item["pair_keys"]
        for item in json.loads((out / "summary.json").read_text())["datasets"]
    }
    assert keys["dplocal/r01_people.sas7bdat"] == ["actual"]
    assert keys["dplocal/plain.sas7bdat"] == []


def test_schema_toml_prefixless_section_matches_prefixed_table(tmp_path: Path) -> None:
    sas, python = _setup_tables(tmp_path)
    schema = _write_schema(tmp_path / "schema.toml", '[people]\npair_keys = ["ACTUAL"]\n')
    out = tmp_path / "out"

    assert run(RunConfig(sas, python, out, schema=schema)) == 1

    keys = {
        item["name"]: item["pair_keys"]
        for item in json.loads((out / "summary.json").read_text())["datasets"]
    }
    assert keys["dplocal/r01_people.sas7bdat"] == ["actual"]


def test_schema_toml_unknown_section_is_an_error(tmp_path: Path) -> None:
    sas, python = _setup_tables(tmp_path)
    schema = _write_schema(tmp_path / "schema.toml", '[nope]\npair_keys = ["x"]\n')

    with pytest.raises(ValueError, match="matching no discovered table"):
        run(RunConfig(sas, python, tmp_path / "out", schema=schema))
