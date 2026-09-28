import json
import shutil
import zipfile
from pathlib import Path

import polars as pl
import pytest

import sentinel_parity.runner as runner_module
from sentinel_parity.config import RunConfig
from sentinel_parity.runner import run

FIXTURE = Path(__file__).resolve().parents[1] / ".parity-fixtures" / "productsales.sas7bdat"


def _inputs(tmp_path: Path, *, differences: bool = True) -> tuple[Path, Path]:
    if not FIXTURE.is_file():
        pytest.fail(
            "external fixtures unavailable; run `python -m tests.acquire_fixtures` explicitly"
        )
    sas, python = tmp_path / "sas", tmp_path / "python"
    (sas / "dplocal").mkdir(parents=True)
    (sas / "msoc").mkdir()
    (python / "dplocal").mkdir(parents=True)
    (python / "msoc").mkdir()
    shutil.copyfile(FIXTURE, sas / "dplocal" / "source.sas7bdat")
    frame = (
        pl.read_parquet(".parity-fixtures/productsales.parquet")
        if Path(".parity-fixtures/productsales.parquet").is_file()
        else None
    )
    if frame is None:
        import polars_readstat

        frame = polars_readstat.ScanReadstat(str(sas / "dplocal" / "source.sas7bdat")).df.collect()
    if differences:
        frame = frame.with_columns((pl.col("ACTUAL") + 1).alias("ACTUAL"))
    frame.write_parquet(python / "dplocal" / "source.parquet")
    return sas, python


def _run(sas: Path, python: Path, out: Path, **kwargs: object) -> int:
    return run(RunConfig(sas, python, out, **kwargs))


def test_excel_runner_generates_index_and_text_sheets(tmp_path: Path) -> None:
    sas, python = _inputs(tmp_path)
    unmatched = sas / "msoc" / "only.sas7bdat"
    shutil.copyfile(FIXTURE, unmatched)
    out = tmp_path / "out"
    assert _run(sas, python, out) == 1
    summary = json.loads((out / "summary.json").read_text())
    assert summary["excel"]["status"] == "generated"
    workbook = out / summary["excel"]["path"]
    assert workbook.is_file()
    with zipfile.ZipFile(workbook) as archive:
        names = [
            item for item in archive.read("xl/workbook.xml").decode().split('<sheet name="')[1:]
        ]
        index_xml = archive.read("xl/worksheets/sheet1.xml").decode()
        data_xml = archive.read("xl/worksheets/sheet2.xml").decode()
        assert "Index" in archive.read("xl/workbook.xml").decode()
        assert "dplocal/source.sas7bdat" in index_xml
        assert "msoc/only.sas7bdat" in index_xml
        assert 't="inlineStr"' in index_xml and 't="inlineStr"' in data_xml
        assert "1" in data_xml and "2" in data_xml
        assert "<f>" not in data_xml and "<hyperlinks" not in data_xml
        assert len(names) >= 2
    assert all("workbook_stage_path" not in item for item in summary["datasets"])


def test_excel_disabled_no_differences_and_omission(tmp_path: Path) -> None:
    sas, python = _inputs(tmp_path, differences=False)
    out = tmp_path / "disabled"
    assert _run(sas, python, out, excel=False) == 0
    disabled = json.loads((out / "summary.json").read_text())["excel"]
    assert disabled["status"] == "disabled"
    assert disabled["reasons"] == []
    sas2, python2 = _inputs(tmp_path / "same", differences=False)
    out2 = tmp_path / "same-out"
    assert _run(sas2, python2, out2) == 0
    assert json.loads((out2 / "summary.json").read_text())["excel"]["status"] == "no_differences"
    sas3, python3 = _inputs(tmp_path / "omitted")
    out3 = tmp_path / "omitted-out"
    assert _run(sas3, python3, out3, excel_max_bytes=1) == 1
    summary = json.loads((out3 / "summary.json").read_text())
    assert summary["excel"]["status"] == "omitted"
    assert summary["excel"]["reasons"]
    assert all(set(reason) == {"code", "dataset_id"} for reason in summary["excel"]["reasons"])
    assert not (out3 / "differences.xlsx").exists()
    assert "omitted" in (out3 / "index.html").read_text()


def test_excel_disabled_reports_zero_workbook_measurements(tmp_path: Path) -> None:
    sas, python = _inputs(tmp_path)
    out = tmp_path / "out"
    assert _run(sas, python, out, excel=False) == 1
    excel = json.loads((out / "summary.json").read_text())["excel"]
    assert excel["status"] == "disabled"
    # No workbook can be staged, so the per-dataset scan is skipped; only the
    # cheap Index projection remains in the measurements.
    assert excel["measurements"]["rows"] == 0


def _second_pair(sas: Path, python: Path) -> None:
    shutil.copyfile(FIXTURE, sas / "msoc" / "second.sas7bdat")
    frame = pl.read_parquet(python / "dplocal" / "source.parquet")
    frame.write_parquet(python / "msoc" / "second.parquet")


def test_excel_rows_per_sheet_bounds_datasets_not_run_total(tmp_path: Path) -> None:
    sas, python = _inputs(tmp_path)
    _second_pair(sas, python)
    probe = tmp_path / "probe"
    assert _run(sas, python, probe) == 1
    sheet_rows = [
        item["workbook_measurements"]["rows"]
        for item in json.loads((probe / "summary.json").read_text())["datasets"]
    ]
    assert len(sheet_rows) == 2 and all(rows > 0 for rows in sheet_rows)
    limit = max(sheet_rows)
    assert sum(sheet_rows) > limit

    sas2, python2 = _inputs(tmp_path / "bounded")
    _second_pair(sas2, python2)
    out = tmp_path / "bounded-out"
    assert _run(sas2, python2, out, excel_max_rows_per_sheet=limit) == 1
    excel = json.loads((out / "summary.json").read_text())["excel"]
    assert excel["status"] == "generated"
    assert (out / "differences.xlsx").is_file()


def test_final_gate_omission_releases_staging(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    sas, python = _inputs(tmp_path)
    _second_pair(sas, python)
    long_stem = "e" * 100
    shutil.copyfile(FIXTURE, sas / "msoc" / f"{long_stem}.sas7bdat")
    third = pl.read_parquet(python / "dplocal" / "source.parquet")
    third.write_parquet(python / "msoc" / f"{long_stem}.parquet")
    original_compare = runner_module.compare
    staged: list[Path] = []

    def observe_compare(*args: object, **kwargs: object) -> dict:
        stage_path = kwargs.get("workbook_stage_path")
        if isinstance(stage_path, Path):
            staged.append(stage_path)
        return original_compare(*args, **kwargs)

    monkeypatch.setattr(runner_module, "compare", observe_compare)
    probe = tmp_path / "probe"
    assert _run(sas, python, probe) == 1
    text_bytes = [
        item["workbook_measurements"]["text_bytes"]
        for item in json.loads((probe / "summary.json").read_text())["datasets"]
    ]
    assert len(text_bytes) == 3 and all(size > 0 for size in text_bytes)

    # Each dataset fits the byte budget as it is staged, but the total plus the
    # Index sheet exceeds it, so the final gate omits the workbook.
    staged.clear()
    budget = sum(text_bytes) + 100
    sas2, python2 = _inputs(tmp_path / "bounded")
    _second_pair(sas2, python2)
    shutil.copyfile(FIXTURE, sas2 / "msoc" / f"{long_stem}.sas7bdat")
    third.write_parquet(python2 / "msoc" / f"{long_stem}.parquet")
    out = tmp_path / "out"
    assert _run(sas2, python2, out, excel_max_bytes=budget) == 1
    summary = json.loads((out / "summary.json").read_text())
    assert summary["excel"]["status"] == "omitted"
    assert {"code": "byte_limit", "dataset_id": None} in summary["excel"]["reasons"]
    assert len(staged) == 3
    assert all(not path.exists() for path in staged)


def test_excel_rejection_never_invokes_writer_or_leaves_stage(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    sas, python = _inputs(tmp_path)
    original_compare = runner_module.compare
    stage_calls = 0
    staged_paths: list[Path] = []

    def observe_compare(*args: object, **kwargs: object) -> dict:
        nonlocal stage_calls
        stage_calls += 1
        stage_path = kwargs.get("workbook_stage_path")
        if isinstance(stage_path, Path):
            staged_paths.append(stage_path)
        return original_compare(*args, **kwargs)

    def forbidden_writer(*args: object, **kwargs: object) -> None:
        raise AssertionError("ineligible workbook must not invoke writer")

    monkeypatch.setattr(runner_module, "compare", observe_compare)
    monkeypatch.setattr(runner_module, "write_workbook", forbidden_writer)
    out = tmp_path / "out"
    assert _run(sas, python, out, excel_max_bytes=1) == 1
    summary = json.loads((out / "summary.json").read_text())
    assert summary["excel"]["status"] == "omitted"
    assert {"code": "byte_limit", "dataset_id": None} in summary["excel"]["reasons"]
    assert stage_calls == 1
    assert len(staged_paths) == 1 and not staged_paths[0].exists()


def test_sheet_row_limit_names_only_the_tripping_dataset(tmp_path: Path) -> None:
    sas, python = _inputs(tmp_path)
    _second_pair(sas, python)
    frame = pl.read_parquet(python / "msoc" / "second.parquet")
    pl.concat([frame, frame.head(3)]).write_parquet(python / "msoc" / "second.parquet")
    probe = tmp_path / "probe"
    assert _run(sas, python, probe) == 1
    rows = {
        item["name"]: item["workbook_measurements"]["rows"]
        for item in json.loads((probe / "summary.json").read_text())["datasets"]
    }
    small, large = rows["dplocal/source.sas7bdat"], rows["msoc/second.sas7bdat"]
    assert large > small

    sas2, python2 = _inputs(tmp_path / "bounded")
    _second_pair(sas2, python2)
    frame2 = pl.read_parquet(python2 / "msoc" / "second.parquet")
    pl.concat([frame2, frame2.head(3)]).write_parquet(python2 / "msoc" / "second.parquet")
    out = tmp_path / "out"
    assert _run(sas2, python2, out, excel_max_rows_per_sheet=small) == 1
    summary = json.loads((out / "summary.json").read_text())
    tripper = next(item for item in summary["datasets"] if item["name"] == "msoc/second.sas7bdat")
    assert summary["excel"]["reasons"] == [{"code": "sheet_row_limit", "dataset_id": tripper["id"]}]
    assert summary["excel"]["measurements"]["rows"] == small


@pytest.mark.parametrize("failure", ["open", "write", "close", "publish"])
def test_excel_publish_failure_cleanup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, failure: str
) -> None:
    sas, python = _inputs(tmp_path)
    out = tmp_path / "out"
    if failure == "open":
        import sentinel_parity.io.workbook_writer as workbook_writer

        def fail_open(*args: object, **kwargs: object) -> None:
            raise OSError("injected workbook open failure")

        monkeypatch.setattr(workbook_writer.xlsxwriter, "Workbook", fail_open)
    elif failure == "write":
        import sentinel_parity.io.workbook_writer as workbook_writer

        original = workbook_writer._write_text

        def fail_write(worksheet: object, row: int, column: int, value: object) -> None:
            if row == 1:
                raise OSError("injected workbook write failure")
            original(worksheet, row, column, value)

        monkeypatch.setattr(workbook_writer, "_write_text", fail_write)
    elif failure == "close":
        import sentinel_parity.io.workbook_writer as workbook_writer

        original_workbook = workbook_writer.xlsxwriter.Workbook

        class CloseFailure:
            def __init__(self, *args: object, **kwargs: object) -> None:
                self.wrapped = original_workbook(*args, **kwargs)
                self.close_count = 0

            def __getattr__(self, name: str) -> object:
                return getattr(self.wrapped, name)

            def close(self) -> None:
                self.close_count += 1
                self.wrapped.close()
                if self.close_count == 1:
                    raise OSError("injected workbook close failure")

        monkeypatch.setattr(workbook_writer.xlsxwriter, "Workbook", CloseFailure)
    else:

        def fail_publish(*args: object, **kwargs: object) -> None:
            raise OSError("injected workbook publication failure")

        monkeypatch.setattr(runner_module, "publish", fail_publish)
    with pytest.raises(OSError):
        _run(sas, python, out)
    assert (out / "run.jsonl").is_file()
    assert not (out / "index.html").exists()
    assert not (out / "summary.json").exists()
    assert not (out / "differences.xlsx").exists()
    assert "publish_error" in (out / "run.jsonl").read_text() if failure == "publish" else True
