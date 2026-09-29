from pathlib import Path

import pytest

from sentinel_parity.io.config_loader import load_config

_LIMITS = (
    "preview_rows",
    "preview_max_bytes",
    "preview_total_max_bytes",
    "preview_cell_chars",
    "excel_max_sheets",
    "excel_max_rows",
    "excel_max_rows_per_sheet",
    "excel_max_bytes",
)


def test_documented_config_loads(tmp_path: Path) -> None:
    example = Path(__file__).resolve().parents[1] / "config.toml.example"
    content = example.read_text(encoding="utf-8")
    content = content.replace("../sas-output", str(tmp_path / "sas"))
    content = content.replace("../parquet-output", str(tmp_path / "python"))
    config_path = tmp_path / "config.toml"
    config_path.write_text(content, encoding="utf-8")

    config = load_config(config_path, {})

    assert config.sas_root == (tmp_path / "sas").resolve()
    assert config.python_root == (tmp_path / "python").resolve()
    assert config.preview_rows == 100
    assert config.preview_max_bytes == 1_048_576
    assert config.preview_total_max_bytes == 10_485_760
    assert config.preview_cell_chars == 512
    assert config.excel_max_sheets == 100
    assert config.excel_max_rows == 100_000
    assert config.excel_max_rows_per_sheet == 25_000
    assert config.excel_max_bytes == 104_857_600


@pytest.mark.parametrize("key", _LIMITS)
def test_limit_cli_override_precedes_toml(tmp_path: Path, key: str) -> None:
    config_path = tmp_path / "run.toml"
    config_path.write_text(f'sas_root="sas"\npython_root="python"\n{key}=2\n', encoding="utf-8")

    config = load_config(config_path, {key: 7})

    assert getattr(config, key) == 7


@pytest.mark.parametrize(
    ("key", "value"),
    [
        (key, value)
        for key in _LIMITS
        for value in ((-1, True, "3") if key == "preview_rows" else (0, -1, True, "3"))
    ],
)
def test_limit_loader_rejects_nonpositive_or_noninteger_toml(
    tmp_path: Path, key: str, value: object
) -> None:
    rendered = "true" if value is True else f'"{value}"' if isinstance(value, str) else str(value)
    config_path = tmp_path / "run.toml"
    config_path.write_text(
        f'sas_root="sas"\npython_root="python"\n{key}={rendered}\n', encoding="utf-8"
    )

    with pytest.raises(ValueError):
        load_config(config_path, {})
