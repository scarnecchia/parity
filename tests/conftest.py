from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / ".parity-fixtures"


@pytest.fixture(scope="session")
def fixtures() -> Path:
    required = (
        "productsales.sas7bdat",
        "productsales.csv",
        "datetime.sas7bdat",
        "datetime.csv",
        "dates_null.sas7bdat",
        "missing_test.sas7bdat",
    )
    missing = [name for name in required if not (FIXTURES / name).is_file()]
    if missing:
        pytest.fail(
            "external fixtures unavailable; run `python -m tests.acquire_fixtures` explicitly: "
            + ", ".join(missing)
        )
    return FIXTURES
