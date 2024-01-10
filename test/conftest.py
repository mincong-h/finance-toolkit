from pathlib import Path
from shutil import copyfile
from tempfile import TemporaryDirectory

import pytest

from finance_toolkit.tx import Configuration
from finance_toolkit.models import ExchangeRateConfig


@pytest.fixture(scope="session")
def location():
    """Return the parent folder containing test files.
    It will last for the entire test session as there is
    no need to recompute the path inbetween tests.
    """
    return Path(__file__).parent


@pytest.fixture()
def sample(location):
    """Return a temporary configuration folder using the sample configuration file."""
    file = location.parent / "finance-tools.sample.yml"
    with TemporaryDirectory() as tmp:
        dst = Path(tmp) / "finance-tools.yml"
        copyfile(file, dst)
        yield Path(tmp)


@pytest.fixture()
def cfg(tmpdir, location):
    """Instantiate a Configuration class using temporary folder and test folder.
    A new object is created for each and every test function.
    """
    source_dir = location / "download"
    target_dir = Path(tmpdir) / "finance"
    target_dir.mkdir()

    return Configuration(
        accounts=[],
        categories=[],
        categories_to_rename={},
        autocomplete=[],
        download_dir=source_dir,
        root_dir=target_dir,
        exchange_rate_cfg=ExchangeRateConfig(watched_currencies=["USD", "CNY"])
    )


@pytest.fixture(autouse=True)
def no_warnings(recwarn):
    """Fail on warning."""

    yield

    warnings = []
    for warning in recwarn:  # pragma: no cover
        warn = f"{warning.filename}:{warning.lineno} {warning.message}"
        warnings.append(warn)
        print(warn)

    assert not warnings
