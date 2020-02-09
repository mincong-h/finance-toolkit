from pathlib import Path
from shutil import copyfile
from tempfile import TemporaryDirectory

import pytest
from tx.tx import Configuration


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
def cfg(tmpdir):
    """Instantiate a Configuration class using temporary folders.
    A new object is created for each and every test function.
    """
    source_dir = Path(tmpdir) / "downloads"
    target_dir = Path(tmpdir) / "finance"

    source_dir.mkdir()
    target_dir.mkdir()

    return Configuration(
        accounts=[],
        categories=[],
        autocomplete=[],
        download_dir=source_dir,
        root_dir=target_dir,
    )
