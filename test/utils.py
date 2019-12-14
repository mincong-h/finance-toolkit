from pathlib import Path

from src.tx import Configuration


def get_test_file(path: str) -> Path:
    return Path(__file__).parent / path


class TestConfig(Configuration):
    def __init__(self, tempdir: str):
        source_dir = Path(tempdir) / "downloads"
        target_dir = Path(tempdir) / "finance"

        source_dir.mkdir()
        target_dir.mkdir()

        super().__init__(
            accounts=[],
            categories=[],
            autocomplete=[],
            download_dir=source_dir,
            root_dir=target_dir,
        )
