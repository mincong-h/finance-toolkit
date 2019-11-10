from pathlib import Path


def get_test_file(path: str) -> Path:
    return Path(__file__).parent / path
