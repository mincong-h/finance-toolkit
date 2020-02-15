from pathlib import Path

from finance_toolkit.utils import Summary


# ---------- Class: Summary ----------


def test_summary_without_source_files():
    summary = Summary(Path("/path/to/sources"))
    assert (
        str(summary)
        == """\
$$$ Summary $$$
---------------
No CSV found in "/path/to/sources".
---------------
Finished."""
    )


def test_summary_with_source_files():
    s = Summary(Path("/path/to/sources"))
    s.add_source(Path("/path/to/sources/def"))
    s.add_source(Path("/path/to/sources/abc"))
    s.add_target(Path("/path/to/targets/456"))
    s.add_target(Path("/path/to/targets/123"))
    assert (
        str(s)
        == """\
$$$ Summary $$$
---------------
2 files copied.
---------------
Sources:
- /path/to/sources/abc
- /path/to/sources/def
Targets:
- /path/to/targets/123
- /path/to/targets/456
Finished."""
    )


# ---------- Class: Configuration ----------


def test_configuration_categories(cfg):
    cfg.category_set.update(
        [
            "food/supermarket",
            "food/restaurant",
            "food/restaurant",  # duplicate
            "gouv/tax",
            "food/work",
        ]
    )
    # results are unique
    assert cfg.categories() == [
        "food/restaurant",
        "food/supermarket",
        "food/work",
        "gouv/tax",
    ]
    # results are filtered
    assert cfg.categories(lambda c: c.startswith("food")) == [
        "food/restaurant",
        "food/supermarket",
        "food/work",
    ]
