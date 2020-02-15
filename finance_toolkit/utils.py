from pathlib import Path
from typing import List, Set, Tuple, Dict

from .accounts import Account


class Configuration:
    """
    Type-safe representation of the user configuration.
    """

    def __init__(
        self,
        accounts: List[Account],
        categories: List[str],
        autocomplete: List[Tuple],
        download_dir: Path,
        root_dir: Path,
    ):
        self.accounts: List[Account] = accounts
        self.category_set: Set[str] = set(categories)
        self.autocomplete: List[Tuple] = autocomplete
        self.download_dir: Path = download_dir
        self.root_dir: Path = root_dir

    def as_dict(self) -> Dict[str, Account]:
        return {a.id: a for a in self.accounts}

    def categories(self, cat_filter=lambda x: True) -> List[str]:
        """
        Gets configured categories.

        :param cat_filter: optional category filter, default to no-op filter (do nothing)
        :return: categories without duplicate, order is guaranteed
        """
        return sorted(c for c in filter(cat_filter, self.category_set))


class Summary:
    def __init__(self, source_dir: Path):
        self.source_dir = source_dir
        self.sources = set()
        self.targets = set()

    def add_target(self, target: Path) -> None:
        self.targets.add(target)

    def add_source(self, source: Path) -> None:
        self.sources.add(source)

    def __repr__(self) -> str:
        if self.sources:
            s = "\n".join([f"- {s}" for s in sorted(self.sources)])
            t = "\n".join([f"- {t}" for t in sorted(self.targets)])
            return f"""\
$$$ Summary $$$
---------------
{len(self.sources)} files copied.
---------------
Sources:
{s}
Targets:
{t}
Finished."""
        else:
            return f"""\
$$$ Summary $$$
---------------
No CSV found in "{self.source_dir}".
---------------
Finished."""
