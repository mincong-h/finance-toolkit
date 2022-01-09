import re
from enum import Enum
from pathlib import Path
from typing import List, Set, Dict, Pattern

from dataclasses import dataclass

from .accounts import Account


class TxType(str, Enum):
    # Income is a compensation obtained via different activities, e.g.
    # work (salary, wage), investment. Usually, an income increases the asset
    # of the portfolio.
    INCOME = "income"

    # Expense is a transaction for purchases. Usually, an expense decreases
    # the asset of the portfolio.
    EXPENSE = "expense"

    # Transfer is a transaction for transferring money from one account to
    # another internally in the portfolio. In a family (multi-user) portfolio,
    # transferring money from one user's account to another is considered as
    # "transfer" because the family asset remains the same.
    TRANSFER = "transfer"

    # Tax is a transaction for paying tax. Usually, such transaction decreases
    # the asset of the portfolio.
    #
    # Tax is not considered as an "expense" because paying tax is an obligation
    # and not an option. Also because some income taxes are invisible in our
    # system since they had been deducted before the salary arrived
    # (prélèvement à la source). Tax is not considered as a (negative) "income"
    # because it includes income-unrelated items, such as property tax and
    # residence tax. So the best choice is to use a dedicated type right now.
    TAX = "tax"

    @staticmethod
    def values() -> Set[str]:
        return {m.value for m in TxType}


@dataclass
class TxCompletion:
    regex: Pattern
    tx_type: str
    main_category: str
    sub_category: str

    def match(self, label: str):
        return self.regex.match(label)

    @staticmethod
    def load(pattern: Dict) -> "TxCompletion":
        """
        Load pattern from configuration. A pattern is a dictionary, declared in YAML as follows:

        .. code-block:: yaml

            expr: '.*FLUNCH.*'
            type: expense
            cat: food/restaurant
            desc: Optional description about this matching pattern. We go to Flunch regularly.

        :param pattern: dictionary for the auto-completion
        :return: a new completion
        """
        return TxCompletion(
            regex=re.compile(pattern["expr"]),
            tx_type=pattern["type"],
            main_category=pattern["cat"].split("/")[0],
            sub_category=pattern["cat"].split("/")[1],
        )


class Configuration:
    """
    Type-safe representation of the user configuration.
    """

    def __init__(
        self,
        accounts: List[Account],
        categories: List[str],
        categories_to_rename: Dict[str, str],
        autocomplete: List[TxCompletion],
        download_dir: Path,
        root_dir: Path,
    ):
        self.accounts: List[Account] = accounts
        self.category_set: Set[str] = set(categories)
        self.categories_to_rename = categories_to_rename
        self.autocomplete: List[TxCompletion] = autocomplete
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
    def __init__(self, cfg: Configuration):
        self.source_dir = cfg.download_dir
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
