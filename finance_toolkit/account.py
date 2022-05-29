import re
from pathlib import Path
from typing import Pattern, List


class Account:
    def __init__(
        self,
        account_type: str,
        account_id: str,
        account_num: str,
        currency: str,
        patterns: List[str],
    ):
        """
        Initialize a new account.

        :param account_type: the type of the account, usually in 3 characters in upper-case, such
            as CHQ (Compte de Chèque), LVA (Livret A), LDD (Livret de Développement Durable), GLD
            (Gold), OPT (Stock options).
        :param account_id: the account id used internally by the Finance Toolkit.
            TODO rename this variable
        :param account_num: the account id provided by your account. This is mainly used for
            detecting downloaded CSV files. You may not need to provide the full id, please
            check the requirements for each bank or each financial service.
            TODO rename this variable
        :param currency: the currency used by this account. The input should be a valid currency
            symbol in uppercase. See [World Currency Symbols](https://www.xe.com/en/symbols.php).
        :param patterns: a list of regex patterns to match the filenames of a given account. We
            need a list because companies may change the naming of the file over time.
        """
        self.type: str = account_type
        self.id: str = account_id
        self.patterns: List[Pattern] = [re.compile(p) for p in patterns]
        self.num: str = account_num
        self.currency_symbol: str = currency
        self.filename: str = f"{account_id}.csv"

    def __hash__(self):
        return hash(
            (
                self.type,
                self.id,
                self.num,
                self.currency_symbol,
                self.filename,
            )
        )

    def __eq__(self, o: object) -> bool:
        if not isinstance(o, type(self)):
            return False
        return (
            self.type == o.type
            and self.id == o.id
            and self.num == o.num
            and self.currency_symbol == o.currency_symbol
            and self.filename == o.filename
        )

    def __repr__(self) -> str:
        return (
            f"{type(self).__name__}<type={self.type!r}, "
            f"id={self.id!r}, num={self.altered_num!r}, "
            f"currency_symbol={self.currency_symbol!r}>"
        )

    @property
    def altered_num(self) -> str:
        """Return part of the account number, to protect the data when displayed."""
        return f"****{self.num[-4:]}"

    def is_account(self, account_full_num: str):
        return account_full_num.endswith(self.num)

    def match(self, path: Path) -> bool:
        # print(f"path.name: {path.name}")
        for p in self.patterns:
            matched = p.match(path.name)
            # print(f"{p}: {matched}")
            if matched:
                return True
        # print(f"result: {result}")
        return False


class CartaAccount(Account):
    pass


class DegiroAccount(Account):
    def __init__(
        self,
        account_type: str,
        account_id: str,
        account_num: str,
        currency: str = "EUR",
    ):
        super().__init__(
            account_type=account_type,
            account_id=account_id,
            account_num=account_num,
            currency=currency,
            patterns=["Portfolio.csv"],
        )


class OctoberAccount(Account):
    def __init__(
        self,
        account_type: str,
        account_id: str,
        account_num: str,
        currency: str = "EUR",
    ):
        super().__init__(
            account_type=account_type,
            account_id=account_id,
            account_num=account_num,
            currency=currency,
            patterns=[f"remboursements-{account_num}.xlsx"],
        )
