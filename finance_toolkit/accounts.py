import re
from datetime import datetime
from pathlib import Path
from typing import Pattern, List


class Account:
    def __init__(
        self, account_type: str, account_id: str, account_num: str, patterns: List[str]
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
        :param patterns: a list of regex patterns to match the filenames of a given account. We
            need a list because companies may change the naming of the file over time.
        """
        self.type: str = account_type
        self.id: str = account_id
        self.patterns: List[Pattern] = [re.compile(p) for p in patterns]
        self.num: str = account_num
        self.filename: str = f"{account_id}.csv"

    def __hash__(self):
        return hash(
            (
                self.type,
                self.id,
                self.num,
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
            and self.filename == o.filename
        )

    def __repr__(self) -> str:
        return (
            f"{type(self).__name__}<type={self.type!r}, "
            f"id={self.id!r}, num={self.altered_num!r}>"
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


class BnpAccount(Account):
    def __init__(self, account_type: str, account_id: str, account_num: str):
        super().__init__(
            account_type=account_type,
            account_id=account_id,
            account_num=account_num,
            patterns=["E\\d{,3}%s\\.csv" % account_num[-4:]],
        )


class BoursoramaAccount(Account):
    def __init__(self, account_type: str, account_id: str, account_num: str):
        super().__init__(
            account_type=account_type,
            account_id=account_id,
            account_num=account_num,
            patterns=[r"export-operations-(?P<date>\d{2}-\d{2}-\d{4})_.+\.csv"],
        )

    def get_operations_date(self, filename: str) -> datetime:
        for pattern in self.patterns:
            match = pattern.match(filename)
            if match:
                d = match.groupdict()["date"]
                # print(d)
                return datetime.strptime(d, "%d-%m-%Y")
        raise ValueError(f"failed to find date from the filename: {filename}")


class CartaAccount(Account):
    pass


class DegiroAccount(Account):
    def __init__(self, account_type: str, account_id: str, account_num: str):
        super().__init__(
            account_type=account_type,
            account_id=account_id,
            account_num=account_num,
            patterns=["Portfolio.csv"],
        )


class FortuneoAccount(Account):
    def __init__(self, account_type: str, account_id: str, account_num: str):
        super().__init__(
            account_type=account_type,
            account_id=account_id,
            account_num=account_num,
            patterns=[
                r"HistoriqueOperations_(\d+)_du_\d{2}_\d{2}_\d{4}_au_\d{2}_\d{2}_\d{4}\.csv"
            ],
        )


class OctoberAccount(Account):
    def __init__(self, account_type: str, account_id: str, account_num: str):
        super().__init__(
            account_type=account_type,
            account_id=account_id,
            account_num=account_num,
            patterns=[f"remboursements-{account_num}.xlsx"],
        )


class RevolutAccount(Account):
    def __init__(self, account_type: str, account_id: str, account_num: str):
        super().__init__(
            account_type=account_type,
            account_id=account_id,
            account_num=account_num,
            patterns=[
                r"Revolut-(.*)-Statement-(.*)\.csv",
                r"account-statement_(\d{4}-\d{2}-\d{2})_(\d{4}-\d{2}-\d{2})_undefined-undefined_%s\.csv"  # noqa
                % account_num,
            ],
        )
