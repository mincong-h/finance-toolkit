import re
from pathlib import Path
from typing import Pattern


class Account:
    def __init__(
        self, account_type: str, account_id: str, account_num: str, pattern: str
    ):
        self.type: str = account_type
        self.id: str = account_id
        self.pattern: Pattern = re.compile(pattern)
        self.num: str = account_num
        self.filename: str = f"{account_id}.csv"

    def __hash__(self):
        return hash((self.type, self.id, self.pattern, self.num))

    def __eq__(self, o: object) -> bool:
        if not isinstance(o, type(self)):
            return False
        return (
            self.type == o.type
            and self.id == o.id
            and self.pattern == o.pattern
            and self.num == o.num
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
        return bool(self.pattern.match(path.name))


class BnpAccount(Account):
    def __init__(self, account_type: str, account_id: str, account_num: str):
        super().__init__(
            account_type=account_type,
            account_id=account_id,
            account_num=account_num,
            pattern="E\\d{,3}%s\\.csv" % account_num[-4:],
        )


class BoursoramaAccount(Account):
    def __init__(self, account_type: str, account_id: str, account_num: str):
        super().__init__(
            account_type=account_type,
            account_id=account_id,
            account_num=account_num,
            pattern=r"export-operations-(\d{2}-\d{2}-\d{4})_.+\.csv",
        )


class CartaAccount(Account):
    pass


class DegiroAccount(Account):
    def __init__(self, account_type: str, account_id: str, account_num: str):
        super().__init__(
            account_type=account_type,
            account_id=account_id,
            account_num=account_num,
            pattern="Portfolio.csv",
        )


class FortuneoAccount(Account):
    def __init__(self, account_type: str, account_id: str, account_num: str):
        super().__init__(
            account_type=account_type,
            account_id=account_id,
            account_num=account_num,
            pattern=r"HistoriqueOperations_(\d+)_du_\d{2}_\d{2}_\d{4}_au_\d{2}_\d{2}_\d{4}\.csv",
        )


class OctoberAccount(Account):
    def __init__(self, account_type: str, account_id: str, account_num: str):
        super().__init__(
            account_type=account_type,
            account_id=account_id,
            account_num=account_num,
            pattern=f"remboursements-{account_num}.xlsx",
        )


class RevolutAccount(Account):
    def __init__(self, account_type: str, account_id: str, account_num: str):
        super().__init__(
            account_type=account_type,
            account_id=account_id,
            account_num=account_num,
            pattern=r"account-statement_(\d{4}-\d{2}-\d{2})_(\d{4}-\d{2}-\d{2})_undefined_undefined_([0-9a-f]+)\.csv",
        )
