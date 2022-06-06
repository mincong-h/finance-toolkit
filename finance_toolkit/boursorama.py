from abc import ABCMeta
from datetime import datetime
from pathlib import Path
from typing import Tuple

import pandas as pd
from pandas import DataFrame

from .account import Account
from .models import TxType, Configuration
from .pipeline import Pipeline, TransactionPipeline, BalancePipeline


class BoursoramaAccount(Account):
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
            patterns=[r"export-operations-(?P<date>\d{2}-\d{2}-\d{4})_.+\.csv"],
        )

    def get_operations_date(self, filename: str) -> datetime:
        for pattern in self.patterns:
            match = pattern.match(filename)
            if match:
                d = match.groupdict()["date"]
                return datetime.strptime(d, "%d-%m-%Y")
        raise ValueError(f"failed to find date from the filename: {filename}")


class BoursoramaPipeline(Pipeline, metaclass=ABCMeta):
    def __init__(self, account: BoursoramaAccount, cfg: Configuration):
        super().__init__(account, cfg)
        self.account: BoursoramaAccount = account

    def read_raw(self, csv: Path) -> Tuple[DataFrame, DataFrame]:
        df = pd.read_csv(
            csv,
            decimal=",",
            delimiter=";",
            dtype={"accountNum": "str"},
            encoding="ISO-8859-1",
            parse_dates=["dateOp", "dateVal"],
            skipinitialspace=True,
            thousands=" ",
        )
        df = df.rename(columns={"accountbalance": "accountBalance"})

        # Boursorama > Transaction
        transactions = df[df["accountNum"].map(self.account.is_account)]
        transactions = transactions.reset_index(drop=True)
        transactions = transactions.rename(
            columns={"dateOp": "Date", "label": "Label", "amount": "Amount"}
        )
        # Boursorama does not provide currency information explicitly, so we create it ourselves.
        transactions = transactions.assign(
            Currency=lambda row: self.account.currency_symbol
        )
        del transactions["dateVal"]
        del transactions["category"]
        del transactions["categoryParent"]

        # Boursorama > Balance
        balances = df.groupby("accountNum")["accountBalance"].max().to_frame()
        balances.reset_index(inplace=True)
        balances["Date"] = self.account.get_operations_date(csv.name) - pd.Timedelta(
            "1 day"
        )
        # Boursorama does not provide currency information explicitly, so we create it ourselves.
        balances = balances.assign(Currency=lambda row: self.account.currency_symbol)
        balances = balances[balances["accountNum"].map(self.account.is_account)]
        balances = balances.reset_index(drop=True)
        balances = balances.rename(columns={"accountBalance": "Amount"})
        balances = balances[["accountNum", "Date", "Amount", "Currency"]]
        return balances, transactions


class BoursoramaTransactionPipeline(BoursoramaPipeline, TransactionPipeline):
    def guess_meta(self, df: DataFrame) -> DataFrame:
        if self.account.type == "LVR":
            df["Type"] = TxType.TRANSFER.value
        elif self.account.type == "CHQ":
            df["Type"] = TxType.EXPENSE.value

        for i, row in df.iterrows():
            for c in self.cfg.autocomplete:
                if c.match(row.Label):
                    df.loc[i, "Type"] = c.tx_type
                    df.loc[i, "MainCategory"] = c.main_category
                    df.loc[i, "SubCategory"] = c.sub_category
                    break
        return df

    def read_new_transactions(self, path: Path):
        _, tx = self.read_raw(path)
        return tx


class BoursoramaBalancePipeline(BoursoramaPipeline, BalancePipeline):
    def read_new_balances(self, csv: Path) -> DataFrame:
        balances, _ = self.read_raw(csv)
        return balances
