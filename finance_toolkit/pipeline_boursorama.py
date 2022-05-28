from abc import ABCMeta
from datetime import datetime
from pathlib import Path
from typing import Tuple

import pandas as pd
from pandas import DataFrame

from .accounts import BoursoramaAccount
from .models import TxType, Configuration
from .pipelines import Pipeline, TransactionPipeline, BalancePipeline


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
        transactions = df[df["accountNum"].map(self.account.is_account)]
        transactions = transactions.reset_index(drop=True)
        transactions = transactions.rename(
            columns={"dateOp": "Date", "label": "Label", "amount": "Amount"}
        )
        del transactions["dateVal"]
        del transactions["category"]
        del transactions["categoryParent"]

        balances = df.groupby("accountNum")["accountBalance"].max().to_frame()
        balances.reset_index(inplace=True)
        balances["Date"] = self.account.get_operations_date(csv.name) - pd.Timedelta(
            "1 day"
        )
        balances = balances[balances["accountNum"].map(self.account.is_account)]
        balances = balances.reset_index(drop=True)
        balances = balances.rename(columns={"accountBalance": "Amount"})
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
