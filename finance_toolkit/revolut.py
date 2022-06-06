from abc import ABCMeta
from pathlib import Path
from typing import Tuple

import pandas as pd
from pandas import DataFrame

from .account import Account
from .models import TxType
from .pipeline import Pipeline, TransactionPipeline, BalancePipeline


class RevolutAccount(Account):
    def __init__(
        self, account_type: str, account_id: str, account_num: str, currency: str
    ):
        super().__init__(
            account_type=account_type,
            account_id=account_id,
            account_num=account_num,
            currency=currency,
            patterns=[
                r"Revolut-(.*)-Statement-(.*)\.csv",
                r"account-statement_(\d{4}-\d{2}-\d{2})_(\d{4}-\d{2}-\d{2})_undefined-undefined_%s\.csv"  # noqa
                % account_num,
            ],
        )


class RevolutPipeline(Pipeline, metaclass=ABCMeta):
    @classmethod
    def read_raw(cls, csv: Path) -> Tuple[DataFrame, DataFrame]:
        df = pd.read_csv(
            csv,
            delimiter=",",
            parse_dates=["Started Date", "Completed Date"],
        )

        balances = df[["Completed Date", "Balance", "Currency"]]
        balances = balances.rename(
            columns={
                "Completed Date": "Date",
                "Balance": "Amount",
            }
        )
        balances = balances[balances["Amount"].notna()]

        # TODO support fields: Type, Product, Fee, State

        tx = df[["Completed Date", "Description", "Amount", "Currency", "Type"]]
        tx = tx.rename(
            columns={
                "Completed Date": "Date",
                "Description": "Label",
            }
        )

        # TODO can we remove these fields?
        tx["MainCategory"] = ""
        tx["SubCategory"] = ""

        return balances, tx


class RevolutTransactionPipeline(RevolutPipeline, TransactionPipeline):
    TYPE_MAPPING = {
        # A top-up transaction makes up to the full amount of your account, so we consider it's
        # likely an income here. This is an opinionated choice.
        "TOPUP": TxType.INCOME.value,
        "TRANSFER": TxType.TRANSFER.value,
        "FEE": TxType.EXPENSE.value,
        "CARD_PAYMENT": TxType.EXPENSE.value,
        "EXCHANGE": TxType.EXPENSE.value,
    }

    def guess_meta(self, df: DataFrame) -> DataFrame:
        for i, row in df.iterrows():
            t = row.Type
            if t in self.TYPE_MAPPING:
                df.loc[i, "Type"] = self.TYPE_MAPPING[t]
            for c in self.cfg.autocomplete:
                if c.match(row.Label):
                    df.loc[i, "Type"] = c.tx_type
                    df.loc[i, "MainCategory"] = c.main_category
                    df.loc[i, "SubCategory"] = c.sub_category
                    break
        return df

    def read_new_transactions(self, path: Path) -> DataFrame:
        _, tx = self.read_raw(path)
        return tx


class RevolutBalancePipeline(RevolutPipeline, BalancePipeline):
    def read_new_balances(self, csv: Path) -> DataFrame:
        balances, _ = self.read_raw(csv)
        return balances
