from abc import ABCMeta
from pathlib import Path
from typing import Tuple

import pandas as pd
from pandas import DataFrame

from .pipelines import Pipeline, TransactionPipeline, BalancePipeline


class RevolutPipeline(Pipeline, metaclass=ABCMeta):
    @classmethod
    def read_raw(cls, csv: Path) -> Tuple[DataFrame, DataFrame]:
        df = pd.read_csv(
            csv,
            delimiter=",",
            parse_dates=["Started Date", "Completed Date"],
        )

        balances = df[["Completed Date", "Balance"]]
        balances = balances.rename(
            columns={
                "Completed Date": "Date",
                "Balance": "Amount",
            }
        )
        balances = balances[balances["Amount"].notna()]

        # TODO support fields: Type, Product, Fee, Currency, State

        tx = df[["Completed Date", "Description", "Amount", "Type"]]
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
        "TOPUP": "expense",
        "TRANSFER": "transfer",
        "FEE": "expense",
        "CARD_PAYMENT": "expense",
        "EXCHANGE": "expense",
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
