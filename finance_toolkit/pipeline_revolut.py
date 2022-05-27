from abc import ABCMeta
from pathlib import Path
from typing import Tuple

import pandas as pd
from pandas import DataFrame

from .pipelines import Pipeline, TransactionPipeline, BalancePipeline


class RevolutPipeline(Pipeline, metaclass=ABCMeta):
    def read_raw(self, csv: Path) -> Tuple[DataFrame, DataFrame]:
        df = pd.read_csv(
            csv,
            delimiter=",",
            parse_dates=["Started Date", "Completed Date"],
        )
        balances = df.copy()
        tx = df.copy()
        return balances, tx


class RevolutTransactionPipeline(RevolutPipeline, TransactionPipeline):
    def guess_meta(self, df: DataFrame) -> DataFrame:
        # TODO
        return df

    def read_new_transactions(self, path: Path) -> DataFrame:
        _, tx = self.read_raw(path)
        return tx


class RevolutBalancePipeline(RevolutPipeline, BalancePipeline):
    def read_new_balances(self, csv: Path) -> DataFrame:
        balances, _ = self.read_raw(csv)
        return balances
