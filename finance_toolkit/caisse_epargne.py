from abc import ABCMeta
from datetime import datetime
from pathlib import Path
from typing import Tuple

import pandas as pd
from pandas import DataFrame

from .account import Account
from .models import Configuration
from .pipeline import Pipeline, TransactionPipeline, BalancePipeline, PipelineDataError


class CaisseEpargneAccount(Account):
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
            patterns=[
                # e.g. "123_01112025_07122025.csv"
                # format: {account}_{startDate}_{endDate}.csv
                # where "123" stands for the account ID and dates are DDMMYYYY
                # (start and end of operations period)
                "%s_\\d{8}_\\d{8}\\.csv" % account_num
            ],
        )


class CaisseEpargnePipeline(Pipeline, metaclass=ABCMeta):
    def __init__(self, account: CaisseEpargneAccount, cfg: Configuration):
        super().__init__(account, cfg)
        self.account: CaisseEpargneAccount = account

    def read_raw(self, csv: Path) -> Tuple[DataFrame, DataFrame]:
        try:
            tx_df = pd.read_csv(
                csv,
                date_format="%d/%m/%Y",
                decimal=",",
                delimiter=";",
                encoding="UTF-8",
                parse_dates=[
                    "Date de comptabilisation",
                    "Date operation",
                    "Date de valeur",
                ],
                skipinitialspace=True,
            )
        except Exception as e:
            raise PipelineDataError(f"Failed to read CSV file {csv}: {e}")

        tx_df.rename(
            columns={
                "Date operation": "Date",
            },
            inplace=True,
        )

        # Process the DataFrame as needed
        return pd.DataFrame(), tx_df  # Placeholder for balance DataFrame


class CaisseEpargneTransactionPipeline(CaisseEpargnePipeline, TransactionPipeline):
    def read_new_transactions(self, csv: Path) -> DataFrame:
        _, tx = self.read_raw(csv)
        return tx


class CaisseEpargneBalancePipeline(CaisseEpargnePipeline, BalancePipeline):
    def read_new_balances(self, csv: Path) -> DataFrame:
        balances, _ = self.read_raw(csv)
        return balances
