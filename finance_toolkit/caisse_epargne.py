from abc import ABCMeta
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
            # e.g. "08062025_1234.csv" for a file downloaded on June 8, 2025, for account 1234
            patterns=[r"(?P<date>\d{8})_(?P<account>\d+)\.csv"],
        )


class CaisseEpargnePipeline(Pipeline, metaclass=ABCMeta):
    def __init__(self, account: CaisseEpargneAccount, cfg: Configuration):
        super().__init__(account, cfg)
        self.account: CaisseEpargneAccount = account

    def read_raw(self, csv: Path) -> Tuple[DataFrame, DataFrame]:
        kwargs = {
            "decimal": ",",
            "delimiter": ";",
            "encoding": "UTF-8",
            "parse_dates": [
                "Date de comptabilisation",
                "Date operation",
                "Date de valeur",
            ],
            "skipinitialspace": True,
        }
        try:
            df = pd.read_csv(csv, **kwargs)
        except Exception as e:
            raise PipelineDataError(f"Failed to read CSV file {csv}: {e}")

        # Process the DataFrame as needed
        return pd.DataFrame(), df  # Placeholder for balance DataFrame


class CaisseEpargneTransactionPipeline(TransactionPipeline):
    def read_new_transactions(self, csv: Path) -> DataFrame:
        _, tx = self.read_raw(csv)
        return tx


class CaisseEpargneBalancePipeline(BalancePipeline):
    def read_new_balances(self, csv: Path) -> DataFrame:
        balances, _ = self.read_raw(csv)
        return balances
