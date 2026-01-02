import re
from abc import ABCMeta
from datetime import datetime
from pathlib import Path
from typing import Tuple

import pandas as pd
from pandas import DataFrame

from .account import Account
from .models import Configuration, TxType
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
                # e.g. "123456789_01112025_07122025.csv"
                # format: {account}_{startDate}_{endDate}.csv
                # where dates are DDMMYYYY (start and end of operations period)
                # account_num is the suffix of the full account number, e.g. "6789"
                # matches any digits before the suffix
                "\\d*%s_\\d{8}_\\d{8}\\.csv"
                % re.escape(account_num)
            ],
        )


class CaisseEpargnePipeline(Pipeline, metaclass=ABCMeta):
    def __init__(self, account: CaisseEpargneAccount, cfg: Configuration):
        super().__init__(account, cfg)
        self.account: CaisseEpargneAccount = account

    def read_raw(self, csv: Path) -> Tuple[DataFrame, DataFrame]:
        kwargs = {
            "date_parser": lambda s: datetime.strptime(s, "%d/%m/%Y"),
            "decimal": ",",
            "delimiter": ";",
            "encoding": "ISO-8859-1",
            "parse_dates": [
                "Date de comptabilisation",
                "Date operation",
                "Date de valeur",
            ],
            "skipinitialspace": True,
        }
        try:
            tx_df = pd.read_csv(csv, **kwargs)
        except ValueError as e:
            with csv.open(encoding=kwargs["encoding"]) as f:
                headers = next(f).strip()
            raise PipelineDataError(
                msg="Failed to read new Caisse d'Epargne data.",
                path=csv,
                headers=headers,
                pandas_kwargs=kwargs,
                pandas_error=e,
            )

        tx_df.rename(
            columns={
                "Date operation": "Date",
                "Libelle operation": "Label",
                "Debit": "Amount",
            },
            inplace=True,
        )

        # Caisse d'Epargne only supports EUR
        tx_df["Currency"] = "EUR"

        return pd.DataFrame(), tx_df


class CaisseEpargneTransactionPipeline(CaisseEpargnePipeline, TransactionPipeline):
    def guess_meta(self, df: DataFrame) -> DataFrame:
        if self.account.type == "CHQ":
            df["Type"] = TxType.EXPENSE.value

        for i, row in df.iterrows():
            for c in self.cfg.autocomplete:
                if c.match(row.Label):
                    df.loc[i, "Type"] = c.tx_type
                    df.loc[i, "MainCategory"] = c.main_category
                    df.loc[i, "SubCategory"] = c.sub_category
                    break
        return df

    def read_new_transactions(self, csv: Path) -> DataFrame:
        _, tx = self.read_raw(csv)
        return tx


class CaisseEpargneBalancePipeline(CaisseEpargnePipeline, BalancePipeline):
    def read_new_balances(self, csv: Path) -> DataFrame:
        balances, _ = self.read_raw(csv)
        return balances
