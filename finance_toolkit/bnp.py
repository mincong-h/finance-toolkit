from abc import ABCMeta
from datetime import datetime
from html import unescape
from pathlib import Path
from typing import Tuple

import pandas as pd
from pandas import DataFrame

from .account import Account
from .models import TxType
from .pipeline import Pipeline, TransactionPipeline, BalancePipeline


class BnpAccount(Account):
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
            patterns=["E\\d{,3}%s\\.csv" % account_num[-4:]],
        )


class BnpPipeline(Pipeline, metaclass=ABCMeta):
    @classmethod
    def parse_fr_float(cls, s: str) -> float:
        v = s.replace(",", ".").replace(" ", "")
        return float(v)

    def read_raw(self, csv: Path) -> Tuple[DataFrame, DataFrame]:
        # BNP Paribas stores the balance in the first line of the CSV
        with csv.open(encoding="ISO-8859-1") as f:
            first = next(f).strip()
            # We need to unescape twice because BNP double-escaped the line
            # Origin: '"Cr&eacute;dit immobilier";"Cr&amp;eacute;dit immobilier";****0170;18/03/2022;;-113 095,26'  # noqa: E501
            #    1st: '"Crédit immobilier";"Cr&eacute;dit immobilier";****0170;18/03/2022;;-113 095,26'             # noqa: E501
            #    2nd: '"Crédit immobilier";"Crédit immobilier";****0170;18/03/2022;;-113 095,26'                    # noqa: E501
            first = unescape(unescape(first))

        # BNP > Balance
        balances = pd.DataFrame.from_records(
            data=[first.split(";")],
            columns=[
                "mainCategory",
                "subCategory",
                "accountNum",
                "Date",
                "unknown",
                "Amount",
            ],
        )
        balances["Date"] = pd.to_datetime(balances["Date"], format="%d/%m/%Y")
        balances["Amount"] = balances["Amount"].apply(self.parse_fr_float)
        # BNP Paribas does not provide currency information explicitly, so we create it ourselves.
        balances = balances.assign(
            Currency=lambda row: self.account.currency_symbol
        )
        del balances["mainCategory"]
        del balances["subCategory"]
        del balances["accountNum"]
        del balances["unknown"]

        # BNP > Transaction
        tx = pd.read_csv(
            csv,
            date_parser=lambda s: datetime.strptime(s, "%d/%m/%Y"),
            decimal=",",
            delimiter=";",
            encoding="ISO-8859-1",
            names=["Date", "bnpMainCategory", "bnpSubCategory", "Label", "Amount"],
            parse_dates=["Date"],
            skipinitialspace=True,
            skiprows=1,
            thousands=" ",
        )

        del tx["bnpMainCategory"]
        del tx["bnpSubCategory"]
        tx = tx.fillna("")

        # BNP Paribas does not provide currency information explicitly, so we create it ourselves.
        tx = tx.assign(Currency=lambda row: self.account.currency_symbol)

        # TODO can we remove these fields?
        tx["Type"] = ""
        tx["MainCategory"] = ""
        tx["SubCategory"] = ""

        return balances, tx


class BnpTransactionPipeline(BnpPipeline, TransactionPipeline):
    def guess_meta(self, df: DataFrame) -> DataFrame:
        if self.account.type == "CDI":
            df["Type"] = TxType.CREDIT.value
        if self.account.type in ["LVA", "LDD"]:
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

    def read_new_transactions(self, path: Path) -> DataFrame:
        _, tx = self.read_raw(path)
        return tx


class BnpBalancePipeline(BnpPipeline, BalancePipeline):
    def read_new_balances(self, csv: Path) -> DataFrame:
        balances, _ = self.read_raw(csv)
        return balances
