from abc import ABCMeta, abstractmethod
from pathlib import Path
from typing import Tuple

import pandas as pd
from pandas import DataFrame
from datetime import datetime

from .accounts import (
    Account,
    BnpAccount,
    BoursoramaAccount,
    FortuneoAccount,
)
from .models import Configuration, Summary, TxType


class Pipeline(metaclass=ABCMeta):
    def __init__(self, account: Account, cfg: Configuration):
        self.account = account
        self.cfg = cfg

    @abstractmethod
    def run(self, path: Path, summary: Summary) -> None:
        """
        Run pipeline

        :param path: the source path where data should be read
        :param summary: the summary containing results of different pipelines
        """


class TransactionPipeline(Pipeline, metaclass=ABCMeta):
    def run(self, source: Path, summary: Summary) -> None:
        # read
        tx = self.read_new_transactions(source)
        summary.add_source(source)

        # process
        tx = self.guess_meta(tx)
        tx["Month"] = tx.Date.apply(lambda date: date.strftime("%Y-%m"))

        # write
        for m in tx["Month"].unique():
            d = self.cfg.root_dir / m
            d.mkdir(exist_ok=True)
            target = d / f"{m}.{self.account.filename}"
            self.append_transactions(target, tx[tx["Month"] == m])
            summary.add_target(target)

    @classmethod
    def append_transactions(cls, csv: Path, new_transactions: DataFrame):
        df = new_transactions.copy()
        cols = [
            "Date",
            "Label",
            "Amount",
            "Type",
            "MainCategory",
            "SubCategory",
        ]

        if csv.exists():
            existing = pd.read_csv(csv, parse_dates=["Date"])
            df = df.append(existing, sort=False)

        df = df.drop_duplicates(subset=["Date", "Label", "Amount"], keep="last")
        df = df.sort_values(by=["Date", "Label"])
        df.to_csv(csv, columns=cols, index=None, date_format="%Y-%m-%d")

    def guess_meta(self, df: DataFrame) -> DataFrame:
        """
        Guess metadata for transactions.

        :param df: the DataFrame for transactions
        :return: a DataFrame containing additional metadata
        """
        return df

    @abstractmethod
    def read_new_transactions(self, csv: Path) -> DataFrame:
        """
        Read new transactions from a CSV file, probably downloaded from internet.

        :param csv: the path of the CSV file
        """
        pass


class NoopTransactionPipeline(TransactionPipeline):
    def run(self, source: Path, summary: Summary) -> None:
        pass

    def read_new_transactions(self, csv: Path) -> DataFrame:
        pass


class BalancePipeline(Pipeline, metaclass=ABCMeta):
    def run(self, path: Path, summary: Summary):
        balances = self.read_new_balances(path)
        balance_file = self.cfg.root_dir / f"balance.{self.account.filename}"
        self.write_balances(balance_file, balances)

        summary.add_source(path)
        summary.add_target(balance_file)

    def read_balance(self, path: Path) -> DataFrame:
        df = pd.read_csv(path, parse_dates=["Date"])
        df = df[["Date", "Amount"]]
        df["Account"] = self.account.id
        df["AccountId"] = self.account.num
        df["AccountType"] = self.account.type
        return df

    @classmethod
    def write_balances(cls, csv: Path, new_lines: DataFrame):
        df = new_lines.copy()
        if csv.exists():
            existing = pd.read_csv(csv, parse_dates=["Date"])
            df = df.append(existing, sort=False)
        df = df.drop_duplicates(subset=["Date"], keep="last")
        df = df.sort_values(by="Date")
        df.to_csv(csv, index=None, columns=["Date", "Amount"])

    @abstractmethod
    def read_new_balances(self, csv: Path) -> DataFrame:
        pass


class GeneralBalancePipeline(BalancePipeline):
    def run(self, path: Path, summary: Summary):
        pass

    def read_new_balances(self, csv: Path) -> DataFrame:
        pass


class BoursoramaPipeline(Pipeline, metaclass=ABCMeta):
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

        m = self.account.pattern.match(csv.name)
        balances = df.groupby("accountNum")["accountBalance"].max().to_frame()
        balances.reset_index(inplace=True)
        balances["Date"] = datetime.strptime(m.group(1), "%d-%m-%Y") - pd.Timedelta(
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


class FortuneoTransactionPipeline(TransactionPipeline):
    def guess_meta(self, df: DataFrame) -> DataFrame:
        for i, row in df.iterrows():
            for c in self.cfg.autocomplete:
                if c.match(row.Label):
                    df.loc[i, "Type"] = c.tx_type
                    df.loc[i, "MainCategory"] = c.main_category
                    df.loc[i, "SubCategory"] = c.sub_category
                    break
        return df

    def read_new_transactions(self, csv: Path) -> DataFrame:
        # encoding: we don't know the exact encoding used by Fortuneo,
        # considering it as UTF-8 until we find a better solution
        tx = pd.read_csv(
            csv,
            decimal=",",
            delimiter=";",
            encoding="UTF-8",
            skipinitialspace=True,
            thousands=" ",
        )
        tx.columns = [
            "Date opération",
            "Date valeur",
            "libellé",
            "Débit",
            "Crédit",
            "empty",
        ]

        # Parse dates manually due to encoding problem
        tx["Date opération"].apply(lambda s: pd.to_datetime(s, format="%d/%m/%Y"))
        tx["Date valeur"].apply(lambda s: pd.to_datetime(s, format="%d/%m/%Y"))
        tx = tx.astype({"Date opération": "datetime64", "Date valeur": "datetime64"})

        tx = tx.fillna("")
        tx["Amount"] = tx.apply(
            lambda row: row["Débit"] if row["Débit"] else row["Crédit"], axis="columns"
        )
        tx["Type"] = ""
        tx["MainCategory"] = ""
        tx["SubCategory"] = ""

        del tx["Date valeur"]
        del tx["empty"]

        tx = tx.rename(columns={"Date opération": "Date", "libellé": "Label"})

        # reorder columns
        tx = tx[
            [
                "Date",
                "Label",
                "Amount",
                "Type",
                "MainCategory",
                "SubCategory",
            ]
        ]
        return tx


class AccountParser:
    def __init__(self, cfg: Configuration):
        self.accounts = cfg.as_dict()

    def parse(self, path: Path) -> Account:
        parts = path.name.split(".")
        account_id = parts[1]
        if len(parts) == 3 and account_id in self.accounts:
            return self.accounts[account_id]
        return Account("unknown", "unknown", "unknown", r"unknown")


