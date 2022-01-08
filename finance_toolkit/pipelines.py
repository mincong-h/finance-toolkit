import re
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
from .utils import Configuration, Summary


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


class BnpPipeline(Pipeline, metaclass=ABCMeta):
    @classmethod
    def parse_fr_float(cls, s: str) -> float:
        v = s.replace(",", ".").replace(" ", "")
        return float(v)

    @classmethod
    def read_raw(cls, csv: Path) -> Tuple[DataFrame, DataFrame]:
        # BNP Paribas stores the balance in the first line of the CSV
        with csv.open(encoding="ISO-8859-1") as f:
            first = next(f).strip()
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
        balances["Amount"] = balances["Amount"].apply(cls.parse_fr_float)
        del balances["mainCategory"]
        del balances["subCategory"]
        del balances["accountNum"]
        del balances["unknown"]

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
        tx["Type"] = ""
        tx["MainCategory"] = ""
        tx["SubCategory"] = ""
        return balances, tx


class BnpTransactionPipeline(BnpPipeline, TransactionPipeline):
    def guess_meta(self, df: DataFrame) -> DataFrame:
        if self.account.type == "CDI":
            df["Type"] = "credit"
        elif self.account.type in ["LVA", "LDD"]:
            df["Type"] = "transfer"
        elif self.account.type == "CHQ":
            df["Type"] = "expense"

        for i, row in df.iterrows():
            for c in self.cfg.autocomplete:
                # TODO compile before
                if re.compile(c.regex).match(row.Label):
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
            df["Type"] = "transfer"
        elif self.account.type == "CHQ":
            df["Type"] = "expense"

        for i, row in df.iterrows():
            for c in self.cfg.autocomplete:
                # TODO compile before
                if re.compile(c.regex).match(row.Label):
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
                # TODO compile before
                if re.compile(c.regex).match(row.Label):
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


class PipelineFactory:
    def __init__(self, cfg: Configuration):
        self.cfg = cfg

    def new_transaction_pipeline(self, account: Account) -> TransactionPipeline:
        if isinstance(account, BnpAccount):
            return BnpTransactionPipeline(account, self.cfg)
        if isinstance(account, BoursoramaAccount):
            return BoursoramaTransactionPipeline(account, self.cfg)
        if isinstance(account, FortuneoAccount):
            return FortuneoTransactionPipeline(account, self.cfg)
        return NoopTransactionPipeline(account, self.cfg)

    def new_balance_pipeline(self, account: Account) -> BalancePipeline:
        if isinstance(account, BnpAccount):
            return BnpBalancePipeline(account, self.cfg)
        if isinstance(account, BoursoramaAccount):
            return BoursoramaBalancePipeline(account, self.cfg)
        return GeneralBalancePipeline(account, self.cfg)

    def parse_balance_pipeline(self, path: Path) -> BalancePipeline:
        account = AccountParser(self.cfg).parse(path)
        return self.new_balance_pipeline(account)
