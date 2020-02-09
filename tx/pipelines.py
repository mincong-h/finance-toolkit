import re
from abc import ABCMeta, abstractmethod
from pathlib import Path
from typing import Tuple

import pandas as pd
from pandas import DataFrame

from .accounts import Account, BnpAccount, BoursoramaAccount
from .utils import Configuration, Summary


class Pipeline(metaclass=ABCMeta):
    def __init__(self, account: Account, cfg: Configuration):
        self.account = account
        self.cfg = cfg

    @abstractmethod
    def run(self, path: Path, dest_dir: Path, summary: Summary) -> None:
        """
        Run pipeline

        :param path: the source path where data should be read
        :param dest_dir: the destination, a directory path where data should be written
        :param summary: the summary containing results of different pipelines
        """


class TransactionPipeline(Pipeline):
    def run(self, path: Path, dest_dir: Path, summary: Summary) -> None:
        pass

    def guess_meta(self, df: DataFrame) -> DataFrame:
        return df

    def read_balance(self, path: Path) -> DataFrame:
        df = pd.read_csv(path, parse_dates=["Date"])
        df = df[["Date", "Amount"]]
        df["Account"] = self.account.id
        df["AccountId"] = self.account.num
        df["AccountType"] = self.account.type
        return df

    def write_balances(self, path: Path, new_balances: DataFrame):
        pass


class BalancePipeline(Pipeline):
    def run(self, path: Path, dest_dir: Path, summary: Summary) -> None:
        pass

    def read_balance(self, path: Path) -> DataFrame:
        df = pd.read_csv(path, parse_dates=["Date"])
        df = df[["Date", "Amount"]]
        df["Account"] = self.account.id
        df["AccountId"] = self.account.num
        df["AccountType"] = self.account.type
        return df

    def write_balances(self, path: Path, new_balances: DataFrame):
        pass


class BnpPipeline(Pipeline, metaclass=ABCMeta):
    @classmethod
    def read_raw(cls, csv: Path) -> Tuple[DataFrame, DataFrame]:
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
        balances["Amount"] = balances["Amount"].apply(
            lambda v: v.replace(",", ".").replace(" ", "")
        )
        balances["Amount"] = balances["Amount"].astype(float)
        del balances["unknown"]

        tx = pd.read_csv(
            csv,
            date_parser=lambda s: pd.datetime.strptime(s, "%d/%m/%Y"),
            decimal=",",
            delimiter=";",
            encoding="ISO-8859-1",
            names=["Date", "bnpMainCategory", "bnpSubCategory", "Label", "Amount"],
            parse_dates=["Date"],
            skipinitialspace=True,
            skiprows=1,
            thousands=" ",
        )
        tx = tx.fillna("")
        tx["Type"] = ""
        tx["mainCategory"] = ""
        tx["subCategory"] = ""
        tx["IsRegular"] = ""
        return balances, tx


class BnpTransactionPipeline(BnpPipeline, TransactionPipeline):
    def run(self, path: Path, dest_dir: Path, summary: Summary) -> None:
        # read
        tx = self.read_new_transactions(path)
        summary.add_source(path)

        # process
        tx = self.guess_meta(tx)
        tx["month"] = tx["Date"].apply(lambda date: date.strftime("%Y-%m"))

        # write
        for m in tx["month"].unique():
            d = dest_dir / m
            if not d.exists():
                d.mkdir()
            csv = d / f"{m}.{self.account.filename}"
            self.append_tx_file(csv, tx[tx["month"] == m])
            summary.add_target(csv)

    def guess_meta(self, df: DataFrame) -> DataFrame:
        if self.account.type == "CDI":
            df["Type"], df["IsRegular"] = "credit", True
        elif self.account.type in ["LVA", "LDD"]:
            df["Type"], df["IsRegular"] = "transfer", True
        elif self.account.type == "CHQ":
            df["Type"] = "expense"

        for i, row in df.iterrows():
            for values, regex in self.cfg.autocomplete:
                if re.compile(regex).match(row.Label):
                    (
                        df.loc[i, "Type"],
                        df.loc[i, "mainCategory"],
                        df.loc[i, "subCategory"],
                        df.loc[i, "IsRegular"],
                    ) = values
                    break
        return df

    @classmethod
    def append_tx_file(cls, csv: Path, data: DataFrame) -> None:
        df = data.copy()
        if csv.exists():
            existing = pd.read_csv(csv, parse_dates=["Date"])
            df = df.append(existing, sort=False)

        df = df.drop_duplicates(subset=["Date", "Label", "Amount"], keep="last")
        df = df.sort_values(by=["Date", "bnpMainCategory", "bnpSubCategory", "Label"])

        cols = [
            "Date",
            "bnpMainCategory",
            "bnpSubCategory",
            "Label",
            "Amount",
            "Type",
            "mainCategory",
            "subCategory",
            "IsRegular",
        ]
        df.to_csv(csv, columns=cols, index=None)

    def read_new_transactions(self, path: Path) -> DataFrame:
        _, tx = self.read_raw(path)
        return tx


class BnpBalancePipeline(BnpPipeline, BalancePipeline):
    def run(self, path: Path, dest_dir: Path, summary: Summary) -> None:
        balances = self.read_new_balances(path)
        b_path = dest_dir / f"balance.{self.account.filename}"
        self.write_balances(b_path, balances)

        summary.add_source(path)
        summary.add_target(b_path)

    @classmethod
    def write_balances(cls, csv: Path, new_lines: DataFrame) -> None:
        df = new_lines.copy()
        if csv.exists():
            existing = pd.read_csv(csv, parse_dates=["Date"])
            df = df.append(existing, sort=False)
        df = df.drop_duplicates(subset=["accountNum", "Date"], keep="last")
        df = df.sort_values(by="Date")
        df.to_csv(csv, index=None)

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
            columns={
                "category": "brsMainCategory",
                "categoryParent": "brsSubCategory",
                "label": "Label",
                "amount": "Amount",
            }
        )

        m = self.account.pattern.match(csv.name)
        balances = df.groupby("accountNum")["accountBalance"].max().to_frame()
        balances.reset_index(inplace=True)
        balances["Date"] = pd.datetime.strptime(m.group(1), "%d-%m-%Y") - pd.Timedelta(
            "1 day"
        )
        balances = balances[balances["accountNum"].map(self.account.is_account)]
        balances = balances.reset_index(drop=True)
        balances = balances.rename(columns={"accountBalance": "Amount"})
        return balances, transactions


class BoursoramaTransactionPipeline(BoursoramaPipeline, TransactionPipeline):
    def run(self, path: Path, dest_dir: Path, summary: Summary):
        # read
        tx = self.read_new_transactions(path)
        summary.add_source(path)

        # process
        tx = self.guess_meta(tx)
        tx["month"] = tx.dateOp.apply(lambda date: date.strftime("%Y-%m"))

        # write
        for m in tx["month"].unique():
            d = dest_dir / m
            if not d.exists():
                d.mkdir()
            csv = d / f"{m}.{self.account.filename}"
            self.append_tx(csv, tx[tx["month"] == m])
            summary.add_target(csv)

    def guess_meta(self, df: DataFrame) -> DataFrame:
        if self.account.type == "LVR":
            df["Type"], df["IsRegular"] = "transfer", False
        elif self.account.type == "CHQ":
            df["Type"], df["IsRegular"] = "expense", False

        for i, row in df.iterrows():
            for values, regex in self.cfg.autocomplete:
                if re.compile(regex).match(row.Label):
                    (
                        df.loc[i, "Type"],
                        df.loc[i, "mainCategory"],
                        df.loc[i, "subCategory"],
                        df.loc[i, "IsRegular"],
                    ) = values
                    break
        return df

    @classmethod
    def append_tx(cls, csv: Path, data: DataFrame):
        df = data.copy()
        cols = [
            "dateOp",
            "dateVal",
            "Label",
            "brsMainCategory",
            "brsSubCategory",
            "supplierFound",
            "Amount",
            "Type",
            "mainCategory",
            "subCategory",
            "IsRegular",
        ]

        if csv.exists():
            existing = pd.read_csv(csv, parse_dates=["dateOp", "dateVal"])
            df = df.append(existing, sort=False)

        df = df.drop_duplicates(subset=["dateOp", "Label", "Amount"], keep="last")
        df = df.sort_values(by=["dateOp", "brsMainCategory", "brsSubCategory", "Label"])

        df.to_csv(csv, columns=cols, index=None, date_format="%Y-%m-%d")

    def read_new_transactions(self, path: Path):
        _, tx = self.read_raw(path)
        return tx


class BoursoramaBalancePipeline(BoursoramaPipeline, BalancePipeline):
    def run(self, path: Path, dest_dir: Path, summary: Summary):
        balances = self.read_new_balances(path)
        balance_file = dest_dir / f"balance.{self.account.filename}"
        self.write_balances(balance_file, balances)

        summary.add_source(path)
        summary.add_target(balance_file)

    @classmethod
    def write_balances(cls, csv: Path, new_lines: DataFrame):
        df = pd.read_csv(csv, parse_dates=["Date"])
        df = df.append(new_lines, sort=False)
        df = df.drop_duplicates(subset="Date", keep="last")
        df = df.sort_values(by="Date")
        df.to_csv(csv, index=None, columns=["Date", "Amount"])

    def read_new_balances(self, csv: Path) -> DataFrame:
        balances, _ = self.read_raw(csv)
        return balances


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

    def new_transaction_pipeline(self, account: Account):
        if isinstance(account, BnpAccount):
            return BnpTransactionPipeline(account, self.cfg)
        if isinstance(account, BoursoramaAccount):
            return BoursoramaTransactionPipeline(account, self.cfg)
        return TransactionPipeline(account, self.cfg)

    def new_balance_pipeline(self, account: Account):
        if isinstance(account, BnpAccount):
            return BnpBalancePipeline(account, self.cfg)
        if isinstance(account, BoursoramaAccount):
            return BoursoramaBalancePipeline(account, self.cfg)
        return BalancePipeline(account, self.cfg)

    def parse_balance_pipeline(self, path: Path):
        account = AccountParser(self.cfg).parse(path)
        return self.new_transaction_pipeline(account)
