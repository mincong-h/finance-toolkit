import logging
from abc import ABCMeta, abstractmethod
from pathlib import Path

import pandas as pd
from pandas import DataFrame

from .account import Account
from .models import Configuration, Summary


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

        # add custom columns if needed
        if "MainCategory" not in tx.columns:
            tx["MainCategory"] = ""
        if "SubCategory" not in tx.columns:
            tx["SubCategory"] = ""
        if "Type" not in tx.columns:
            tx["Type"] = ""

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

    def append_transactions(self, csv: Path, new_transactions: DataFrame):
        df = new_transactions.copy()
        if csv.exists():
            existing = pd.read_csv(csv, parse_dates=["Date"])

            # keep backward compatibility: existing data don't have column "Currency"
            if "Currency" in existing.columns:
                logging.debug(f'Column "Currency" exists in file: {csv}, skip filling')
            else:
                logging.debug(
                    f'Column "Currency" does not exist in file: {csv}, filling it with the account currency'  # noqa
                )
                existing = existing.assign(
                    Currency=lambda row: self.account.currency_symbol
                )

            df = df.append(existing, sort=False)

        df = df.drop_duplicates(subset=["Date", "Label", "Amount"], keep="last")
        df = df.sort_values(by=["Date", "Label"])
        df.to_csv(
            csv,
            columns=[
                "Date",
                "Label",
                "Amount",
                "Currency",
                "Type",
                "MainCategory",
                "SubCategory",
            ],
            index=None,
            date_format="%Y-%m-%d",
        )

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
        Read new transactions from a CSV file, probably downloaded from internet. The
        implementation of this abstract method should return a data-frame containing the following
        fields, the order of the fields should be respected as well:

            1. "Date": pandas.Timestamp, required.
            2. "Label": string, required.
            3. "Amount": float, required.
            # TODO can we remove these fields?
            4. "Type": string, required.
            5. "MainCategory": string, required.
            6. "SubCategory": string, required.

        This allows merging transactions from different accounts in the downstream.

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
        new_lines = self.read_new_balances(path)

        original_balance_file = self.cfg.root_dir / self.account.balance_filename
        original_balance_df = self.insert_balance(original_balance_file, new_lines)
        self.write_balance(original_balance_file, original_balance_df)

        summary.add_source(path)
        summary.add_target(original_balance_file)

        if self.account.is_currency_conversion_needed:
            converted_balance_file = self.cfg.root_dir / self.account.converted_balance_filename
            converted_balance_df = self.convert_balance_to_euro(original_balance_df)
            self.write_balance(converted_balance_file, converted_balance_df)
            summary.add_target(converted_balance_file)

    def read_balance(self, path: Path) -> DataFrame:
        logging.debug(f'Reading balance from {path}')
        df = pd.read_csv(path, parse_dates=["Date"])
        df = df[["Date", "Amount"]]
        df["Account"] = self.account.id
        df["AccountId"] = self.account.num
        df["AccountType"] = self.account.type
        return df

    def insert_balance(self, csv: Path, new_lines: DataFrame) -> DataFrame:
        logging.debug(f"Writing balance to {csv}")
        df = new_lines.copy()
        if csv.exists():
            existing = pd.read_csv(csv, parse_dates=["Date"])

            # keep backward compatibility: existing data don't have column "Currency"
            if "Currency" in existing.columns:
                logging.debug(f'Column "Currency" exists in file: {csv}, skip filling')
            else:
                logging.debug(
                    f'Column "Currency" does not exist in file: {csv}, filling it with the account currency: {self.account.currency_symbol}'  # noqa
                )
                existing = existing.assign(
                    Currency=lambda row: self.account.currency_symbol
                )

            df = df.append(existing, sort=False)

        df = df.drop_duplicates(subset=["Date"], keep="last")
        df = df.sort_values(by="Date")
        return df

    def write_balance(self, csv: Path, df: DataFrame) -> DataFrame:
        df.to_csv(csv, index=None, columns=["Date", "Amount", "Currency"], float_format="%.2f")

    def convert_balance_to_euro(self, balance_df: DataFrame) -> DataFrame:
        balance_df = balance_df.copy()
        balance_df["Date"] = pd.to_datetime(balance_df["Date"])
        balance_df["Date"] = balance_df["Date"].dt.date  # remove time before the join

        exchange_rate_df = pd.read_csv(self.cfg.get_exchange_rate_csv_path(), parse_dates=["Date"])
        exchange_rate_df = exchange_rate_df.fillna(method="ffill")  # forward fill: propagate last valid observation forward to next valid
        exchange_rate_df["EUR"] = 1.0

        logging.debug(f"Converting the balance of account {self.account.id} from {self.account.currency_symbol} to EUR")
        result_df = pd.merge(balance_df, exchange_rate_df,
                             left_on="Date",
                             right_on=exchange_rate_df["Date"].dt.date,
                             how="left")
        result_df["Amount"] = result_df["Amount"] * result_df[self.account.currency_symbol]
        result_df["Currency"] = "EUR"
        result_df = result_df[["Date", "Amount", "Currency"]]

        return result_df

    @abstractmethod
    def read_new_balances(self, csv: Path) -> DataFrame:
        pass


class GeneralBalancePipeline(BalancePipeline):
    def run(self, path: Path, summary: Summary):
        pass

    def read_new_balances(self, csv: Path) -> DataFrame:
        pass


class AccountParser:
    def __init__(self, cfg: Configuration):
        self.accounts = cfg.as_dict()

    def parse(self, path: Path) -> Account:
        parts = path.name.split(".")
        account_id = parts[1]
        if len(parts) == 3 and account_id in self.accounts:
            return self.accounts[account_id]
        return Account(
            account_type="unknown",
            account_id="unknown",
            account_num="unknown",
            patterns=[r"unknown"],
        )


class PipelineDataError(ValueError):
    def __init__(
        self,
        msg: str,
        path: Path,
        headers: str,
        pandas_error: ValueError,
        pandas_kwargs,
    ):
        self.msg = msg
        self.path = path
        self.headers = headers
        self.pandas_kwargs = pandas_kwargs
        self.pandas_error = pandas_error

    def __str__(self):
        return f"""\
{self.msg} Details:
  path={self.path}
  headers={self.headers}
  pandas_kwargs={self.pandas_kwargs}
  pandas_error={self.pandas_error}"""
