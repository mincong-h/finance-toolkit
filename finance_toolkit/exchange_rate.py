import logging
from abc import ABCMeta
from datetime import datetime
from pathlib import Path
import pandas as pd
from pandas import DataFrame
import re

from .pipeline import Pipeline
from .models import Summary


class ExchangeRatePipeline(Pipeline, metaclass=ABCMeta):
    """
    Pipeline to download exchange rates from the Bank of France and save them to a CSV file.

    The 6 first lines of the CSV file are special. For example:

        Titre :;Dollar australien (AUD);Lev bulgare (BGN);Real brésilien (BRL)
        Code série :;EXR.D.AUD.EUR.SP00.A;EXR.D.BGN.EUR.SP00.A;EXR.D.BRL.EUR.SP00.A
        Unité :;Dollar Australien (AUD);Lev Nouveau (BGN);Real Bresilien (BRL)
        Magnitude :;Unités (0);Unités (0);Unités (0)
        Méthode d'observation :;Fin de période (E);Fin de période (E);Fin de période (E)
        Source :;BCE (Banque Centrale Européenne) (4F0);BCE (Banque Centrale Européenne) (4F0);...

    This has an impact on the way we read the CSV file.
    """
    def run(self, csv: Path, summary: Summary) -> None:
        logging.debug(f"Running {self.__class__.__name__} on {csv}")
        with csv.open() as f:
            #
            next(f)  # title (Titre)
            next(f)  # series code (Code série)
            unit_str = next(f)  # units (Unité)
            logging.debug(unit_str)

        rate_df = pd.read_csv(
            csv,
            date_parser=lambda s: datetime.strptime(s, "%d/%m/%Y"),
            parse_dates=['Date'],
            decimal=",",
            delimiter=";",
            na_values="-",
            skiprows=6,  # Titre, Code série, Unité, Magnitude, Méthode d'observation, Source
            names=[self.extract_code(u) for u in unit_str.split(";")]
        )
        rate_df = rate_df[['Date'] + self.cfg.exchange_rate_currencies]
        rate_df = rate_df.sort_values(by=['Date'], ascending=True)
        today = get_today()
        while rate_df.iloc[-1]['Date'] < today:
            rate_df = rate_df.append({'Date': rate_df.iloc[-1]['Date'] + pd.DateOffset(1)}, ignore_index=True)

        target = self.cfg.exchange_rate_csv_path
        summary.add_source(csv)
        summary.add_target(target)

        logging.debug(f"Saving exchange rates to {target}")
        logging.debug(rate_df.tail())
        rate_df.to_csv(target, index=False, date_format="%Y-%m-%d")

    def extract_code(self, s: str) -> str:
        match = re.search(r'\((\w+)\)', s)
        if match:
            return match.group(1)
        else:
            return 'Date'


def get_today():  # faciliate testing
    return datetime.today()


class ConvertBalancePipeline(Pipeline, metaclass=ABCMeta):
    def run(self, balance_csv: Path, summary: Summary) -> None:
        logging.debug(f"Running {self.__class__.__name__} on {balance_csv}")

        balance_df = pd.read_csv(balance_csv, parse_dates=["Date"])

        converted_balance_file = self.cfg.root_dir / self.account.converted_balance_filename
        converted_balance_df = self.convert_balance_to_euro(balance_df)

        self.write_balance(converted_balance_file, converted_balance_df)

        summary.add_source(balance_csv)
        summary.add_target(converted_balance_file)

    def convert_balance_to_euro(self, balance_df: DataFrame) -> DataFrame:
        balance_df = balance_df.copy()
        balance_df["_DateOnly"] = pd.to_datetime(balance_df["Date"]).dt.date  # remove time part

        exchange_rate_df = pd.read_csv(self.cfg.exchange_rate_csv_path, parse_dates=["Date"])
        exchange_rate_df.rename(columns={"Date": "_ExDate"}, inplace=True)
        # forward fill: propagate last valid observation forward to next valid
        exchange_rate_df = exchange_rate_df.fillna(method="ffill")
        exchange_rate_df["EUR"] = 1.0

        logging.debug(f"Converting the balance of account {self.account.id} from {self.account.currency_symbol} to EUR")  # noqa
        result_df = pd.merge(balance_df, exchange_rate_df,
                             left_on="_DateOnly",
                             right_on=exchange_rate_df["_ExDate"].dt.date,
                             how="left")

        # amount in EUR = amount in currency / exchange rate
        # e.g. amount in EUR = 100 USD / 1.0956 = 91.29 EUR
        result_df["Amount"] = result_df["Amount"] / result_df[self.account.currency_symbol]
        result_df["Currency"] = "EUR"
        result_df = result_df[["Date", "Amount", "Currency"]]

        return result_df

    def write_balance(self, csv: Path, df: DataFrame) -> DataFrame:
        df.to_csv(csv, index=None, columns=["Date", "Amount", "Currency"], float_format="%.2f")
