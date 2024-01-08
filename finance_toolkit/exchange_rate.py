import logging
from abc import ABCMeta
import datetime
from pathlib import Path
import pandas as pd
import re

from .pipeline import Pipeline
from .models import Summary


class ExchangeRatePipeline(Pipeline, metaclass=ABCMeta):
    """
    Pipeline to download exchange rates from the Bank of France and save them to a CSV file.
    """
    def run(self, csv: Path, summary: Summary) -> None:
        logging.debug(f"Running {self.__class__.__name__} on {csv}")
        with csv.open() as f:
            # The 6 first lines of the CSV file are special. For example:
            #
            #     Titre :;Dollar australien (AUD);Lev bulgare (BGN);Real brésilien (BRL)
            #     Code série :;EXR.D.AUD.EUR.SP00.A;EXR.D.BGN.EUR.SP00.A;EXR.D.BRL.EUR.SP00.A
            #     Unité :;Dollar Australien (AUD);Lev Nouveau (BGN);Real Bresilien (BRL)
            #     Magnitude :;Unités (0);Unités (0);Unités (0)
            #     Méthode d'observation :;Fin de période (E);Fin de période (E);Fin de période (E)
            #     Source :;BCE (Banque Centrale Européenne) (4F0);BCE (Banque Centrale Européenne) (4F0);BCE (Banque Centrale Européenne) (4F0)
            #
            next(f)  # title (Titre)
            next(f)  # series code (Code série)
            unit_str = next(f)  # units (Unité)
            logging.debug(unit_str)

        rates = pd.read_csv(
            csv,
            date_parser=lambda s: datetime.strptime(s, "%d/%m/%Y"),
            decimal=",",
            delimiter=";",
            skiprows=6,  # Titre, Code série, Unité, Magnitude, Méthode d'observation, Source
            names=[self.extract_code(u) for u in unit_str.split(";")]
        )
        rates = rates[['Dates', 'USD', 'CNY']]
        logging.debug(f"Head of {csv}\n{rates.head()}")

    def extract_code(self, s: str) -> str:
        match = re.search(r'\((\w+)\)', s)
        if match:
            return match.group(1)
        else:
            return 'Dates'
