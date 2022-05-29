from pathlib import Path

import pandas as pd
from pandas import DataFrame

from .pipelines import TransactionPipeline


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
