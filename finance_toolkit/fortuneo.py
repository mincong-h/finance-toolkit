from pathlib import Path

import pandas as pd
from pandas import DataFrame

from .account import Account
from .pipeline import TransactionPipeline


class FortuneoAccount(Account):
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
                r"HistoriqueOperations_(\d+)_du_\d{2}_\d{2}_\d{4}_au_\d{2}_\d{2}_\d{4}\.csv"
            ],
        )


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

        # Fortuneo does not provide currency information explicitly, so we create it ourselves.
        tx = tx.assign(Currency=lambda row: self.account.currency_symbol.symbol)
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
                "Currency",
                "Type",
                "MainCategory",
                "SubCategory",
            ]
        ]
        return tx
