#!/usr/bin/env python3
"""Finance Tools"""
from pathlib import Path
from typing import List, Tuple, Dict

import pandas as pd
import yaml
from pandas import DataFrame, Series

from .accounts import (
    Account,
    BnpAccount,
    BoursoramaAccount,
    CartaAccount,
    DegiroAccount,
    FortuneoAccount,
    OctoberAccount,
)
from .pipelines import PipelineFactory, AccountParser
from .utils import Configuration, Summary


class Configurator:
    """
    Configurator parses and validates user configuration.
    """

    @classmethod
    def load_accounts(cls, raw: Dict) -> List[Account]:
        accounts = []
        for symbolic_name, fields in raw.items():
            company = fields["company"]
            if company == "BNP":
                if "expr" in fields:
                    print(
                        "BNP Paribas has its own naming convention for downloaded files,"
                        f" you cannot overwrite it: expr={fields['expr']!r}"
                    )
                accounts.append(
                    BnpAccount(
                        account_type=fields["type"],
                        account_id=symbolic_name,
                        account_num=fields["id"],
                    )
                )
            elif company == "Boursorama":
                if "expr" in fields:
                    print(
                        "Boursorama has its own naming convention for downloaded files,"
                        f" you cannot overwrite it: expr={fields['expr']!r}"
                    )
                accounts.append(
                    BoursoramaAccount(
                        account_type=fields["type"],
                        account_id=symbolic_name,
                        account_num=fields["id"],
                    )
                )
            elif company == "Carta":
                accounts.append(
                    CartaAccount(
                        account_type=fields["type"],
                        account_id=symbolic_name,
                        account_num=fields["id"],
                        pattern=fields["expr"],
                    )
                )
            elif company == "Degiro":
                accounts.append(
                    DegiroAccount(
                        account_type=fields["type"],
                        account_id=symbolic_name,
                        account_num=fields["id"],
                    )
                )
            elif company == "Fortuneo":
                if "expr" in fields:
                    print(
                        "Fortuneo has its own naming convention for downloaded files,"
                        f" you cannot overwrite it: expr={fields['expr']!r}"
                    )
                accounts.append(
                    FortuneoAccount(
                        account_type=fields["type"],
                        account_id=symbolic_name,
                        account_num=fields["id"],
                    )
                )
            elif company == "October":
                if "expr" in fields:
                    print(
                        "October has its own naming convention for downloaded files,"
                        f" you cannot overwrite it: expr={fields['expr']!r}"
                    )
                accounts.append(
                    OctoberAccount(
                        account_type=fields["type"],
                        account_id=symbolic_name,
                        account_num=fields["id"],  # full id required by data lookup
                    )
                )
            else:
                print(f"Unknown account: {symbolic_name}, fields={fields}")
        accounts.sort(key=lambda a: a.id)
        return accounts

    @classmethod
    def load_categories(cls, raw: List[str]) -> List[str]:
        return [] if raw is None else raw

    @classmethod
    def load_autocomplete(cls, raw: List) -> List[Tuple]:
        patterns = []
        if raw is not None:
            for pattern in raw:
                columns = (
                    pattern["type"],
                    pattern["cat"].split("/")[0],  # main category
                    pattern["cat"].split("/")[1],  # sub category
                    pattern["regular"],
                )
                patterns.append((columns, pattern["expr"]))
        return patterns

    @classmethod
    def parse_yaml(cls, path: Path) -> Configuration:
        data = yaml.safe_load(path.read_text())
        accounts = cls.load_accounts(data["accounts"])
        categories = cls.load_categories(data["categories"])
        autocomplete = cls.load_autocomplete(data["auto-complete"])
        download_dir = Path(data["download-dir"]).expanduser()
        root_dir = path.parent
        return Configuration(
            accounts=accounts,
            categories=categories,
            autocomplete=autocomplete,
            download_dir=download_dir,
            root_dir=root_dir,
        )


LABELS = {
    "CHQ": "Compte de Chèque",
    "LVA": "Livret A",
    "LVR": "Livret",
    "LDD": "Livret Développement Durable",
    "CDI": "Crédit Immobilier",
    "AV1": "Assurant Vie (sans risque)",
    "STK": "Stock",
}
TX_TYPES = {"income", "expense", "transfer", "credit"}


def validate_tx(row: Series, cfg: Configuration) -> str:
    if row.Type not in TX_TYPES:
        return f"Unknown transaction type: {row.Type}"

    category = f"{row.Category}/{row.SubCategory}"
    if row.Type == "expense" and category not in cfg.categories():
        return f"Category {category!r} does not exist."

    if row.IsRegular not in [True, False]:
        return f"Unknown regularity: {row.IsRegular}"

    return ""


def read_boursorama_tx(path: Path, cfg: Configuration) -> DataFrame:
    df = pd.read_csv(path, parse_dates=["dateOp", "dateVal"])
    df = df.drop(columns=["dateVal", "supplierFound"])
    df = df.rename(
        columns={
            "dateOp": "Date",
            "brsMainCategory": "ShortType",
            "brsSubCategory": "LongType",
            "mainCategory": "Category",
            "subCategory": "SubCategory",
        }
    )
    df = df[
        [
            "Date",
            "ShortType",
            "LongType",
            "Label",
            "Amount",
            "Type",
            "Category",
            "SubCategory",
            "IsRegular",
        ]
    ]
    errors = []
    for idx, row in df.iterrows():
        err = validate_tx(row, cfg)
        if err:
            df.drop(idx, inplace=True)
            errors.append((idx + 2, err))  # base-1 (+1) and header (+1)
    if errors:
        print(f"{path}:")
        for line, err in errors:
            print(f"  - Line {line}: {err}")
    return df


def read_bnp_tx(path: Path, cfg: Configuration) -> DataFrame:
    df = pd.read_csv(path, parse_dates=["Date"])
    df = df.rename(
        columns={
            "bnpMainCategory": "ShortType",
            "bnpSubCategory": "LongType",
            "mainCategory": "Category",
            "subCategory": "SubCategory",
        }
    )
    errors = []
    for idx, row in df.iterrows():
        err = validate_tx(row, cfg)
        if err:
            df.drop(idx, inplace=True)
            errors.append((idx + 2, err))  # base-1 (+1) and header (+1)
    if errors:
        print(f"{path}:")
        for line, err in errors:
            print(f"  - Line {line}: {err}")
    return df


def merge_bank_tx(dfs: List[DataFrame]) -> DataFrame:
    m = dfs[0]
    for df in dfs[1:]:
        m = m.append(df, sort=False)
    return m.reset_index(drop=True)


def merge_balances(paths: List[Path], cfg: Configuration) -> DataFrame:
    m = pd.DataFrame(columns=["Date", "Account", "AccountId", "Amount", "AccountType"])

    for path in paths:
        pipeline = PipelineFactory(cfg).parse_balance_pipeline(path)
        df = pipeline.read_balance(path)
        m = m.append(df, sort=False)

    m = m.sort_values(by=["Date", "Account"])
    m = m[["Date", "Account", "AccountId", "Amount", "AccountType"]]
    return m.reset_index(drop=True)


# --------------------
# Top Level Commands
# --------------------


def move(cfg: Configuration):
    paths = [child for child in cfg.download_dir.iterdir() if child.is_file()]
    summary = Summary(cfg.download_dir)
    factory = PipelineFactory(cfg)
    for path in paths:
        for account in cfg.accounts:
            if account.match(path):
                pipeline = factory.new_transaction_pipeline(account)
                pipeline.run(path, cfg.root_dir, summary)
                balance_pipeline = factory.new_balance_pipeline(account)
                balance_pipeline.run(path, cfg.root_dir, summary)
    print(summary)


def merge(cfg: Configuration):
    bank_transactions = []
    cols = [
        "Date",
        "Account",
        "ShortType",
        "LongType",
        "Label",
        "Amount",
        "Type",
        "Category",
        "SubCategory",
        "IsRegular",
    ]
    for path in cfg.root_dir.glob("20[1-9]*/*.csv"):
        a = AccountParser(cfg).parse(path)
        if isinstance(a, BoursoramaAccount):
            df = read_boursorama_tx(path, cfg)
            df = df.rename(
                columns={
                    "dateOp": "Date",
                    "brsMainCategory": "LongType",
                    "brsSubCategory": "ShortType",
                    "mainCategory": "Category",
                    "subCategory": "SubCategory",
                }
            )
            df["Account"] = a.id
            bank_transactions.append(df[cols])
        elif a.type in {"CHQ", "LVA", "LDD", "CDI"}:
            df = read_bnp_tx(path, cfg)
            df = df.rename(
                columns={
                    "bnpMainCategory": "LongType",
                    "bnpSubCategory": "ShortType",
                    "mainCategory": "Category",
                    "subCategory": "SubCategory",
                }
            )
            df["Account"] = a.id
            bank_transactions.append(df[cols])

    tx = merge_bank_tx(bank_transactions)
    tx = tx.sort_values(
        by=["Date", "Account", "ShortType", "LongType", "Label", "Amount"]
    )
    tx.to_csv(cfg.root_dir / "total.csv", columns=cols, index=False)
    # TODO export results
    b = merge_balances([p for p in cfg.root_dir.glob("balance.*.csv")], cfg)
    b.to_csv(cfg.root_dir / "balance.csv", index=False)
    print("Merge done")