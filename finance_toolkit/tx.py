"""Finance Tools"""

import os
from pathlib import Path
import re
from typing import List, Dict

import pandas as pd
import yaml
from pandas import DataFrame, Series

from .account import (
    Account,
    DegiroAccount,
    OctoberAccount,
)
from .bnp import BnpAccount
from .boursorama import BoursoramaAccount
from .caisse_epargne import CaisseEpargneAccount
from .fortuneo import FortuneoAccount
from .models import Configuration, Summary, TxCompletion, TxType, ExchangeRateConfig
from .pipeline import AccountParser
from .pipeline_factory import PipelineFactory
from .revolut import RevolutAccount


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
            elif company == "Caisse d'Epargne":
                if "expr" in fields:
                    print(
                        "Caisse d'Epargne has its own naming convention for downloaded files,"
                        f" you cannot overwrite it: expr={fields['expr']!r}"
                    )
                accounts.append(
                    CaisseEpargneAccount(
                        account_type=fields["type"],
                        account_id=symbolic_name,
                        account_num=fields["id"],
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
            elif company == "Revolut":
                if "expr" in fields:
                    print(
                        "Revolut has its own naming convention for downloaded files,"
                        f" you cannot overwrite it: expr={fields['expr']!r}"
                    )
                accounts.append(
                    RevolutAccount(
                        account_type=fields["type"],
                        account_id=symbolic_name,
                        account_num=fields["id"],
                        currency=fields["currency"],
                        extra_patterns=(
                            fields["expressions"] if "expressions" in fields else None
                        ),
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
                accounts.append(
                    Account(
                        account_type=fields["type"],
                        account_id=symbolic_name,
                        account_num=fields["id"],
                        patterns=["unknown"],
                        currency=fields["currency"],
                    )
                )

        # Validate no duplicate Caisse d'Epargne account IDs (suffixes)
        cls._validate_no_duplicate_caisse_epargne_accounts(accounts)

        accounts.sort(key=lambda a: a.id)
        return accounts

    @classmethod
    def _validate_no_duplicate_caisse_epargne_accounts(cls, accounts: List[Account]):
        """
        Validate that no two Caisse d'Epargne accounts have the same account number (suffix).
        Since account numbers are suffixes, duplicates would cause ambiguous file matching.
        """
        ce_accounts = [a for a in accounts if isinstance(a, CaisseEpargneAccount)]
        seen_nums: Dict[str, List[str]] = {}
        for account in ce_accounts:
            if account.num not in seen_nums:
                seen_nums[account.num] = []
            seen_nums[account.num].append(account.id)

        dup_details = [
            f"  - Account ID suffix '{num}' is used by: {', '.join(ids)}"
            for num, ids in seen_nums.items()
            if len(ids) > 1
        ]
        if dup_details:
            raise ValueError(
                "Duplicate Caisse d'Epargne account ID suffixes found. "
                "This would cause ambiguous file matching.\n" + "\n".join(dup_details)
            )

    @classmethod
    def load_categories(cls, raw: List[str]) -> List[str]:
        return [] if raw is None else raw

    @classmethod
    def load_autocomplete(cls, raw: List) -> List[TxCompletion]:
        return [] if raw is None else [TxCompletion.load(p) for p in raw]

    @classmethod
    def load_exchange_rates(cls, raw: Dict) -> ExchangeRateConfig:
        return ExchangeRateConfig(watched_currencies=raw["watched-currencies"])

    @classmethod
    def parse_yaml(cls, path: Path) -> Configuration:
        data = yaml.safe_load(path.read_text())
        accounts = cls.load_accounts(data["accounts"])
        categories = cls.load_categories(data["categories"])
        categories_to_rename = data["categories_to_rename"]
        autocomplete = cls.load_autocomplete(data["auto-complete"])
        download_dir = Path(data["download-dir"]).expanduser()
        root_dir = path.parent
        exchange_rate_cfg = cls.load_exchange_rates(data["exchange-rate"])
        return Configuration(
            accounts=accounts,
            categories=categories,
            categories_to_rename=categories_to_rename,
            autocomplete=autocomplete,
            download_dir=download_dir,
            root_dir=root_dir,
            exchange_rate_cfg=exchange_rate_cfg,
        )

    @classmethod
    def load(cls, path: Path) -> Configuration:
        cfg = cls.parse_yaml(path)
        # override download directory
        if os.getenv("DOWNLOAD_DIR"):
            cfg.download_dir = Path(os.getenv("DOWNLOAD_DIR")).expanduser()
        return cfg


LABELS = {
    "CHQ": "Compte de Chèque",
    "LVA": "Livret A",
    "LVR": "Livret",
    "LDD": "Livret Développement Durable",
    "CDI": "Crédit Immobilier",
    "AV1": "Assurant Vie (sans risque)",
    "STK": "Stock",
}


def validate_tx(row: Series, cfg: Configuration) -> str:
    if row.Type not in TxType.values():
        return f"Unknown transaction type: {row.Type}"

    category = f"{row.MainCategory}/{row.SubCategory}"
    if row.Type == TxType.EXPENSE.value and category not in cfg.categories():
        return f"Category {category!r} does not exist."

    return ""  # no error


def read_transactions(path: Path, cfg: Configuration) -> DataFrame:
    df = pd.read_csv(path, parse_dates=["Date"])
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


def rename_categories(df: DataFrame, cfg: Configuration) -> DataFrame:
    for old_category, new_category in cfg.categories_to_rename.items():
        old_main, old_sub = old_category.split("/")
        new_main, new_sub = new_category.split("/")
        selection = (df["MainCategory"] == old_main) & (df["SubCategory"] == old_sub)
        df.loc[selection, ["MainCategory", "SubCategory"]] = new_main, new_sub
    return df


def merge_bank_tx(dfs: List[DataFrame], cfg: Configuration) -> DataFrame:
    merged_df = dfs[0]
    for df in dfs[1:]:
        merged_df = merged_df.append(df, sort=False)

    merged_df = rename_categories(merged_df, cfg)

    return merged_df.reset_index(drop=True)


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
    summary = Summary(cfg)
    factory = PipelineFactory(cfg)
    for path in paths:
        for account in cfg.accounts:
            if account.match(path):
                factory.new_transaction_pipeline(account).run(path, summary)
                factory.new_balance_pipeline(account).run(path, summary)

        if re.match(r"Webstat_Export_(.+)\.csv", path.name):
            factory.new_exchange_rate_pipeline().run(path, summary)
    print(summary)


def convert(cfg: Configuration):
    parser = AccountParser(cfg)
    summary = Summary(cfg, action="convert")
    factory = PipelineFactory(cfg)

    for path in cfg.root_dir.glob("balance.*.csv"):
        result = parser.parse_path(path)
        if result and result.is_currency_conversion_needed:
            factory.new_convert_balance_pipeline(result.account).run(
                result.path, summary
            )
    print(summary)


def merge(cfg: Configuration):
    bank_transactions = []
    cols = [
        "Date",
        "Account",
        "Label",
        "Amount",
        "Type",
        "MainCategory",
        "SubCategory",
    ]
    for path in cfg.root_dir.glob("20[1-9]*/*.csv"):
        account = AccountParser(cfg).parse(path)
        df = read_transactions(path, cfg)
        df["Account"] = account.id
        bank_transactions.append(df[cols])

    tx = merge_bank_tx(bank_transactions, cfg)
    tx = tx.sort_values(by=["Date", "Account", "Label", "Amount"])
    tx["Month"] = tx["Date"].apply(lambda d: d.strftime("%Y-%m"))

    tx.to_csv(
        cfg.root_dir / "total.csv",
        columns=[
            "Date",
            "Month",
            "Account",
            "Label",
            "Amount",
            "Type",
            "MainCategory",
            "SubCategory",
        ],
        index=False,
    )

    # note: we only scan euro-related CSV files because euro is the base currencye
    b = merge_balances([p for p in cfg.root_dir.glob("balance.*.EUR.csv")], cfg)
    b.to_csv(cfg.root_dir / "balance.csv", index=False)
    print("Merge done")
