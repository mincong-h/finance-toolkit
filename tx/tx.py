#!/usr/bin/env python3
"""Finance Tools"""
import re
from pathlib import Path
from typing import List, Tuple, Dict, Set
import yaml

import pandas as pd
from pandas import DataFrame, Series
from .accounts import (
    Account,
    BnpAccount,
    BoursoramaAccount,
    CartaAccount,
    DegiroAccount,
    OctoberAccount,
)


class Summary:
    def __init__(self, source_dir: Path):
        self.source_dir = source_dir
        self.sources = set()
        self.targets = set()

    def add_target(self, target: Path) -> None:
        self.targets.add(target)

    def add_source(self, source: Path) -> None:
        self.sources.add(source)

    def __repr__(self) -> str:
        if self.sources:
            s = "\n".join([f"- {s}" for s in sorted(self.sources)])
            t = "\n".join([f"- {t}" for t in sorted(self.targets)])
            return f"""\
$$$ Summary $$$
---------------
{len(self.sources)} files copied.
---------------
Sources:
{s}
Targets:
{t}
Finished."""
        else:
            return f"""\
$$$ Summary $$$
---------------
No CSV found in "{self.source_dir}".
---------------
Finished."""


class Configuration:
    """
    Type-safe representation of the user configuration.
    """

    def __init__(
        self,
        accounts: List[Account],
        categories: List[str],
        autocomplete: List[Tuple],
        download_dir: Path,
        root_dir: Path,
    ):
        self.accounts: List[Account] = accounts
        self.category_set: Set[str] = set(categories)
        self.autocomplete: List[Tuple] = autocomplete
        self.download_dir: Path = download_dir
        self.root_dir: Path = root_dir

    def as_dict(self) -> Dict[str, Account]:
        return {a.id: a for a in self.accounts}

    def categories(self, cat_filter=lambda x: True) -> List[str]:
        """
        Gets configured categories.

        :param cat_filter: optional category filter, default to no-op filter (do nothing)
        :return: categories without duplicate, order is guaranteed
        """
        return sorted(c for c in filter(cat_filter, self.category_set))


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


class AccountPipeline:
    def __init__(self, account: Account, cfg: Configuration):
        self.account = account
        self.cfg = cfg

    def integrate(self, path: Path, dest_dir: Path, summary: Summary) -> None:
        # do nothing
        return

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
        # do nothing
        return

    @classmethod
    def parse_account(cls, path: Path, cfg: Configuration) -> Account:
        parts = path.name.split(".")
        account_id = parts[1]
        accounts = cfg.as_dict()
        if len(parts) == 3 and account_id in accounts:
            return accounts[account_id]
        return Account("unknown", "unknown", "unknown", r"unknown")

    @classmethod
    def create_pipeline(cls, account: Account, cfg: Configuration):
        if isinstance(account, BnpAccount):
            return BnpPipeline(account, cfg)
        if isinstance(account, BoursoramaAccount):
            return BoursoramaPipeline(account, cfg)
        return AccountPipeline(account, cfg)

    @classmethod
    def create_pipeline_from_path(cls, path: Path, cfg: Configuration):
        return cls.create_pipeline(cls.parse_account(path, cfg), cfg)


class BnpPipeline(AccountPipeline):
    def integrate(self, path: Path, dest_dir: Path, summary: Summary) -> None:
        # read
        balances, tx = self.read_raw(path)
        summary.add_source(path)

        # process
        tx = self.guess_meta(tx)
        tx["month"] = tx["Date"].apply(lambda date: date.strftime("%Y-%m"))
        b_path = dest_dir / f"balance.{self.account.filename}"

        # write
        self.write_balances(b_path, balances)
        summary.add_target(b_path)
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

    def read_balance(self, path: Path) -> DataFrame:
        df = pd.read_csv(path, parse_dates=["Date"])
        df = df[["Date", "Amount"]]
        df["Account"] = self.account.id
        df["AccountId"] = self.account.num
        df["AccountType"] = self.account.type
        return df

    @classmethod
    def write_balances(cls, csv: Path, new_lines: DataFrame) -> None:
        df = new_lines.copy()
        if csv.exists():
            existing = pd.read_csv(csv, parse_dates=["Date"])
            df = df.append(existing, sort=False)
        df = df.drop_duplicates(subset=["accountNum", "Date"], keep="last")
        df = df.sort_values(by="Date")
        df.to_csv(csv, index=None)

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


class BoursoramaPipeline(AccountPipeline):
    def integrate(self, path: Path, dest_dir: Path, summary: Summary):
        # read
        balances, tx = self.read_raw(path)
        summary.add_source(path)

        # process
        tx = self.guess_meta(tx)
        tx["month"] = tx.dateOp.apply(lambda date: date.strftime("%Y-%m"))
        balance_file = dest_dir / f"balance.{self.account.filename}"

        # write
        self.write_balances(balance_file, balances)
        summary.add_target(balance_file)
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

    def read_balance(self, path: Path) -> DataFrame:
        df = pd.read_csv(path, parse_dates=["Date"])
        df["Account"] = self.account.id
        df["AccountId"] = self.account.num
        df["AccountType"] = self.account.type
        return df

    @classmethod
    def write_balances(cls, csv: Path, new_lines: DataFrame):
        df = pd.read_csv(csv, parse_dates=["Date"])
        df = df.append(new_lines, sort=False)
        df = df.drop_duplicates(subset="Date", keep="last")
        df = df.sort_values(by="Date")
        df.to_csv(csv, index=None, columns=["Date", "Amount"])

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
        pipeline = AccountPipeline.create_pipeline_from_path(path, cfg)
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
    for path in paths:
        for account in cfg.accounts:
            if account.match(path):
                pipeline = AccountPipeline.create_pipeline(account, cfg)
                pipeline.integrate(path, cfg.root_dir, summary)
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
        a = AccountPipeline.parse_account(path, cfg)
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
