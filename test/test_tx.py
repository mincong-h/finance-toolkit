import re
from unittest.mock import patch, call

import pandas as pd
import yaml
from pandas.testing import assert_frame_equal

from finance_toolkit import tx
from finance_toolkit.account import Account
from finance_toolkit.models import (
    Configuration,
    TxCompletion,
)
from finance_toolkit.tx import (
    BnpAccount,
    BoursoramaAccount,
    Configurator,
    DegiroAccount,
    FortuneoAccount,
    OctoberAccount,
    RevolutAccount,
)


# ---------- Top Level Functions ----------


def test_read_bnp_tx_ok(cfg):
    cfg.category_set.add("food/workfood")

    csv = cfg.root_dir / "2019-03.mhuang-CHQ.csv"
    csv.write_text(
        """\
Date,Label,Amount,Type,MainCategory,SubCategory
2018-04-30,DU 270418 MC DONALDS PARIS 18 CARTE 4974,-4.95,expense,food,workfood
"""
    )
    actual_df = tx.read_transactions(csv, cfg)
    expected_df = pd.DataFrame(
        {
            "Date": pd.Timestamp("2018-04-30"),
            "Label": "DU 270418 MC DONALDS PARIS 18 CARTE 4974",
            "Amount": -4.95,
            "Type": "expense",
            "MainCategory": "food",
            "SubCategory": "workfood",
        },
        index=[0],
    )
    assert_frame_equal(actual_df, expected_df)


@patch("builtins.print")
def test_read_bnp_tx_validate_errors(mocked_print, cfg):
    cfg.category_set.add("food/restaurant")

    csv = cfg.root_dir / "2019-03.mhuang-CHQ.csv"
    csv.write_text(
        """\
Date,Label,Amount,Type,MainCategory,SubCategory
2018-04-30,myLabel,-1.0,expense,food,restaurant
2018-04-30,myLabel,-2.0,expense,,
2018-04-30,myLabel,-3.0,expense,food,
2018-04-30,myLabel,-4.0,expense,,restaurant
"""
    )
    actual_df = tx.read_transactions(csv, cfg)
    expected_df = pd.DataFrame(
        {
            "Date": pd.Timestamp("2018-04-30"),
            "Label": "myLabel",
            "Amount": -1.0,
            "Type": "expense",
            "MainCategory": "food",
            "SubCategory": "restaurant",
        },
        index=[0],
    )
    assert_frame_equal(actual_df, expected_df)
    assert mocked_print.mock_calls == [
        call(f"{csv}:"),
        call("  - Line 3: Category 'nan/nan' does not exist."),
        call("  - Line 4: Category 'food/nan' does not exist."),
        call("  - Line 5: Category 'nan/restaurant' does not exist."),
    ]


def test_read_boursorama_tx_ok(cfg):
    cfg.autocomplete.append(
        TxCompletion(
            tx_type="expense",
            main_category="food",
            sub_category="restaurant",
            regex=re.compile(r".*ROYAL PLAISANC.*"),
        )
    )
    cfg.category_set.add("food/restaurant")

    csv = cfg.root_dir / "2019-06.mhuang-BRS-CHQ.csv"
    csv.write_text(
        """\
Date,Label,Amount,Type,MainCategory,SubCategory
2019-06-26,CARTE 25/06/19 93 ROYAL PLAISANC CB*1234,-20.1,expense,food,restaurant
"""
    )
    actual_df = tx.read_transactions(csv, cfg)
    expected_df = pd.DataFrame(
        {
            "Date": pd.Timestamp("2019-06-26"),
            "Label": "CARTE 25/06/19 93 ROYAL PLAISANC CB*1234",
            "Amount": -20.1,
            "Type": "expense",
            "MainCategory": "food",
            "SubCategory": "restaurant",
        },
        index=[0],
    )
    assert_frame_equal(actual_df, expected_df)


@patch("builtins.print")
def test_read_boursorama_tx_validation_errors(mocked_print, cfg):
    """
    Ensure validation errors are handled correctly.
    """
    cfg.category_set.add("food/restaurant")

    csv = cfg.root_dir / "2019-06.mhuang-BRS-CHQ.csv"
    csv.write_text(
        """\
Date,Label,Amount,Type,MainCategory,SubCategory
2019-06-26,myLabel,-1.0,expense,food,restaurant
2019-06-26,myLabel,-2.0,expense,,
2019-06-26,myLabel,-3.0,expense,food,
2019-06-26,myLabel,-4.0,expense,,restaurant
"""
    )
    actual_df = tx.read_transactions(csv, cfg)
    expected_df = pd.DataFrame(
        {
            "Date": pd.Timestamp("2019-06-26"),
            "Label": "myLabel",
            "Amount": -1.0,
            "Type": "expense",
            "MainCategory": "food",
            "SubCategory": "restaurant",
        },
        index=[0],
    )
    assert_frame_equal(actual_df, expected_df)
    assert mocked_print.mock_calls == [
        call(f"{csv}:"),
        call("  - Line 3: Category 'nan/nan' does not exist."),
        call("  - Line 4: Category 'food/nan' does not exist."),
        call("  - Line 5: Category 'nan/restaurant' does not exist."),
    ]


def test_merge_bank_tx(cfg):
    df1 = pd.DataFrame(
        {
            "Date": pd.Timestamp("2019-06-26"),
            "ShortType": "aShortType",
            "LongType": "aLongType",
            "Label": "aLabel",
            "Amount": -20.1,
            "Type": "expense",
            "Category": "food",
            "SubCategory": "resto",
        },
        index=[0],
    )
    df2 = pd.DataFrame(
        {
            "Date": pd.Timestamp("2019-06-27"),
            "ShortType": "aShortType",
            "LongType": "aLongType",
            "Label": "aLabel",
            "Amount": -20.1,
            "Type": "expense",
            "Category": "food",
            "SubCategory": "resto",
        },
        index=[0],
    )

    actual_df = tx.merge_bank_tx([df1, df2], cfg)
    expected_df = pd.DataFrame(
        {
            "Date": [pd.Timestamp("2019-06-26"), pd.Timestamp("2019-06-27")],
            "ShortType": ["aShortType", "aShortType"],
            "LongType": ["aLongType", "aLongType"],
            "Label": ["aLabel", "aLabel"],
            "Amount": [-20.1, -20.1],
            "Type": ["expense", "expense"],
            "Category": ["food", "food"],
            "SubCategory": ["resto", "resto"],
        }
    )
    assert_frame_equal(actual_df, expected_df)


def test_merge_balances(cfg):
    cfg.accounts.extend(
        [
            BnpAccount("CHQ", "astark-BNP-CHQ", "123"),
            BnpAccount("CHQ", "astark-BRS-CHQ", "456"),
        ]
    )
    bnp = cfg.root_dir / "balance.astark-BNP-CHQ.csv"
    brs = cfg.root_dir / "balance.astark-BRS-CHQ.csv"
    bnp.write_text(
        """\
Date,Amount
2018-07-04,100.00
2019-07-04,100.00
"""
    )
    brs.write_text(
        """\
Date,Amount
2018-07-04,200.0
2019-07-04,200.0
"""
    )

    actual_df = tx.merge_balances([bnp, brs], cfg)
    cols = ["Date", "Account", "AccountId", "Amount", "AccountType"]
    data = [
        (pd.Timestamp("2018-07-04"), "astark-BNP-CHQ", "123", 100.0, "CHQ"),
        (pd.Timestamp("2018-07-04"), "astark-BRS-CHQ", "456", 200.0, "CHQ"),
        (pd.Timestamp("2019-07-04"), "astark-BNP-CHQ", "123", 100.0, "CHQ"),
        (pd.Timestamp("2019-07-04"), "astark-BRS-CHQ", "456", 200.0, "CHQ"),
    ]
    expected_df = pd.DataFrame(columns=cols, data=data)
    assert_frame_equal(actual_df, expected_df)


def test_rename_categories(cfg: Configuration):
    # Given
    cols = [
        "Date",
        "Account",
        "Label",
        "Amount",
        "Type",
        "MainCategory",
        "SubCategory",
    ]
    data = [
        (
            pd.Timestamp("2021-09-06"),
            "astark-BNP-CHQ",
            "MyLabel",
            100.0,
            "expense",
            "MainCategoryToRename",
            "SubCategoryToRename",
        ),
        (
            pd.Timestamp("2021-09-06"),
            "astark-BNP-CHQ",
            "MyLabel",
            100.0,
            "expense",
            "MainCategoryToKeep",
            "SubCategoryToKeep",
        ),
    ]
    origin_df = pd.DataFrame(columns=cols, data=data)

    cfg.categories_to_rename[
        "MainCategoryToRename/SubCategoryToRename"
    ] = "AnotherMainCategory/AnotherSubCategory"

    # When
    actual_df = tx.rename_categories(origin_df, cfg)

    # Then
    expected_data = [
        (
            pd.Timestamp("2021-09-06"),
            "astark-BNP-CHQ",
            "MyLabel",
            100.0,
            "expense",
            "AnotherMainCategory",
            "AnotherSubCategory",
        ),
        (
            pd.Timestamp("2021-09-06"),
            "astark-BNP-CHQ",
            "MyLabel",
            100.0,
            "expense",
            "MainCategoryToKeep",
            "SubCategoryToKeep",
        ),
    ]
    expected_df = pd.DataFrame(columns=cols, data=expected_data)
    assert_frame_equal(actual_df, expected_df)


def test_validate_tx(cfg):
    cfg.category_set.add("food/workfood")

    err1 = tx.validate_tx(
        pd.Series(
            {
                "Date": pd.Timestamp("2018-04-30"),
                "Label": "aLabel",
                "Amount": -4.95,
                "Type": "X",
                "MainCategory": "food",
                "SubCategory": "workfood",
            }
        ),
        cfg,
    )
    assert "Unknown transaction type: X" == err1

    err2 = tx.validate_tx(
        pd.Series(
            {
                "Date": pd.Timestamp("2018-04-30"),
                "Label": "aLabel",
                "Amount": -4.95,
                "Type": "expense",
                "MainCategory": "X",
                "SubCategory": "workfood",
            }
        ),
        cfg,
    )
    assert "Category 'X/workfood' does not exist." == err2

    err3 = tx.validate_tx(
        pd.Series(
            {
                "Date": pd.Timestamp("2018-04-30"),
                "Label": "aLabel",
                "Amount": -4.95,
                "Type": "expense",
                "MainCategory": "food",
                "SubCategory": "X",
            }
        ),
        cfg,
    )
    assert "Category 'food/X' does not exist." == err3

    err4 = tx.validate_tx(
        pd.Series(
            {
                "Date": pd.Timestamp("2018-04-30"),
                "Label": "aLabel",
                "Amount": -4.95,
                "Type": "expense",
                "MainCategory": "food",
                "SubCategory": "workfood",
            }
        ),
        cfg,
    )
    assert "" == err4


# ---------- Class: Configurator ----------


def test_configurator_load_categories_with_content():
    # language=yml
    cfg = yaml.safe_load(
        """\
categories:
  - a/1
  - a/2
  - b/1
  - b/2
"""
    )
    assert Configurator.load_categories(cfg["categories"]) == [
        "a/1",
        "a/2",
        "b/1",
        "b/2",
    ]


def test_configurator_load_categories_without_content():
    # language=yml
    cfg = yaml.safe_load(
        """\
categories:  # none
"""
    )
    assert Configurator.load_categories(cfg["categories"]) == []


def test_configurator_load_accounts_ok():
    # language=yml
    cfg = yaml.safe_load(
        """\
accounts:
  sstark-BNP-CHQ:
    company: BNP
    type: CHQ
    id: '****0001'
    currency: EUR
    label: Sansa Stark - BNP Paribas (Compte de Chèque)
  sstark-BNP-LVA:
    company: BNP
    type: LVA
    id: '****0002'
    currency: EUR
    label: Sansa Stark - BNP Paribas (Livret A)
  astark-BRS-CHQ:
    company: Boursorama
    type: CHQ
    id: '****0001'
    currency: EUR
    label: Arya Stark - Boursorama (Compte de Chèque)
  astark-DGR-STK:
    company: Degiro
    type: STK
    id: '****0002'
    currency: EUR
    label: Arya Stark - Degiro (Stock)
  astark-OCT-CWL:
    company: October
    type: CWL
    id: 'astark'
    currency: EUR
    label: Arya Stark - October (CrowdLending)
  astark-REV-EUR:
    company: Revolut
    type: CHQ
    id: 'astark1'
    currency: EUR
    label: Arya Stark - Revolut (Euro)
  astark-REV-USD:
    company: Revolut
    type: CHQ
    id: 'astark2'
    currency: USD
    label: Arya Stark - Revolut (US Dollar)
"""
    )
    # results are sorted by lexicographical order on symbolic name
    assert Configurator.load_accounts(cfg["accounts"]) == [
        BoursoramaAccount(
            account_type="CHQ", account_id="astark-BRS-CHQ", account_num="****0001"
        ),
        DegiroAccount(
            account_type="STK", account_id="astark-DGR-STK", account_num="****0002"
        ),
        OctoberAccount(
            account_type="CWL", account_id="astark-OCT-CWL", account_num="astark"
        ),
        RevolutAccount(
            account_type="CHQ",
            account_id="astark-REV-EUR",
            account_num="astark1",
            currency="EUR",
        ),
        RevolutAccount(
            account_type="CHQ",
            account_id="astark-REV-USD",
            account_num="astark2",
            currency="USD",
        ),
        BnpAccount(
            account_type="CHQ", account_id="sstark-BNP-CHQ", account_num="****0001"
        ),
        BnpAccount(
            account_type="LVA", account_id="sstark-BNP-LVA", account_num="****0002"
        ),
    ]


@patch("builtins.print")
def test_configurator_load_accounts_expr(mocked_print):
    # language=yml
    cfg = yaml.safe_load(
        """\
accounts:
  sstark-BNP-CHQ:
    company: BNP
    type: CHQ
    id: '****0001'
    label: BNP Paribas has its own naming convention, field 'expr' is not accepted.
    expr: 'patter1\\.csv'
  astark-BRS-CHQ:
    company: Boursorama
    type: CHQ
    id: '****0002'
    expr: 'patter2\\.csv'
    label: Boursorama has its own naming convention, field 'expr' is not accepted.
  astark-FTN-CHQ:
    company: Fortuneo
    type: CHQ
    id: '12345'
    expr: 'patterFortuneo\\.csv'
    label: Fortuneo has its own naming convention, field 'expr' is not accepted.
  astark-OCT-CWL:
    company: October
    type: CWL
    id: 'astark'
    expr: 'pattern3\\.csv'
    label: Arya Stark - October (CrowdLending)
"""
    )
    assert Configurator.load_accounts(cfg["accounts"]) == [
        BoursoramaAccount("CHQ", "astark-BRS-CHQ", "****0002"),
        FortuneoAccount("CHQ", "astark-FTN-CHQ", "12345"),
        OctoberAccount("CWL", "astark-OCT-CWL", "astark"),
        BnpAccount("CHQ", "sstark-BNP-CHQ", "****0001"),
    ]
    assert mocked_print.mock_calls == [
        call(
            "BNP Paribas has its own naming convention for downloaded files,"
            " you cannot overwrite it: expr='patter1\\\\.csv'"
        ),
        call(
            "Boursorama has its own naming convention for downloaded files,"
            " you cannot overwrite it: expr='patter2\\\\.csv'"
        ),
        call(
            "Fortuneo has its own naming convention for downloaded files,"
            " you cannot overwrite it: expr='patterFortuneo\\\\.csv'"
        ),
        call(
            "October has its own naming convention for downloaded files,"
            " you cannot overwrite it: expr='pattern3\\\\.csv'"
        ),
    ]


def test_configurator_load_accounts_unknown_account():
    # language=yml
    cfg = yaml.safe_load(
        """\
accounts:
  rstark-???-CHQ:
    company: unknown
    type: CHQ
    id: '****0001'
    currency: EUR
"""
    )
    assert Configurator.load_accounts(cfg["accounts"]) == [
        Account(
            account_type="CHQ",
            account_num="****0001",
            account_id="rstark-???-CHQ",
            patterns=["unknown"],
            currency="EUR",
        )
    ]


def test_configurator_autocomplete_with_content():
    # language=yml
    cfg = yaml.safe_load(
        """\
auto-complete:
  - expr: '.*FLUNCH.*'
    type: expense
    cat: food/restaurant
    desc: Optional description about this matching pattern. We go to Flunch regularly.
  - expr: '.*FOUJITA.*'
    type: expense
    cat: food/restaurant
    desc: Foujita is a nice Japanese restaurant near l'Opéra, we go there sometimes.
  - expr: '.*FRANPRIX 5584.*'
    type: expense
    cat: food/restaurant
"""
    )
    assert Configurator.load_autocomplete(cfg["auto-complete"]) == [
        TxCompletion(
            tx_type="expense",
            main_category="food",
            sub_category="restaurant",
            regex=re.compile(r".*FLUNCH.*"),
        ),
        TxCompletion(
            tx_type="expense",
            main_category="food",
            sub_category="restaurant",
            regex=re.compile(r".*FOUJITA.*"),
        ),
        TxCompletion(
            tx_type="expense",
            main_category="food",
            sub_category="restaurant",
            regex=re.compile(r".*FRANPRIX 5584.*"),
        ),
    ]


def test_configurator_autocomplete_without_content():
    # language=yml
    cfg = yaml.safe_load(
        """\
    auto-complete: # none
    """
    )
    assert Configurator.load_autocomplete(cfg["auto-complete"]) == []


def test_configurator_parse_yaml(sample):
    cfg = Configurator.parse_yaml(sample / "finance-tools.yml")
    assert cfg.accounts == [
        BnpAccount(
            account_type="CHQ",
            account_id="astark-BNP-CHQ",
            account_num="00000002",
        ),
        DegiroAccount(
            account_type="STK",
            account_id="astark-DGR-STK",
            account_num="00000003",
        ),
        FortuneoAccount(
            account_type="CHQ",
            account_id="astark-FTN-CHQ",
            account_num="12345",
        ),
        OctoberAccount(
            account_type="CWL",
            account_id="astark-OCT-CWL",
            account_num="astark",
        ),
        RevolutAccount(
            account_type="CHQ",
            account_id="astark-REV-EUR",
            account_num="astark1",
            currency="EUR",
        ),
        RevolutAccount(
            account_type="CHQ",
            account_id="astark-REV-USD",
            account_num="astark2",
            currency="USD",
            extra_patterns=[
                r"account-statement_(\d{4}-\d{2}-\d{2})_(\d{4}-\d{2}-\d{2})_en_(\w+)\.csv"
            ],
        ),
        BnpAccount(
            account_type="LVA",
            account_id="sstark-BNP-LVA",
            account_num="00000001",
        ),
    ]
