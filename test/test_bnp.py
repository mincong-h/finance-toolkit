import re
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from finance_toolkit.bnp import (
    BnpAccount,
    BnpBalancePipeline,
    BnpTransactionPipeline,
)
from finance_toolkit.models import Summary, TxType
from finance_toolkit.tx import TxCompletion


def test_bnp_pipeline_integrate(cfg):
    (cfg.root_dir / "2018-08").mkdir()
    (cfg.root_dir / "2018-09").mkdir()

    # Given two existing CSVs for transactions
    tx08 = cfg.root_dir / "2018-08" / "2018-08.xxx.csv"
    tx09 = cfg.root_dir / "2018-09" / "2018-09.xxx.csv"
    tx08.write_text(
        """\
Date,Label,Amount,Type,MainCategory,SubCategory
2018-08-30,myLabel,-0.49,expense,main,sub
"""
    )
    tx09.write_text(
        """\
Date,Label,Amount,Type,MainCategory,SubCategory
2018-09-01,myLabel,-1.49,expense,main,sub
"""
    )

    # And a file for balance
    b = cfg.root_dir / "balance.xxx.csv"
    with b.open("w") as f:
        f.write("mainCategory,subCategory,accountNum,Date,Amount\n")
        f.write("main,sub,****1234,2018-08-02,24.37\n")

    # And a file to be integrated
    new_file = cfg.root_dir / "E0001234.csv"
    with new_file.open("w") as f:
        f.write("this;is;balance;03/09/2018;line;1 234,56\n")
        f.write("31/08/2018;M;S;myLabel;-0,99\n")
        f.write("02/09/2018;M;S;myLabel;-2,49\n")

    # When integrating new lines
    summary = Summary(cfg)
    account = BnpAccount("CHQ", "xxx", "****1234")
    cfg.accounts.append(account)
    BnpTransactionPipeline(account, cfg).run(new_file, summary)
    BnpBalancePipeline(account, cfg).run(new_file, summary)

    # Then the new lines are integrated
    assert (
        tx08.read_text()
        == """\
Date,Label,Amount,Currency,Type,MainCategory,SubCategory
2018-08-30,myLabel,-0.49,EUR,expense,main,sub
2018-08-31,myLabel,-0.99,EUR,expense,,
"""
    )
    assert (
        tx09.read_text()
        == """\
Date,Label,Amount,Currency,Type,MainCategory,SubCategory
2018-09-01,myLabel,-1.49,EUR,expense,main,sub
2018-09-02,myLabel,-2.49,EUR,expense,,
"""
    )

    # And the summary is correct
    assert new_file in summary.sources
    assert tx08 in summary.targets
    assert tx09 in summary.targets
    assert b in summary.targets


def test_bnp_balance_pipeline_write_balances(cfg):
    # Given an existing CSV file with 2 rows
    csv = cfg.root_dir / "balance.xxx.csv"
    csv.write_text(
        """\
Date,Amount
2018-08-02,724.37
2018-07-04,189.29
"""
    )

    # When writing new row into the CSV file
    new_lines = pd.DataFrame(
        {
            "Date": [pd.Timestamp("2018-09-02")],
            "Amount": [924.37],
            "Currency": ["EUR"],
        }
    )
    account = BnpAccount("CHQ", "xxx", "****1234")
    BnpBalancePipeline(account, cfg).write_balances(csv, new_lines)

    # Then rows are available and sorted
    assert (
        csv.read_text()
        == """\
Date,Amount,Currency
2018-07-04,189.29,EUR
2018-08-02,724.37,EUR
2018-09-02,924.37,EUR
"""
    )


def test_bnp_pipeline_read_raw_20190703(cfg):
    # Given an existing CSV for BNP
    # When reading its content
    csv = cfg.download_dir / "E1851234.csv"
    account = BnpAccount("CHQ", "xxx", "****1234")
    actual_balances, actual_transactions = BnpTransactionPipeline(
        account, cfg
    ).read_raw(csv)

    # Then the balances DataFrame is read correctly
    expected_balances = pd.DataFrame(
        columns=["Date", "Amount", "Currency"],
        data=[(pd.Timestamp("2019-07-03"), -123456.78, "EUR")],
    )
    assert_frame_equal(actual_balances, expected_balances)

    # And the transactions DataFrame is read correctly
    t_cols = [
        "Date",
        "Label",
        "Amount",
        "Currency",
        "Type",
        "MainCategory",
        "SubCategory",
    ]
    t_data = [
        (
            pd.Timestamp("2019-06-05"),
            "AMORTISSEMENT PRET 1234",
            67.97,
            "EUR",
            "",
            "",
            "",
        )
    ]
    expected_transactions = pd.DataFrame(columns=t_cols, data=t_data)
    assert_frame_equal(actual_transactions, expected_transactions)


def test_bnp_pipeline_read_raw_20220318(cfg):
    # Given an existing CSV for BNP
    # When reading its content
    csv = cfg.download_dir / "E0790170.csv"
    account = BnpAccount("CHQ", "xxx", "****1234")
    actual_balances, actual_transactions = BnpTransactionPipeline(
        account, cfg
    ).read_raw(csv)

    # Then the balances DataFrame is read correctly
    expected_balances = pd.DataFrame(
        columns=["Date", "Amount", "Currency"],
        data=[(pd.Timestamp("2022-03-18"), -123456.78, "EUR")],
    )
    assert_frame_equal(actual_balances, expected_balances)

    # And the transactions DataFrame is read correctly
    t_cols = [
        "Date",
        "Label",
        "Amount",
        "Currency",
        "Type",
        "MainCategory",
        "SubCategory",
    ]
    t_data = [
        (
            pd.Timestamp("2022-01-05"),
            "AMORTISSEMENT PRET 1234",
            70.93,
            "EUR",
            "",
            "",
            "",
        ),
        (
            pd.Timestamp("2022-02-05"),
            "AMORTISSEMENT PRET 1234",
            71.03,
            "EUR",
            "",
            "",
            "",
        ),
        (
            pd.Timestamp("2022-03-05"),
            "AMORTISSEMENT PRET 1234",
            71.13,
            "EUR",
            "",
            "",
            "",
        ),
    ]
    expected_transactions = pd.DataFrame(columns=t_cols, data=t_data)
    assert_frame_equal(actual_transactions, expected_transactions)


@pytest.mark.parametrize(
    "cat, tx_type",
    [
        # case 0: Crédit Immobilier (CDI)
        ("CDI", TxType.CREDIT),
        # case 1: Livret A (LVA)
        ("LVA", TxType.TRANSFER),
        # case 2: Livret Développement Durable (LDD)
        ("LDD", TxType.TRANSFER),
        # case 3: Compte de Chèque (CHQ)
        ("CHQ", TxType.EXPENSE),
    ],
)
def test_bnp_pipeline_guess_meta_account_type(cat, tx_type, cfg):
    cols = ["Label", "Type", "mainCategory", "subCategory"]

    account = BnpAccount(cat, "xxx", "****1234")
    cfg.accounts.append(account)
    pipeline = BnpTransactionPipeline(account=account, cfg=cfg)
    raw = pd.DataFrame(columns=cols, data=[("Label", "", "", "")])
    expected = pd.DataFrame(columns=cols, data=[("Label", tx_type.value, "", "")])
    actual = pipeline.guess_meta(raw)
    assert_frame_equal(actual, expected)


def test_bnp_pipeline_guess_meta_transaction_label(cfg):
    cols = ["Label", "Type", "MainCategory", "SubCategory"]
    raw = pd.DataFrame(
        columns=cols,
        data=[
            ("FOUJITA", "", "", ""),  # find
            ("FOUJITA LEETCODE", "", "", ""),  # find first
        ],
    )
    expected = pd.DataFrame(
        columns=cols,
        data=[
            ("FOUJITA", "expense", "food", "resto"),
            ("FOUJITA LEETCODE", "expense", "food", "resto"),
        ],
    )

    account = BnpAccount("CHQ", "xxx", "****1234")
    cfg.accounts.append(account)
    cfg.autocomplete.extend(
        [
            TxCompletion(
                tx_type="expense",
                main_category="food",
                sub_category="resto",
                regex=re.compile(r".*FOUJITA.*"),
            ),
            TxCompletion(
                tx_type="expense",
                main_category="util",
                sub_category="tech",
                regex=re.compile(r".*LEETCODE.*"),
            ),
        ]
    )
    actual = BnpTransactionPipeline(account, cfg).guess_meta(raw)
    assert_frame_equal(actual, expected)


def test_bnp_pipeline_append_tx_file_nonexistent_csv(cfg):
    new_transactions = pd.DataFrame(
        {
            "Date": [pd.Timestamp("2019-08-01")],
            "Label": ["myLabel"],
            "Amount": [10.0],
            "Currency": ["EUR"],
            "Type": [None],
            "MainCategory": [None],
            "SubCategory": [None],
        }
    )
    with TemporaryDirectory() as root:
        csv = Path(root) / "my.csv"
        account = BnpAccount("CHQ", "xxx", "****1234")
        BnpTransactionPipeline(account, cfg).append_transactions(csv, new_transactions)
        assert (
            csv.read_text()
            == """\
Date,Label,Amount,Currency,Type,MainCategory,SubCategory
2019-08-01,myLabel,10.0,EUR,,,
"""
        )


def test_bnp_pipeline_append_tx_file_existing_csv(cfg):
    new_transactions = pd.DataFrame(
        columns=[
            "Date",
            "Label",
            "Amount",
            "Currency",
            "Type",
            "MainCategory",
            "SubCategory",
        ],
        data=[(pd.Timestamp("2019-08-01"), "myLabel", 10.0, "EUR", "", "", "")],
    )
    with TemporaryDirectory() as root:
        csv = Path(root) / "my.csv"
        csv.write_text(
            """\
Date,Label,Amount,Type,MainCategory,SubCategory
2019-08-01,myLabel,10.0,myType,main,sub
"""
        )
        account = BnpAccount("CHQ", "xxx", "****1234")
        BnpTransactionPipeline(account, cfg).append_transactions(csv, new_transactions)
        assert (
            csv.read_text()
            == """\
Date,Label,Amount,Currency,Type,MainCategory,SubCategory
2019-08-01,myLabel,10.0,EUR,myType,main,sub
"""
        )


def test_bnp_pipeline_append_tx_file_drop_duplicates(cfg):
    """
    Mainly for third party payment websites, like PayPal.
    They don't provide distinguishable label.
    """
    df = pd.DataFrame(
        columns=[
            "Date",
            "Label",
            "Amount",
            "Currency",
            "Type",
            "MainCategory",
            "SubCategory",
        ],
        data=[
            (pd.Timestamp("2019-08-01"), "myLabel", 10.0, "EUR", "", "", ""),
            (pd.Timestamp("2019-08-01"), "myLabel", 11.0, "EUR", "", "", ""),
        ],
    )
    with TemporaryDirectory() as root:
        csv = Path(root) / "my.csv"
        account = BnpAccount("CHQ", "xxx", "****1234")
        BnpTransactionPipeline(account, cfg).append_transactions(csv, df)
        assert (
            csv.read_text()
            == """\
Date,Label,Amount,Currency,Type,MainCategory,SubCategory
2019-08-01,myLabel,10.0,EUR,,,
2019-08-01,myLabel,11.0,EUR,,,
"""
        )
