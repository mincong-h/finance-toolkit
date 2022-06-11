import re
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from finance_toolkit.boursorama import (
    BoursoramaAccount,
    BoursoramaBalancePipeline,
    BoursoramaTransactionPipeline,
)
from finance_toolkit.models import Summary, TxType
from finance_toolkit.pipeline import PipelineDataError
from finance_toolkit.tx import TxCompletion


def test_boursorama_pipeline_integrate(cfg):
    (cfg.root_dir / "2019-08").mkdir()
    (cfg.root_dir / "2019-09").mkdir()

    # Given two existing CSVs for transactions
    tx08 = cfg.root_dir / "2019-08" / "2019-08.xxx.csv"
    tx09 = cfg.root_dir / "2019-09" / "2019-09.xxx.csv"
    tx08.write_text(
        """\
Date,Label,Amount,Type,MainCategory,SubCategory
2019-08-29,VIR Virement interne depuis BOURSORA,30.0,transfer,,
"""
    )
    tx09.write_text(
        """\
Date,Label,Amount,Type,MainCategory,SubCategory
2019-09-01,VIR Virement interne depuis BOURSORA,40.0,transfer,,
"""
    )

    # And a file for balance
    balance_file = cfg.root_dir / "balance.xxx.csv"
    balance_file.write_text(
        """\
Date,Amount
2019-08-29,300.0
2019-09-01,200.0
"""
    )

    # And a file to be integrated
    new_file = cfg.root_dir / "export-operations-04-09-2019_23-17-18.csv"
    new_file.write_text(
        """\
dateOp;dateVal;Label;category;categoryParent;Amount;accountNum;accountLabel;accountbalance
2019-08-30;2019-08-30;"VIR Virement interne depuis BOURSORA";"Virements reçus de comptes à comptes";"Mouvements internes créditeurs";10,00;00001234;"COMPTE SUR LIVRET";"1 000,00"
2019-09-02;2019-09-02;"VIR Virement interne depuis BOURSORA";"Virements reçus de comptes à comptes";"Mouvements internes créditeurs";11,00;00001234;"COMPTE SUR LIVRET";"1 000,00"
""",  # noqa: E501
        encoding="ISO-8859-1",
    )

    # When integrating new lines
    summary = Summary(cfg)
    account = BoursoramaAccount("LVR", "xxx", "001234")
    cfg.accounts.append(account)
    BoursoramaTransactionPipeline(account, cfg).run(new_file, summary)

    # Then the new lines are integrated
    assert (
        tx08.read_text()
        == """\
Date,Label,Amount,Currency,Type,MainCategory,SubCategory
2019-08-29,VIR Virement interne depuis BOURSORA,30.0,EUR,transfer,,
2019-08-30,VIR Virement interne depuis BOURSORA,10.0,EUR,transfer,,
"""
    )
    assert (
        tx09.read_text()
        == """\
Date,Label,Amount,Currency,Type,MainCategory,SubCategory
2019-09-01,VIR Virement interne depuis BOURSORA,40.0,EUR,transfer,,
2019-09-02,VIR Virement interne depuis BOURSORA,11.0,EUR,transfer,,
"""
    )

    # And the balance is correct
    BoursoramaBalancePipeline(account, cfg).run(new_file, summary)
    assert (
        balance_file.read_text()
        == """\
Date,Amount,Currency
2019-08-29,300.0,EUR
2019-09-01,200.0,EUR
2019-09-03,1000.0,EUR
"""
    )

    # And the summary is correct
    assert new_file in summary.sources
    assert tx08 in summary.targets
    assert tx09 in summary.targets
    assert balance_file in summary.targets


@patch("pandas.read_csv")
def test_pipeline_read_raw(mocked_read_csv, cfg):
    mocked_read_csv.side_effect = ValueError("oops")
    csv = cfg.root_dir / "export-operations-11-06-2022_18-00-00.csv"
    csv.write_text(
        """\
dateOp;dateVal;label;category;categoryParent;amount;comment;accountNum;accountLabel;accountbalance
2021-08-17;2021-08-17;"Prime Parrainage";"Virements reçus";"Virements reçus";130,00;;00040677485;"BOURSORAMA BANQUE";226.68
"""  # noqa: E501
    )

    account = BoursoramaAccount("type1", "name1", "001234")
    cfg.accounts.append(account)
    captured_error = None

    try:
        BoursoramaTransactionPipeline(account, cfg).read_raw(csv)
    except PipelineDataError as e:
        captured_error = e

    assert (
        str(captured_error)
        == f"""\
Failed to read new Boursorama data. Details:
  path={csv}
  headers=dateOp;dateVal;label;category;categoryParent;amount;comment;accountNum;accountLabel;accountbalance
  pandas_kwargs={{'decimal': ',', 'delimiter': ';', 'dtype': {{'accountNum': 'str'}}, 'encoding': 'ISO-8859-1', 'parse_dates': ['dateOp', 'dateVal'], 'skipinitialspace': True, 'thousands': ' '}}
  pandas_error=oops"""  # noqa: E501
    )


def test_boursorama_account_read_raw(cfg):
    csv = cfg.download_dir / "export-operations-30-03-2019_08-50-51.csv"

    account = BoursoramaAccount("type1", "name1", "001234")
    cfg.accounts.append(account)
    actual_balances, actual_transactions = BoursoramaTransactionPipeline(
        account, cfg
    ).read_raw(csv)

    expected_balances = pd.DataFrame(
        columns=["accountNum", "Date", "Amount", "Currency"],
        data=[("001234", pd.Timestamp("2019-03-29"), 370.0, "EUR")],
    )
    assert_frame_equal(expected_balances, actual_balances)
    expected_transactions = pd.DataFrame(
        columns=[
            "Date",
            "Label",
            "Amount",
            "accountNum",
            "accountLabel",
            "accountBalance",
            "Currency",
        ],
        data=[
            (
                pd.Timestamp("2019-03-12"),
                "Prime Parrainage",
                80.0,
                "001234",
                "BOURSORAMA BANQUE",
                370.0,
                "EUR",
            ),
            (
                pd.Timestamp("2019-03-12"),
                "VIR VIREMENT CREATION COMPTE",
                300.0,
                "001234",
                "BOURSORAMA BANQUE",
                370.0,
                "EUR",
            ),
            (
                pd.Timestamp("2019-03-12"),
                "VIR VIREMENT CREATION COMPTE",
                -10.0,
                "001234",
                "BOURSORAMA BANQUE",
                370.0,
                "EUR",
            ),
        ],
    )
    assert_frame_equal(expected_transactions, actual_transactions)


def test_boursorama_account_read_raw_account_2(cfg):
    csv = cfg.download_dir / "export-operations-30-03-2019_08-50-51.csv"

    account = BoursoramaAccount("type2", "name2", "003607")
    cfg.accounts.append(account)
    actual_balances, actual_transactions = BoursoramaTransactionPipeline(
        account, cfg
    ).read_raw(csv)

    expected_balances = pd.DataFrame(
        columns=[
            "accountNum",
            "Date",
            "Amount",
            "Currency",
        ],
        data=[
            (
                "003607",
                pd.Timestamp("2019-03-29"),  # date from filename, not row
                4810.0,
                "EUR",
            ),
        ],
    )
    assert_frame_equal(expected_balances, actual_balances)
    expected_transactions = pd.DataFrame(
        {
            "Date": pd.Timestamp("2019-03-12"),
            "Label": "VIR VIREMENT CREATION COMPTE",
            "Amount": 10.0,
            "accountNum": "003607",
            "accountLabel": "COMPTE SUR LIVRET",
            "accountBalance": 4810.0,
            "Currency": "EUR",
        },
        index=[0],
    )
    assert_frame_equal(expected_transactions, actual_transactions)


def test_boursorama_account_write_balances(cfg):
    with TemporaryDirectory() as d:
        # Given an existing CSV file with 2 rows
        csv = Path(d) / "balance.xxx.csv"
        csv.write_text(
            """\
Date,Amount
2019-03-01,300.00
2019-03-12,370.00
"""
        )

        # When writing new row into the CSV file
        new_lines = pd.DataFrame(
            columns=["Date", "Amount", "Currency"],
            data=[(pd.Timestamp("2019-03-10"), 320.00, "EUR")],
        )
        account = BoursoramaAccount("type2", "name2", "003607")
        BoursoramaBalancePipeline(account, cfg).write_balances(csv, new_lines)

        # Then rows are available and sorted
        assert (
            csv.read_text()
            == """\
Date,Amount,Currency
2019-03-01,300.0,EUR
2019-03-10,320.0,EUR
2019-03-12,370.0,EUR
"""
        )


def test_boursorama_pipeline_append_tx(cfg):
    with TemporaryDirectory() as d:
        # Given an existing CSV
        csv = Path(d) / "2018-09.xxx.csv"
        csv.write_text(
            """\
Date,Label,Amount,Type,MainCategory,SubCategory
2018-09-26,CARTE 25/09/18 93 LABEL,-20.1,expense,food,resto
"""
        )

        # When writing new row into the CSV file
        new_lines = pd.DataFrame(
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
                (
                    pd.Timestamp("2018-09-27"),
                    "L",
                    -10.0,
                    "EUR",
                    "expense",
                    "M",
                    "S",
                ),
            ],
        )
        account = BoursoramaAccount("type2", "name2", "003607")
        BoursoramaTransactionPipeline(account, cfg).append_transactions(csv, new_lines)

        # Then rows are available and sorted
        assert (
            csv.read_text()
            == """\
Date,Label,Amount,Currency,Type,MainCategory,SubCategory
2018-09-26,CARTE 25/09/18 93 LABEL,-20.1,EUR,expense,food,resto
2018-09-27,L,-10.0,EUR,expense,M,S
"""
        )


def test_boursorama_pipeline_append_tx_drop_duplicates(cfg):
    with TemporaryDirectory() as d:
        # Given an existing CSV
        csv = Path(d) / "my.csv"
        csv.write_text(
            """\
Date,Label,Amount,Type,MainCategory,SubCategory
2018-09-26,myLabel,-20.1,expense,food,resto
"""
        )

        # When writing new row into the CSV file
        new_lines = pd.DataFrame(
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
                (
                    pd.Timestamp("2018-09-26"),
                    "myLabel",
                    -20.1,
                    "EUR",
                    "expense",
                    "food",
                    "resto",
                )
            ],
        )
        account = BoursoramaAccount("type2", "name2", "003607")
        BoursoramaTransactionPipeline(account, cfg).append_transactions(csv, new_lines)

        # Then rows has no duplicates
        assert (
            csv.read_text()
            == """\
Date,Label,Amount,Currency,Type,MainCategory,SubCategory
2018-09-26,myLabel,-20.1,EUR,expense,food,resto
"""
        )


@pytest.mark.parametrize(
    "cat, tx_type",
    [
        # case 0: Livret (LVR)
        ("LVR", "transfer"),
        # case 1: Compte de Chèque (CHQ)
        ("CHQ", "expense"),
    ],
)
def test_boursorama_pipeline_guess_meta_account_type(cat, tx_type, cfg):
    account = BoursoramaAccount(cat, "xxx", "****1234")
    cfg.accounts.append(account)
    raw_df = pd.DataFrame(
        {
            "Label": ["Label"],
            "Type": [""],
            "mainCategory": [""],
            "subCategory": [""],
        }
    )
    expected_df = pd.DataFrame(
        {
            "Label": ["Label"],
            "Type": [tx_type],
            "mainCategory": [""],
            "subCategory": [""],
        }
    )
    actual_df = BoursoramaTransactionPipeline(account, cfg).guess_meta(raw_df)
    assert_frame_equal(actual_df, expected_df)


def test_boursorama_account_guess_mata_transaction_label(cfg):
    cols = ["Label", "Type", "MainCategory", "SubCategory"]

    account = BoursoramaAccount("LVR", "xxx", "****1234")
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
    actual = BoursoramaTransactionPipeline(account, cfg).guess_meta(raw)
    assert_frame_equal(actual, expected)


def test_boursorama_account_guess_mata_transaction_label_for_tax(cfg):
    account = BoursoramaAccount("LVR", "xxx", "****1234")
    cfg.accounts.append(account)
    cfg.autocomplete.extend(
        [
            TxCompletion(
                tx_type="tax",
                main_category="tax",
                sub_category="residence-tax",
                regex=re.compile(".*IMPOT TH.*"),
            ),
            TxCompletion(
                tx_type="tax",
                main_category="tax",
                sub_category="property-tax",
                regex=re.compile(".*IMPOT TF.*"),
            ),
            TxCompletion(
                tx_type="tax",
                main_category="tax",
                sub_category="income-tax",
                regex=re.compile(".*IMPOT REVENUS.*"),
            ),
            TxCompletion(
                tx_type="tax",
                main_category="tax",
                sub_category="social-charges",
                regex=re.compile(".*PRELEVEMENT SOCIAUX.*"),
            ),
        ]
    )
    raw = pd.DataFrame(
        {
            "Label": [
                "PRLV SEPA D.G.F.I.P. IMPOT x ECH/x ID EMETTEUR/x MDT/x REF/x LIB/x x                      x  IMPOT TH",  # noqa
                "PRLV SEPA D.G.F.I.P. IMPOT x ECH/x ID EMETTEUR/x MDT/x REF/x LIB/x x                      x  IMPOT TF",  # noqa
                "PRLV SEPA DGFIP IMPOT x ECH/x ID EMETTEUR/x MDT/x REF/x x 01 LIB/SOLDE IMPOT REVENUS 2020 N DE FACTURE x",  # noqa
                "PRELEVEMENT SOCIAUX/FISCAUX",
            ],
            "Type": [""] * 4,
            "MainCategory": [""] * 4,
            "SubCategory": [""] * 4,
        }
    )
    expected = pd.DataFrame(
        {
            "Label": [
                "PRLV SEPA D.G.F.I.P. IMPOT x ECH/x ID EMETTEUR/x MDT/x REF/x LIB/x x                      x  IMPOT TH",  # noqa
                "PRLV SEPA D.G.F.I.P. IMPOT x ECH/x ID EMETTEUR/x MDT/x REF/x LIB/x x                      x  IMPOT TF",  # noqa
                "PRLV SEPA DGFIP IMPOT x ECH/x ID EMETTEUR/x MDT/x REF/x x 01 LIB/SOLDE IMPOT REVENUS 2020 N DE FACTURE x",  # noqa
                "PRELEVEMENT SOCIAUX/FISCAUX",
            ],
            "Type": [TxType.TAX.value] * 4,
            "MainCategory": ["tax"] * 4,
            "SubCategory": [
                "residence-tax",
                "property-tax",
                "income-tax",
                "social-charges",
            ],
        }
    )
    actual = BoursoramaTransactionPipeline(account, cfg).guess_meta(raw)
    assert_frame_equal(actual, expected)
