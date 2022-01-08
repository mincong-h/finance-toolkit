from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from finance_toolkit.accounts import (
    Account,
    BnpAccount,
    BoursoramaAccount,
    FortuneoAccount,
)
from finance_toolkit.pipelines import (
    BnpPipeline,
    BnpBalancePipeline,
    BnpTransactionPipeline,
    BoursoramaBalancePipeline,
    BoursoramaTransactionPipeline,
    FortuneoTransactionPipeline,
    GeneralBalancePipeline,
    NoopTransactionPipeline,
    PipelineFactory,
)
from finance_toolkit.utils import Summary


# ---------- Class: AccountPipeline ----------


def test_new_transaction_pipeline(cfg):
    p1 = PipelineFactory(cfg).new_transaction_pipeline(
        BnpAccount("CHQ", "foo-BNP-CHQ", "****0001")
    )
    p2 = PipelineFactory(cfg).new_transaction_pipeline(
        BoursoramaAccount("CHQ", "foo-BNP-CHQ", "****0001")
    )
    p3 = PipelineFactory(cfg).new_transaction_pipeline(
        FortuneoAccount("CHQ", "foo-FTN-CHQ", "12345")
    )
    p4 = PipelineFactory(cfg).new_transaction_pipeline(
        Account("unknown", "unknown", "unknown", "unknown")
    )

    assert isinstance(p1, BnpTransactionPipeline)
    assert isinstance(p2, BoursoramaTransactionPipeline)
    assert isinstance(p3, FortuneoTransactionPipeline)
    assert isinstance(p4, NoopTransactionPipeline)


def test_new_balance_pipeline(cfg):
    p1 = PipelineFactory(cfg).new_balance_pipeline(
        BnpAccount("CHQ", "foo-BNP-CHQ", "****0001")
    )
    p2 = PipelineFactory(cfg).new_balance_pipeline(
        BoursoramaAccount("CHQ", "foo-BNP-CHQ", "****0001")
    )
    p3 = PipelineFactory(cfg).new_balance_pipeline(
        Account("unknown", "unknown", "unknown", "unknown")
    )

    assert isinstance(p1, BnpBalancePipeline)
    assert isinstance(p2, BoursoramaBalancePipeline)
    assert isinstance(p3, GeneralBalancePipeline)


# ---------- Class: BnpPipeline ----------


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
Date,Label,Amount,Type,MainCategory,SubCategory
2018-08-30,myLabel,-0.49,expense,main,sub
2018-08-31,myLabel,-0.99,expense,,
"""
    )
    assert (
        tx09.read_text()
        == """\
Date,Label,Amount,Type,MainCategory,SubCategory
2018-09-01,myLabel,-1.49,expense,main,sub
2018-09-02,myLabel,-2.49,expense,,
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
        columns=["Date", "Amount"], data=[(pd.Timestamp("2018-09-02"), 924.37)]
    )
    BnpBalancePipeline.write_balances(csv, new_lines)

    # Then rows are available and sorted
    assert (
        csv.read_text()
        == """\
Date,Amount
2018-07-04,189.29
2018-08-02,724.37
2018-09-02,924.37
"""
    )


def test_bnp_pipeline_read_raw(cfg):
    # Given an existing CSV for BNP
    # When reading its content
    csv = cfg.download_dir / "E1851234.csv"
    actual_balances, actual_transactions = BnpPipeline.read_raw(csv)

    # Then the balances DataFrame is read correctly
    expected_balances = pd.DataFrame(
        columns=["Date", "Amount"], data=[(pd.Timestamp("2019-07-03"), -123456.78)]
    )
    assert_frame_equal(actual_balances, expected_balances)

    # And the transactions DataFrame is read correctly
    t_cols = [
        "Date",
        "Label",
        "Amount",
        "Type",
        "MainCategory",
        "SubCategory",
    ]
    t_data = [
        (
            pd.Timestamp("2019-06-05"),
            "AMORTISSEMENT PRET 1234",
            67.97,
            "",
            "",
            "",
        )
    ]
    expected_transactions = pd.DataFrame(columns=t_cols, data=t_data)
    assert_frame_equal(actual_transactions, expected_transactions)


@pytest.mark.parametrize(
    "cat, label, value",
    [
        # case 0: Crédit Immobilier (CDI)
        ("CDI", "credit", True),
        # case 1: Livret A (LVA)
        ("LVA", "transfer", True),
        # case 2: Livret Développement Durable (LDD)
        ("LDD", "transfer", True),
        # case 3: Compte de Chèque (CHQ)
        ("CHQ", "expense", ""),
    ],
)
def test_bnp_pipeline_guess_meta_account_type(cat, label, value, cfg):
    cols = ["Label", "Type", "mainCategory", "subCategory"]

    account = BnpAccount(cat, "xxx", "****1234")
    cfg.accounts.append(account)
    pipeline = BnpTransactionPipeline(account=account, cfg=cfg)
    raw = pd.DataFrame(columns=cols, data=[("Label", "", "", "", "")])
    expected = pd.DataFrame(columns=cols, data=[("Label", label, "", "", value)])
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
            ("FOUJITA", "expense", "food", "resto", True),
            ("FOUJITA LEETCODE", "expense", "food", "resto", True),
        ],
    )

    account = BnpAccount("CHQ", "xxx", "****1234")
    cfg.accounts.append(account)
    cfg.autocomplete.extend(
        [
            (("expense", "food", "resto", True), r".*FOUJITA.*"),
            (("expense", "util", "tech", False), r".*LEETCODE.*"),
        ]
    )
    actual = BnpTransactionPipeline(account, cfg).guess_meta(raw)
    assert_frame_equal(actual, expected)


def test_bnp_pipeline_append_tx_file_nonexistent_csv():
    df = pd.DataFrame(
        {
            "Date": [pd.Timestamp("2019-08-01")],
            "Label": ["myLabel"],
            "Amount": [10.0],
            "Type": [None],
            "MainCategory": [None],
            "SubCategory": [None],
        }
    )
    with TemporaryDirectory() as root:
        csv = Path(root) / "my.csv"
        BnpTransactionPipeline.append_transactions(csv, df)
        assert (
            csv.read_text()
            == """\
Date,Label,Amount,Type,MainCategory,SubCategory
2019-08-01,myLabel,10.0,,,
"""
        )


def test_bnp_pipeline_append_tx_file_existing_csv():
    df = pd.DataFrame(
        columns=[
            "Date",
            "Label",
            "Amount",
            "Type",
            "MainCategory",
            "SubCategory",
        ],
        data=[(pd.Timestamp("2019-08-01"), "myLabel", 10.0, "", "", "")],
    )
    with TemporaryDirectory() as root:
        csv = Path(root) / "my.csv"
        csv.write_text(
            """\
Date,Label,Amount,Type,MainCategory,SubCategory
2019-08-01,myLabel,10.0,myType,main,sub
"""
        )
        BnpTransactionPipeline.append_transactions(csv, df)
        assert (
            csv.read_text()
            == """\
Date,Label,Amount,Type,MainCategory,SubCategory
2019-08-01,myLabel,10.0,myType,main,sub
"""
        )


def test_bnp_pipeline_append_tx_file_drop_duplicates():
    """
    Mainly for third party payment websites, like PayPal.
    They don't provide distinguishable label.
    """
    df = pd.DataFrame(
        columns=[
            "Date",
            "Label",
            "Amount",
            "Type",
            "MainCategory",
            "SubCategory",
        ],
        data=[
            (pd.Timestamp("2019-08-01"), "myLabel", 10.0, "", "", ""),
            (pd.Timestamp("2019-08-01"), "myLabel", 11.0, "", "", ""),
        ],
    )
    with TemporaryDirectory() as root:
        csv = Path(root) / "my.csv"
        BnpTransactionPipeline.append_transactions(csv, df)
        assert (
            csv.read_text()
            == """\
Date,Label,Amount,Type,MainCategory,SubCategory
2019-08-01,myLabel,10.0,,,
2019-08-01,myLabel,11.0,,,
"""
        )


# ---------- Class: BoursoramaPipeline ----------


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
    b = cfg.root_dir / "balance.xxx.csv"
    b.write_text(
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
Date,Label,Amount,Type,MainCategory,SubCategory
2019-08-29,VIR Virement interne depuis BOURSORA,30.0,transfer,,
2019-08-30,VIR Virement interne depuis BOURSORA,10.0,transfer,,
"""
    )
    assert (
        tx09.read_text()
        == """\
Date,Label,Amount,Type,MainCategory,SubCategory
2019-09-01,VIR Virement interne depuis BOURSORA,40.0,transfer,,
2019-09-02,VIR Virement interne depuis BOURSORA,11.0,transfer,,
"""
    )

    # And the balance is correct
    BoursoramaBalancePipeline(account, cfg).run(new_file, summary)
    assert (
        b.read_text()
        == """\
Date,Amount
2019-08-29,300.0
2019-09-01,200.0
2019-09-03,1000.0
"""
    )

    # And the summary is correct
    assert new_file in summary.sources
    assert tx08 in summary.targets
    assert tx09 in summary.targets
    assert b in summary.targets


def test_boursorama_account_read_raw(cfg):
    csv = cfg.download_dir / "export-operations-30-03-2019_08-50-51.csv"

    account = BoursoramaAccount("type1", "name1", "001234")
    cfg.accounts.append(account)
    b, tx = BoursoramaTransactionPipeline(account, cfg).read_raw(csv)

    expected_b = pd.DataFrame(
        {"accountNum": "001234", "Amount": 370.0, "Date": pd.Timestamp("2019-03-29")},
        index=[0],
    )
    assert_frame_equal(expected_b, b)
    expected_tx = pd.DataFrame(
        columns=[
            "Date",
            "Label",
            "Amount",
            "accountNum",
            "accountLabel",
            "accountBalance",
        ],
        data=[
            (
                pd.Timestamp("2019-03-12"),
                "Prime Parrainage",
                80.0,
                "001234",
                "BOURSORAMA BANQUE",
                370.0,
            ),
            (
                pd.Timestamp("2019-03-12"),
                "VIR VIREMENT CREATION COMPTE",
                300.0,
                "001234",
                "BOURSORAMA BANQUE",
                370.0,
            ),
            (
                pd.Timestamp("2019-03-12"),
                "VIR VIREMENT CREATION COMPTE",
                -10.0,
                "001234",
                "BOURSORAMA BANQUE",
                370.0,
            ),
        ],
    )
    assert_frame_equal(expected_tx, tx)


def test_boursorama_account_read_raw_account_2(cfg):
    csv = cfg.download_dir / "export-operations-30-03-2019_08-50-51.csv"

    account = BoursoramaAccount("type2", "name2", "003607")
    cfg.accounts.append(account)
    b, tx = BoursoramaTransactionPipeline(account, cfg).read_raw(csv)

    expected_b = pd.DataFrame(
        {
            "accountNum": "003607",
            "Amount": 4810.0,
            "Date": pd.Timestamp("2019-03-29"),  # date from filename, not row
        },
        index=[0],
    )
    assert_frame_equal(expected_b, b)
    expected_tx = pd.DataFrame(
        {
            "Date": pd.Timestamp("2019-03-12"),
            "Label": "VIR VIREMENT CREATION COMPTE",
            "Amount": 10.0,
            "accountNum": "003607",
            "accountLabel": "COMPTE SUR LIVRET",
            "accountBalance": 4810.0,
        },
        index=[0],
    )
    assert_frame_equal(expected_tx, tx)


def test_boursorama_account_write_balances():
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
            columns=["Date", "Amount"], data=[(pd.Timestamp("2019-03-10"), 320.00)]
        )
        BoursoramaBalancePipeline.write_balances(csv, new_lines)

        # Then rows are available and sorted
        assert (
            csv.read_text()
            == """\
Date,Amount
2019-03-01,300.0
2019-03-10,320.0
2019-03-12,370.0
"""
        )


def test_boursorama_pipeline_append_tx():
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
        cols = [
            "Date",
            "Label",
            "Amount",
            "Type",
            "MainCategory",
            "SubCategory",
        ]
        data = [
            (
                pd.Timestamp("2018-09-27"),
                "L",
                -10.0,
                "expense",
                "M",
                "S",
            )
        ]
        new_lines = pd.DataFrame(columns=cols, data=data)
        BoursoramaTransactionPipeline.append_transactions(csv, new_lines)

        # Then rows are available and sorted
        assert (
            csv.read_text()
            == """\
Date,Label,Amount,Type,MainCategory,SubCategory
2018-09-26,CARTE 25/09/18 93 LABEL,-20.1,expense,food,resto
2018-09-27,L,-10.0,expense,M,S
"""
        )


def test_boursorama_pipeline_append_tx_drop_duplicates():
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
        cols = [
            "Date",
            "Label",
            "Amount",
            "Type",
            "MainCategory",
            "SubCategory",
        ]
        data = [
            (
                pd.Timestamp("2018-09-26"),
                "myLabel",
                -20.1,
                "expense",
                "food",
                "resto",
            )
        ]
        new_lines = pd.DataFrame(columns=cols, data=data)
        BoursoramaTransactionPipeline.append_transactions(csv, new_lines)

        # Then rows has no duplicates
        assert (
            csv.read_text()
            == """\
Date,Label,Amount,Type,MainCategory,SubCategory
2018-09-26,myLabel,-20.1,expense,food,resto
"""
        )


@pytest.mark.parametrize(
    "cat, label",
    [
        # case 0: Livret (LVR)
        ("LVR", "transfer"),
        # case 1: Compte de Chèque (CHQ)
        ("CHQ", "expense"),
    ],
)
def test_boursorama_pipeline_guess_meta_account_type(cat, label, cfg):
    cols = ["Label", "Type", "mainCategory", "subCategory"]

    account = BoursoramaAccount(cat, "xxx", "****1234")
    cfg.accounts.append(account)
    raw_df = pd.DataFrame(columns=cols, data=[("Label", "", "", "", "")])
    expected_df = pd.DataFrame(columns=cols, data=[("Label", label, "", "", False)])
    actual_df = BoursoramaTransactionPipeline(account, cfg).guess_meta(raw_df)
    assert_frame_equal(actual_df, expected_df)


def test_boursorama_account_guess_mata_transaction_label(cfg):
    cols = ["Label", "Type", "MainCategory", "SubCategory"]

    account = BoursoramaAccount("LVR", "xxx", "****1234")
    cfg.accounts.append(account)
    cfg.autocomplete.extend(
        [
            (("expense", "food", "resto", True), r".*FOUJITA.*"),
            (("expense", "util", "tech", False), r".*LEETCODE.*"),
        ]
    )
    raw = pd.DataFrame(
        columns=cols,
        data=[
            ("FOUJITA", "", "", "", ""),  # find
            ("FOUJITA LEETCODE", "", "", "", ""),  # find first
        ],
    )
    expected = pd.DataFrame(
        columns=cols,
        data=[
            ("FOUJITA", "expense", "food", "resto", True),
            ("FOUJITA LEETCODE", "expense", "food", "resto", True),
        ],
    )
    actual = BoursoramaTransactionPipeline(account, cfg).guess_meta(raw)
    assert_frame_equal(actual, expected)
