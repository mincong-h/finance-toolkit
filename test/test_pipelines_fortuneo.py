from pathlib import Path

import pandas as pd
from pandas.testing import assert_frame_equal

from finance_toolkit.accounts import FortuneoAccount
from finance_toolkit.pipelines import FortuneoTransactionPipeline
from finance_toolkit.models import Summary, TxCompletion


def test_fortuneo_transaction_pipeline_read_new_transactions(cfg):
    csv = (
        cfg.download_dir / "HistoriqueOperations_12345_du_14_01_2019_au_14_12_2019.csv"
    )

    account = FortuneoAccount("aType", "anId", "12345")
    cfg.accounts.append(account)
    actual = FortuneoTransactionPipeline(account, cfg).read_new_transactions(csv)

    data = [
        (
            pd.Timestamp("2019-12-13"),
            "CARTE 12/12 FNAC METZ",
            -6.4,
            "",
            "",
            "",
        ),
        (
            pd.Timestamp("2019-12-13"),
            "CARTE 12/12 BRIOCHE DOREE METZ",
            -10.9,
            "",
            "",
            "",
        ),
        (
            pd.Timestamp("2019-12-13"),
            "CARTE 12/12 AMAZON EU SARL PAYLI2090401/",
            -45.59,
            "",
            "",
            "",
        ),
        (
            pd.Timestamp("2019-12-12"),
            "CARTE 11/12 LECLERC MARLY",
            -15.75,
            "",
            "",
            "",
        ),
        (
            pd.Timestamp("2019-04-30"),
            "VIR MALAKOFF MEDERIC PREVOYANCE",
            45.0,
            "",
            "",
            "",
        ),
    ]
    expected = pd.DataFrame(
        columns=[
            "Date",
            "Label",
            "Amount",
            "Type",
            "MainCategory",
            "SubCategory",
        ],
        data=data,
    )
    assert_frame_equal(actual, expected)


def test_append_transactions_existing_target(cfg, tmpdir):
    account = FortuneoAccount("aType", "anId", "12345")
    pipeline = FortuneoTransactionPipeline(account, cfg)

    # given an existing CSV
    csv = Path(tmpdir) / "my.csv"
    csv.write_text(
        """\
Date,Label,Amount,Type,MainCategory,SubCategory
2020-02-12,Label A,20.0,expense,foo,bar
2020-02-13,Label B,30.0,expense,foo,bar
2020-02-14,Label C,40.0,expense,foo,bar
"""
    )

    # when appending new transactions
    pipeline.append_transactions(
        csv,
        pd.DataFrame(
            {
                "Date": [pd.Timestamp("2020-02-13"), pd.Timestamp("2020-02-14")],
                "Label": ["Label B", "Label D"],
                "Amount": [30.0, 40.0],
            }
        ),
    )

    # then they are appended successfully
    content = """\
Date,Label,Amount,Type,MainCategory,SubCategory
2020-02-12,Label A,20.0,expense,foo,bar
2020-02-13,Label B,30.0,expense,foo,bar
2020-02-14,Label C,40.0,expense,foo,bar
2020-02-14,Label D,40.0,,,
"""
    assert csv.read_text() == content


def test_append_transactions_nonexistent_target(cfg, tmpdir):
    account = FortuneoAccount("aType", "anId", "12345")
    pipeline = FortuneoTransactionPipeline(account, cfg)

    # given a nonexistent CSV
    csv = Path(tmpdir) / "my.csv"

    # when appending new transactions
    pipeline.append_transactions(
        csv,
        pd.DataFrame(
            {
                "Date": [pd.Timestamp("2020-02-13"), pd.Timestamp("2020-02-14")],
                "Label": ["Label B", "Label D"],
                "Amount": [30.0, 40.0],
                "Type": None,
                "MainCategory": None,
                "SubCategory": None,
            }
        ),
    )

    # then they are appended successfully
    content = """\
Date,Label,Amount,Type,MainCategory,SubCategory
2020-02-13,Label B,30.0,,,
2020-02-14,Label D,40.0,,,
"""
    assert csv.read_text() == content


def test_run(cfg):
    # given a Fortuneo account and data to be integrated
    account = FortuneoAccount("CHQ", "astark-FTN-CHQ", "12345")
    cfg.accounts.append(account)
    csv = (
        cfg.download_dir / "HistoriqueOperations_12345_du_14_01_2019_au_14_12_2019.csv"
    )
    summary = Summary(cfg)
    pipeline = FortuneoTransactionPipeline(account, cfg)

    # when running the pipeline
    pipeline.run(csv, summary)

    # then the transactions are integrated
    tx201904 = cfg.root_dir / "2019-04" / "2019-04.astark-FTN-CHQ.csv"
    assert (
        tx201904.read_text()
        == """\
Date,Label,Amount,Type,MainCategory,SubCategory
2019-04-30,VIR MALAKOFF MEDERIC PREVOYANCE,45.0,,,
"""
    )
    tx201912 = cfg.root_dir / "2019-12" / "2019-12.astark-FTN-CHQ.csv"
    assert (
        tx201912.read_text()
        == """\
Date,Label,Amount,Type,MainCategory,SubCategory
2019-12-12,CARTE 11/12 LECLERC MARLY,-15.75,,,
2019-12-13,CARTE 12/12 AMAZON EU SARL PAYLI2090401/,-45.59,,,
2019-12-13,CARTE 12/12 BRIOCHE DOREE METZ,-10.9,,,
2019-12-13,CARTE 12/12 FNAC METZ,-6.4,,,
"""
    )
    assert (
        str(summary)
        == f"""\
$$$ Summary $$$
---------------
1 files copied.
---------------
Sources:
- {csv}
Targets:
- {tx201904}
- {tx201912}
Finished."""
    )


def test_guess_meta(cfg):
    # Given a Fortuneo account
    account = FortuneoAccount("CHQ", "astark-FTN-CHQ", "12345")
    cfg.accounts.append(account)
    pipeline = FortuneoTransactionPipeline(account, cfg)
    cfg.autocomplete = [
        TxCompletion(
            tx_type="expense",
            main_category="shopping",
            sub_category="online",
            regex=r".*AMAZON.*",
        ),
        TxCompletion(
            tx_type="expense",
            main_category="shopping",
            sub_category="offline",
            regex=r".*FNAC METZ.*",
        ),
        TxCompletion(
            tx_type="expense",
            main_category="food",
            sub_category="supermarket",
            regex=r".*LECLERC MARLY.*",
        ),
    ]
    csv = (
        cfg.download_dir / "HistoriqueOperations_12345_du_14_01_2019_au_14_12_2019.csv"
    )

    # And transactions to be guessed
    transactions = pipeline.read_new_transactions(csv)

    # When guessing metadata on transactions
    transactions = pipeline.guess_meta(transactions)

    # Then the result is correct
    expected = pd.DataFrame(
        columns=[
            "Date",
            "Label",
            "Amount",
            "Type",
            "MainCategory",
            "SubCategory",
        ],
        data=[
            (
                pd.Timestamp("2019-12-13"),
                "CARTE 12/12 FNAC METZ",
                -6.4,
                "expense",
                "shopping",
                "offline",
            ),
            (
                pd.Timestamp("2019-12-13"),
                "CARTE 12/12 BRIOCHE DOREE METZ",
                -10.9,
                "",
                "",
                "",
            ),
            (
                pd.Timestamp("2019-12-13"),
                "CARTE 12/12 AMAZON EU SARL PAYLI2090401/",
                -45.59,
                "expense",
                "shopping",
                "online",
            ),
            (
                pd.Timestamp("2019-12-12"),
                "CARTE 11/12 LECLERC MARLY",
                -15.75,
                "expense",
                "food",
                "supermarket",
            ),
            (
                pd.Timestamp("2019-04-30"),
                "VIR MALAKOFF MEDERIC PREVOYANCE",
                45.0,
                "",
                "",
                "",
            ),
        ],
    )
    assert_frame_equal(transactions, expected)
