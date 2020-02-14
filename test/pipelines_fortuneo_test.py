from pathlib import Path

import pandas as pd
from pandas.util.testing import assert_frame_equal

from tx.accounts import FortuneoAccount
from tx.pipelines import FortuneoTransactionPipeline


def test_fortuneo_transaction_pipeline_read_new_transactions(location, cfg):
    csv = location / "HistoriqueOperations_12345_du_14_01_2019_au_14_12_2019.csv"

    account = FortuneoAccount("aType", "anId", "12345")
    cfg.accounts.append(account)
    actual = FortuneoTransactionPipeline(account, cfg).read_new_transactions(csv)

    data = [
        (pd.Timestamp("2019-12-13"), "CARTE 12/12 FNAC METZ", -6.4, "", "", "", "",),
        (
            pd.Timestamp("2019-12-13"),
            "CARTE 12/12 BRIOCHE DOREE METZ",
            -10.9,
            "",
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
            "",
        ),
        (
            pd.Timestamp("2019-12-12"),
            "CARTE 11/12 LECLERC MARLY",
            -15.75,
            "",
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
            "IsRegular",
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
Date,Label,Amount,Type,MainCategory,SubCategory,IsRegular
2020-02-12,Label A,20.0,expense,foo,bar,True
2020-02-13,Label B,30.0,expense,foo,bar,True
2020-02-14,Label C,40.0,expense,foo,bar,True
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
Date,Label,Amount,Type,MainCategory,SubCategory,IsRegular
2020-02-12,Label A,20.0,expense,foo,bar,True
2020-02-13,Label B,30.0,expense,foo,bar,True
2020-02-14,Label C,40.0,expense,foo,bar,True
2020-02-14,Label D,40.0,,,,
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
            }
        ),
    )

    # then they are appended successfully
    content = """\
Date,Label,Amount,Type,MainCategory,SubCategory,IsRegular
2020-02-13,Label B,30.0,,,,
2020-02-14,Label D,40.0,,,,
"""
    assert csv.read_text() == content
