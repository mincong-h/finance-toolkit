from pathlib import Path

import pandas as pd
from pandas.testing import assert_frame_equal

from finance_toolkit.account import Account
from finance_toolkit.pipeline import GeneralBalancePipeline


def test_read_balance(cfg, tmpdir):
    # Given
    account = Account(
        account_type="CHQ",
        account_id="anAccountId",
        account_num="anAccountNum",
        currency="EUR",
        patterns=[r"unknown"],
    )
    pipeline = GeneralBalancePipeline(account, cfg)
    csv = Path(tmpdir) / "my.csv"
    csv.write_text(
        """\
Date,Amount
2020-11-20,100.0
2020-11-21,99.0
"""
    )

    # When
    actual_balance_df = pipeline.read_balance(csv)

    # Then
    expected_balance_df = pd.DataFrame(
        columns=["Date", "Amount", "Account", "AccountId", "AccountType"],
        data=[
            (pd.Timestamp("2020-11-20"), 100.0, "anAccountId", "anAccountNum", "CHQ"),
            (pd.Timestamp("2020-11-21"), 99.0, "anAccountId", "anAccountNum", "CHQ"),
        ],
    )
    assert_frame_equal(actual_balance_df, expected_balance_df)
