import pandas as pd
from pandas.testing import assert_frame_equal

from finance_toolkit.pipeline_revolut import RevolutPipeline


def test_revolute_pipeline_read_raw_2022_05_27(cfg):
    # Given
    csv = (
        cfg.download_dir
        / "account-statement_2021-01-01_2022-05-27_undefined-undefined_abc123.csv"
    )
    # When
    actual_balances, actual_transactions = RevolutPipeline.read_raw(csv)

    # Then
    expected_balances = pd.DataFrame(
        columns=["Date", "Amount"], data=[(pd.Timestamp("2021-01-05 14:00:41"), 74.43)]
    )
    assert_frame_equal(actual_balances, expected_balances)
