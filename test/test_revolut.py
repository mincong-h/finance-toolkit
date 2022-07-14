import pandas as pd
from pandas.testing import assert_frame_equal

from finance_toolkit.models import Summary
from finance_toolkit.revolut import (
    RevolutAccount,
    RevolutBalancePipeline,
    RevolutTransactionPipeline,
)


def test_read_raw_2022_05_27_euro(cfg):
    # Given
    csv = (
        cfg.download_dir
        / "account-statement_2021-01-01_2022-05-27_undefined-undefined_abc123.csv"
    )
    account = RevolutAccount(
        account_type="EUR",
        account_id="user-REV-EUR",
        account_num="abc123",
        currency="EUR",
    )

    # When
    actual_balances, actual_transactions = RevolutTransactionPipeline(
        account, cfg
    ).read_raw(csv)

    # Then
    expected_balances = pd.DataFrame(
        columns=["Date", "Amount", "Currency"],
        data=[(pd.Timestamp("2021-01-05 14:00:41"), 74.43, "EUR")],
    )
    assert_frame_equal(actual_balances, expected_balances)

    expected_transactions = pd.DataFrame(
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
                pd.Timestamp("2021-01-05 14:00:41"),
                "Payment from M  Huang Mincong",
                10.00,
                "EUR",
                "TOPUP",
                "",
                "",
            ),
            (
                pd.Timestamp("2021-11-19 08:35:35"),
                "Balance migration to another region or legal entity",
                -100.00,
                "EUR",
                "TRANSFER",
                "",
                "",
            ),
        ],
    )
    assert_frame_equal(actual_transactions, expected_transactions)


def test_read_raw_2022_05_27_dollar(cfg):
    # Given
    csv = (
        cfg.download_dir
        / "account-statement_2021-01-01_2022-05-27_undefined-undefined_abc123.csv"
    )
    account = RevolutAccount(
        account_type=RevolutAccount.TYPE_CASH,
        account_id="user-REV-USD",
        account_num="abc123",
        currency="USD",
    )

    # When
    actual_balances, actual_transactions = RevolutTransactionPipeline(
        account, cfg
    ).read_raw(csv)

    # Then both data-frames are empty
    assert actual_balances.columns.values.tolist() == ["Date", "Amount", "Currency"]
    assert len(actual_balances) == 0

    assert actual_transactions.columns.values.tolist() == [
        "Date",
        "Label",
        "Amount",
        "Currency",
        "Type",
        "MainCategory",
        "SubCategory",
    ]
    assert len(actual_transactions.columns)


def test_integration_normal(cfg):
    (cfg.root_dir / "2021-01").mkdir()
    (cfg.root_dir / "2021-12").mkdir()

    # Given some existing files
    tx01 = cfg.root_dir / "2021-01" / "2021-01.user-REV-EUR.csv"
    tx01.write_text(
        """\
Date,Label,Amount,Currency,Type,MainCategory,SubCategory
2021-01-01,This is an existing transaction,10.0,EUR,transfer,,
"""
    )

    balances = cfg.root_dir / "balance.user-REV-EUR.csv"
    balances.write_text(
        """\
Date,Amount,Currency
2021-01-01 00:00:00,10.00,EUR
"""
    )

    account = RevolutAccount(
        account_type="EUR",
        account_id="user-REV-EUR",
        account_num="abc123",
        currency="EUR",
    )
    summary = Summary(cfg)
    new_file = (
        cfg.download_dir
        / "account-statement_2021-01-01_2022-05-27_undefined-undefined_abc123.csv"
    )

    # When running pipeline to integrate new lines
    RevolutBalancePipeline(account, cfg).run(new_file, summary)

    # Then
    assert (
        balances.read_text()
        == """\
Date,Amount,Currency
2021-01-01 00:00:00,10.0,EUR
2021-01-05 14:00:41,74.43,EUR
"""
    )

    # When
    RevolutTransactionPipeline(account, cfg).run(new_file, summary)
    # Then
    assert (
        tx01.read_text()
        == """\
Date,Label,Amount,Currency,Type,MainCategory,SubCategory
2021-01-01,This is an existing transaction,10.0,EUR,transfer,,
2021-01-05,Payment from M  Huang Mincong,10.0,EUR,income,,
"""
    )


def test_integration_missing_currency(cfg):
    """Ensure that we can integrate new data (balance and transactions) even if the currency is
    missing in the existing files."""
    (cfg.root_dir / "2021-01").mkdir()
    (cfg.root_dir / "2021-12").mkdir()

    # Given some existing files
    tx01 = cfg.root_dir / "2021-01" / "2021-01.user-REV-EUR.csv"
    tx01.write_text(
        """\
Date,Label,Amount,Type,MainCategory,SubCategory
2021-01-01,This is an existing transaction,10.0,transfer,,
"""
    )

    balances = cfg.root_dir / "balance.user-REV-EUR.csv"
    balances.write_text(
        """\
Date,Amount
2021-01-01 00:00:00,10.00
"""
    )

    account = RevolutAccount(
        account_type="EUR",
        account_id="user-REV-EUR",
        account_num="abc123",
        currency="EUR",
    )
    summary = Summary(cfg)
    new_file = (
        cfg.download_dir
        / "account-statement_2021-01-01_2022-05-27_undefined-undefined_abc123.csv"
    )

    # When running pipeline to integrate new lines
    RevolutBalancePipeline(account, cfg).run(new_file, summary)

    # Then
    assert (
        balances.read_text()
        == """\
Date,Amount,Currency
2021-01-01 00:00:00,10.0,EUR
2021-01-05 14:00:41,74.43,EUR
"""
    )

    # When
    RevolutTransactionPipeline(account, cfg).run(new_file, summary)
    # Then
    assert (
        tx01.read_text()
        == """\
Date,Label,Amount,Currency,Type,MainCategory,SubCategory
2021-01-01,This is an existing transaction,10.0,EUR,transfer,,
2021-01-05,Payment from M  Huang Mincong,10.0,EUR,income,,
"""
    )


def test_integration_deduplicate(cfg):
    # TODO
    pass
