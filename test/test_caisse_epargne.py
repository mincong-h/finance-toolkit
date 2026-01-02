import re
from pathlib import Path

import pandas as pd
from pandas.testing import assert_frame_equal

from finance_toolkit.caisse_epargne import (
    CaisseEpargneAccount,
    CaisseEpargneTransactionPipeline,
)
from finance_toolkit.models import Summary, TxType
from finance_toolkit.tx import TxCompletion


def test_caisse_epargne_account_pattern():
    account = CaisseEpargneAccount("CHQ", "test-CEP-CHQ", "12345678")
    assert account.match(Path("12345678_01112024_30112024.csv"))
    assert not account.match(Path("87654321_01112024_30112024.csv"))
    assert not account.match(Path("12345678.csv"))


def test_caisse_epargne_account_pattern_suffix():
    # Account number is a suffix of the full account number in the filename
    account = CaisseEpargneAccount("CHQ", "test-CEP-CHQ", "5678")
    # Should match files where the account number ends with the suffix
    assert account.match(Path("12345678_01112024_30112024.csv"))
    assert account.match(Path("99995678_01012025_31012025.csv"))
    # Should not match files where the suffix doesn't match
    assert not account.match(Path("12345679_01112024_30112024.csv"))
    assert not account.match(Path("56781234_01112024_30112024.csv"))


def test_caisse_epargne_transaction_pipeline_read_new_transactions(cfg):
    csv = cfg.download_dir / "12345678_01112024_30112024.csv"

    account = CaisseEpargneAccount("CHQ", "test-CEP-CHQ", "12345678")
    cfg.accounts.append(account)
    actual = CaisseEpargneTransactionPipeline(account, cfg).read_new_transactions(csv)

    expected = pd.DataFrame(
        columns=[
            "Date",
            "Label",
            "Amount",
            "Currency",
        ],
        data=[
            (
                pd.Timestamp("2024-11-14"),
                "CB SUPERMARCHE CENTRAL FACT 141124",
                -45.50,
                "EUR",
            ),
            (
                pd.Timestamp("2024-11-13"),
                "CB RESTAURANT ABC FACT 131124",
                -28.90,
                "EUR",
            ),
            (
                pd.Timestamp("2024-11-11"),
                "CB PHARMACIE DURAND FACT 111124",
                -12.30,
                "EUR",
            ),
            (pd.Timestamp("2024-11-09"), "VIR INST Employeur SA", None, "EUR"),
            (pd.Timestamp("2024-11-05"), "PRLV ASSURANCE HABITATION", -89.00, "EUR"),
        ],
    )
    assert_frame_equal(actual[["Date", "Label", "Amount", "Currency"]], expected)


def test_caisse_epargne_transaction_pipeline_guess_meta_account_type_chq(cfg):
    cols = ["Label", "Type", "MainCategory", "SubCategory"]

    account = CaisseEpargneAccount("CHQ", "test-CEP-CHQ", "12345678")
    cfg.accounts.append(account)
    pipeline = CaisseEpargneTransactionPipeline(account=account, cfg=cfg)
    raw = pd.DataFrame(columns=cols, data=[("Some Label", "", "", "")])
    expected = pd.DataFrame(
        columns=cols, data=[("Some Label", TxType.EXPENSE.value, "", "")]
    )
    actual = pipeline.guess_meta(raw)
    assert_frame_equal(actual, expected)


def test_caisse_epargne_transaction_pipeline_guess_meta_autocomplete(cfg):
    cols = ["Label", "Type", "MainCategory", "SubCategory"]
    raw = pd.DataFrame(
        columns=cols,
        data=[
            ("CB SUPERMARCHE CENTRAL", "", "", ""),
            ("CB RESTAURANT ABC", "", "", ""),
            ("PRLV ASSURANCE HABITATION", "", "", ""),
        ],
    )
    expected = pd.DataFrame(
        columns=cols,
        data=[
            ("CB SUPERMARCHE CENTRAL", "expense", "food", "supermarket"),
            ("CB RESTAURANT ABC", "expense", "food", "restaurant"),
            ("PRLV ASSURANCE HABITATION", "expense", "housing", "insurance"),
        ],
    )

    account = CaisseEpargneAccount("CHQ", "test-CEP-CHQ", "12345678")
    cfg.accounts.append(account)
    cfg.autocomplete.extend(
        [
            TxCompletion(
                tx_type="expense",
                main_category="food",
                sub_category="supermarket",
                regex=re.compile(r".*SUPERMARCHE.*"),
            ),
            TxCompletion(
                tx_type="expense",
                main_category="food",
                sub_category="restaurant",
                regex=re.compile(r".*RESTAURANT.*"),
            ),
            TxCompletion(
                tx_type="expense",
                main_category="housing",
                sub_category="insurance",
                regex=re.compile(r".*ASSURANCE.*"),
            ),
        ]
    )
    actual = CaisseEpargneTransactionPipeline(account, cfg).guess_meta(raw)
    assert_frame_equal(actual, expected)


def test_caisse_epargne_transaction_pipeline_run(cfg):
    # Given a Caisse d'Epargne account and data to be integrated
    account = CaisseEpargneAccount("CHQ", "test-CEP-CHQ", "12345678")
    cfg.accounts.append(account)
    csv = cfg.download_dir / "12345678_01112024_30112024.csv"
    summary = Summary(cfg)
    pipeline = CaisseEpargneTransactionPipeline(account, cfg)

    # When running the pipeline
    pipeline.run(csv, summary)

    # Then the transactions are integrated
    tx202411 = cfg.root_dir / "2024-11" / "2024-11.test-CEP-CHQ.csv"
    assert tx202411.exists()

    # And the summary is correct
    assert csv in summary.sources
    assert tx202411 in summary.targets
