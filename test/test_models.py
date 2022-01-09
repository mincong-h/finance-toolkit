import re

from finance_toolkit.models import Summary, TxCompletion


# ---------- Class: Summary ----------


def test_summary_without_source_files(cfg):
    summary = Summary(cfg)
    assert (
        str(summary)
        == f"""\
$$$ Summary $$$
---------------
No CSV found in "{cfg.download_dir}".
---------------
Finished."""
    )


def test_summary_with_source_files(cfg):
    s = Summary(cfg)
    s.add_source(cfg.download_dir / "def")
    s.add_source(cfg.download_dir / "abc")
    s.add_target(cfg.root_dir / "456")
    s.add_target(cfg.root_dir / "123")
    assert (
        str(s)
        == f"""\
$$$ Summary $$$
---------------
2 files copied.
---------------
Sources:
- {cfg.download_dir}/abc
- {cfg.download_dir}/def
Targets:
- {cfg.root_dir}/123
- {cfg.root_dir}/456
Finished."""
    )


# ---------- Class: Configuration ----------


def test_configuration_categories(cfg):
    cfg.category_set.update(
        [
            "food/supermarket",
            "food/restaurant",
            "food/restaurant",  # duplicate
            "gouv/tax",
            "food/work",
        ]
    )
    # results are unique
    assert cfg.categories() == [
        "food/restaurant",
        "food/supermarket",
        "food/work",
        "gouv/tax",
    ]
    # results are filtered
    assert cfg.categories(lambda c: c.startswith("food")) == [
        "food/restaurant",
        "food/supermarket",
        "food/work",
    ]


# ---------- Class: TxCompletion ----------


def test_tx_completion_for_residence_tax():
    completion = TxCompletion(
        tx_type="tax",
        main_category="tax",
        sub_category="residence-tax",
        regex=re.compile(".*IMPOT TH.*"),
    )
    assert completion.match(
        "PRLV SEPA D.G.F.I.P. IMPOT x ECH/x ID EMETTEUR/x MDT/x REF/x LIB/x x                      x  IMPOT TH"  # noqa
    )


def test_tx_completion_for_property_tax():
    completion = TxCompletion(
        tx_type="tax",
        main_category="tax",
        sub_category="residence-tax",
        regex=re.compile(".*IMPOT TF.*"),
    )
    assert completion.match(
        "PRLV SEPA D.G.F.I.P. IMPOT x ECH/x ID EMETTEUR/x MDT/x REF/x LIB/x x                      x  IMPOT TF"  # noqa
    )


def test_tx_completion_for_social_charges():
    completion = TxCompletion(
        tx_type="tax",
        main_category="tax",
        sub_category="social-charges",
        regex=re.compile(".*PRELEVEMENT SOCIAUX.*"),
    )
    assert completion.match("PRELEVEMENT SOCIAUX/FISCAUX")


def test_tx_completion_for_income_tax():
    completion = TxCompletion(
        tx_type="tax",
        main_category="tax",
        sub_category="income-tax",
        regex=re.compile(".*IMPOT REVENUS.*"),
    )
    assert completion.match(
        "PRLV SEPA DGFIP IMPOT x ECH/x ID EMETTEUR/x MDT/x REF/x x 01 LIB/SOLDE IMPOT REVENUS 2020 N DE FACTURE x"  # noqa
    )
