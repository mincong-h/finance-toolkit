from pathlib import Path

from finance_toolkit.account import (
    Account,
    DegiroAccount,
    OctoberAccount,
)
from finance_toolkit.bnp import BnpAccount
from finance_toolkit.boursorama import BoursoramaAccount
from finance_toolkit.fortuneo import FortuneoAccount
from finance_toolkit.revolut import RevolutAccount


# ---------- Class: Account ----------


def test_account_eq_hash():
    a = Account(
        account_type="aType",
        account_id="anId",
        account_num="****0001",
        currency="EUR",
        patterns=[r".*"],
    )
    b = Account(
        account_type="aType",
        account_id="anId",
        account_num="****0001",
        currency="EUR",
        patterns=[r".*"],
    )
    assert a == b
    assert hash(a) == hash(b)


def test_account_repr_attrs():
    a = Account(
        account_type="aType",
        account_id="anId",
        account_num="00000001",
        currency="USD",
        patterns=[r".*"],
    )
    assert (
        repr(a)
        == "Account<type='aType', id='anId', num='****0001', currency_symbol='USD', balance_filename='balance.anId.USD.csv', converted_balance_filename='balance.anId.EUR.csv'>"  # noqa: E501
    )
    assert a.type == "aType"
    assert a.id == "anId"
    assert a.num == "00000001"
    assert a.altered_num == "****0001"
    assert a.currency_symbol == "USD"
    assert a.balance_filename == "balance.anId.USD.csv"
    assert a.converted_balance_filename == "balance.anId.EUR.csv"


# ---------- Class: BnpAccount ----------


def test_bnp_account_match():
    a1 = BnpAccount(
        account_type="aType",
        account_id="anId",
        account_num="****0267",
    )
    assert a1.match(Path("E3580267.csv"))
    assert a1.match(Path("E1230267.csv"))

    a2 = BnpAccount(
        account_type="aType",
        account_id="anId",
        account_num="****0170",
    )
    assert a2.match(Path("E3580170.csv"))
    assert a2.match(Path("E1230170.csv"))


def test_bnp_account_eq():
    a = BnpAccount(
        account_type="aType",
        account_id="anId",
        account_num="****0001",
    )
    b = BnpAccount(
        account_type="aType",
        account_id="anId",
        account_num="****0001",
    )
    c = Account(
        account_type="aType",
        account_id="anId",
        account_num="****0001",
        currency="EUR",
        patterns=[r"E\d{,3}0001\.csv"],
    )
    assert a == b
    assert a != c


def test_bnp_account_hash():
    a = BnpAccount(
        account_type="aType",
        account_id="anId",
        account_num="****0001",
    )
    b = BnpAccount(
        account_type="aType",
        account_id="anId",
        account_num="****0001",
    )
    c = Account(
        account_type="aType",
        account_id="anId",
        account_num="****0001",
        currency="EUR",
        patterns=[r"E\d{,3}0001\.csv"],
    )
    assert hash(a) == hash(b)
    assert hash(a) == hash(c)


# ---------- Class: BoursoramaAccount ----------


def test_boursorama_account_eq():
    a = BoursoramaAccount(
        account_type="aType",
        account_id="anId",
        account_num="****0001",
    )
    b = BoursoramaAccount(
        account_type="aType",
        account_id="anId",
        account_num="****0001",
    )
    c = Account(
        account_type="aType",
        account_id="anId",
        account_num="****0001",
        currency="EUR",
        patterns=[r"export-operations-(\d{2}-\d{2}-\d{4})_.+\.csv"],
    )
    assert a == b
    assert a != c


def test_boursorama_account_hash():
    a = BoursoramaAccount(
        account_type="aType",
        account_id="anId",
        account_num="****0001",
    )
    b = BoursoramaAccount(
        account_type="aType",
        account_id="anId",
        account_num="****0001",
    )
    c = Account(
        account_type="aType",
        account_id="anId",
        account_num="****0001",
        patterns=[r"export-operations-(\d{2}-\d{2}-\d{4})_.+\.csv"],
        currency="EUR",
    )
    assert hash(a) == hash(b)
    assert hash(a) == hash(c)


# ---------- Class: DegiroAccount ----------


def test_degiro_account_match():
    a = DegiroAccount(
        account_type="aType",
        account_id="anId",
        account_num="****0001",
    )
    assert a.match(Path("Portfolio.csv"))


# ---------- Class: Fortuneo ----------


def test_fortuneo_account_match():
    a = FortuneoAccount(
        account_type="aType",
        account_id="anId",
        account_num="12345",
    )
    assert a.match(Path("HistoriqueOperations_12345_du_14_01_2019_au_14_12_2019.csv"))


# ---------- Class: OctoberAccount ----------


def test_october_account_match():
    a = OctoberAccount(
        account_type="aType",
        account_id="anId",
        account_num="myLogin",
    )
    assert a.match(Path("remboursements-myLogin.xlsx"))


# ---------- Class: RevolutAccount ----------


def test_revolut_account_match_euro():
    account1 = RevolutAccount(
        account_type="aType", account_id="anId", account_num="myLogin", currency="EUR"
    )
    assert account1.match(Path("Revolut-EUR-Statement-Oct – Nov 2020.csv"))
    assert account1.match(
        Path("account-statement_2021-01-01_2022-05-27_undefined-undefined_abc123.csv")
    )
    assert (
        account1.match(Path("account-statement_2021-01-01_2022-05-27_en_abc123.csv"))
        is False
    )

    account2 = RevolutAccount(
        account_type="aType",
        account_id="anId",
        account_num="abc123",
        currency="EUR",
    )
    assert account2.match(Path("Revolut-EUR-Statement-Oct – Nov 2020.csv"))
    assert account2.match(
        Path("account-statement_2021-01-01_2022-05-27_undefined-undefined_abc123.csv")
    )
    assert account2.match(
        Path("account-statement_2021-01-01_2022-05-27_undefined-undefined_edf123.csv")
    )
    assert (
        account1.match(Path("account-statement_2021-01-01_2022-05-27_en_abc123.csv"))
        is False
    )


def test_revolut_account_match_dollar():
    account = RevolutAccount(
        account_type="aType", account_id="anId", account_num="myLogin", currency="USD"
    )
    assert account.match(Path("Revolut-EUR-Statement-Oct – Nov 2020.csv"))
    assert account.match(
        Path("account-statement_2021-01-01_2022-05-27_undefined-undefined_abc123.csv")
    )
    assert account.match(
        Path("account-statement_2021-01-01_2022-05-27_undefined-undefined_edf123.csv")
    )
    assert (
        account.match(Path("account-statement_2021-01-01_2022-05-27_en_abc123.csv"))
        is False
    )
    assert (
        account.match(Path("account-statement_2021-01-01_2022-05-27_en_edf123.csv"))
        is False
    )
