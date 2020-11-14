from pathlib import Path

from finance_toolkit.accounts import (
    Account,
    BnpAccount,
    BoursoramaAccount,
    DegiroAccount,
    FortuneoAccount,
    OctoberAccount,
    RevolutAccount,
)


# ---------- Class: Account ----------


def test_account_eq_hash():
    a = Account("aType", "anId", "****0001", r".*")
    b = Account("aType", "anId", "****0001", r".*")
    assert a == b
    assert hash(a) == hash(b)


def test_account_repr_attrs():
    a = Account("aType", "anId", "00000001", r".*")
    assert repr(a) == "Account<type='aType', id='anId', num='****0001'>"
    assert a.type == "aType"
    assert a.id == "anId"
    assert a.num == "00000001"
    assert a.altered_num == "****0001"


# ---------- Class: BnpAccount ----------


def test_bnp_account_match():
    a1 = BnpAccount("aType", "anId", "****0267")
    assert a1.match(Path("E3580267.csv"))
    assert a1.match(Path("E1230267.csv"))

    a2 = BnpAccount("aType", "anId", "****0170")
    assert a2.match(Path("E3580170.csv"))
    assert a2.match(Path("E1230170.csv"))


def test_bnp_account_eq():
    a = BnpAccount("aType", "anId", "****0001")
    b = BnpAccount("aType", "anId", "****0001")
    c = Account("aType", "anId", "****0001", r"E\d{,3}0001\.csv")
    assert a == b
    assert a != c


def test_bnp_account_hash():
    a = BnpAccount("aType", "anId", "****0001")
    b = BnpAccount("aType", "anId", "****0001")
    c = Account("aType", "anId", "****0001", r"E\d{,3}0001\.csv")
    assert hash(a) == hash(b)
    assert hash(a) == hash(c)


# ---------- Class: BoursoramaAccount ----------


def test_boursorama_account_eq():
    a = BoursoramaAccount("aType", "anId", "****0001")
    b = BoursoramaAccount("aType", "anId", "****0001")
    c = Account(
        "aType", "anId", "****0001", r"export-operations-(\d{2}-\d{2}-\d{4})_.+\.csv"
    )
    assert a == b
    assert a != c


def test_boursorama_account_hash():
    a = BoursoramaAccount("aType", "anId", "****0001")
    b = BoursoramaAccount("aType", "anId", "****0001")
    c = Account(
        "aType", "anId", "****0001", r"export-operations-(\d{2}-\d{2}-\d{4})_.+\.csv"
    )
    assert hash(a) == hash(b)
    assert hash(a) == hash(c)


# ---------- Class: DegiroAccount ----------


def test_degiro_account_match():
    a = DegiroAccount("aType", "anId", "****0001")
    assert a.match(Path("Portfolio.csv"))


# ---------- Class: Fortuneo ----------


def test_fortuneo_account_match():
    a = FortuneoAccount("aType", "anId", "12345")
    assert a.match(Path("HistoriqueOperations_12345_du_14_01_2019_au_14_12_2019.csv"))


# ---------- Class: OctoberAccount ----------


def test_october_account_match():
    a = OctoberAccount("aType", "anId", "myLogin")
    assert a.match(Path("remboursements-myLogin.xlsx"))


# ---------- Class: RevolutAccount ----------


def test_revolut_account_match():
    a = RevolutAccount("aType", "anId", "myLogin")
    assert a.match(Path("Revolut-EUR-Statement-Oct – Nov 2020.csv"))
