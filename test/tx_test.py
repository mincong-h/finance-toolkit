from tempfile import TemporaryDirectory
from unittest.mock import patch, call

import pytest
from pandas.testing import assert_frame_equal
from src import tx
from src.tx import *


# ---------- Top Level Functions ----------


def test_read_bnp_tx_ok(cfg):
    cfg.category_set.add("food/workfood")

    csv = cfg.root_dir / "2019-03.mhuang-CHQ.csv"
    csv.write_text(
        """\
Date,bnpMainCategory,bnpSubCategory,Label,Amount,Type,mainCategory,subCategory,IsRegular
2018-04-30,FAC.CB,FACTURE CARTE,DU 270418 MC DONALDS PARIS 18 CARTE 4974,-4.95,expense,food,workfood,True
"""
    )
    actual_df = tx.read_bnp_tx(csv, cfg)
    expected_df = pd.DataFrame(
        {
            "Date": pd.Timestamp("2018-04-30"),
            "ShortType": "FAC.CB",
            "LongType": "FACTURE CARTE",
            "Label": "DU 270418 MC DONALDS PARIS 18 CARTE 4974",
            "Amount": -4.95,
            "Type": "expense",
            "Category": "food",
            "SubCategory": "workfood",
            "IsRegular": True,
        },
        index=[0],
    )
    assert_frame_equal(actual_df, expected_df)


@patch("builtins.print")
def test_read_bnp_tx_validate_errors(mocked_print, cfg):
    cfg.category_set.add("food/restaurant")

    csv = cfg.root_dir / "2019-03.mhuang-CHQ.csv"
    csv.write_text(
        """\
Date,bnpMainCategory,bnpSubCategory,Label,Amount,Type,mainCategory,subCategory,IsRegular
2018-04-30,main,sub,myLabel,-1.0,expense,food,restaurant,True
2018-04-30,main,sub,myLabel,-2.0,expense,,,True
2018-04-30,main,sub,myLabel,-3.0,expense,food,,True
2018-04-30,main,sub,myLabel,-4.0,expense,,restaurant,True
"""
    )
    actual_df = tx.read_bnp_tx(csv, cfg)
    expected_df = pd.DataFrame(
        {
            "Date": pd.Timestamp("2018-04-30"),
            "ShortType": "main",
            "LongType": "sub",
            "Label": "myLabel",
            "Amount": -1.0,
            "Type": "expense",
            "Category": "food",
            "SubCategory": "restaurant",
            "IsRegular": True,
        },
        index=[0],
    )
    assert_frame_equal(actual_df, expected_df)
    assert mocked_print.mock_calls == [
        call(f"{csv}:"),
        call("  - Line 3: Category 'nan/nan' does not exist."),
        call("  - Line 4: Category 'food/nan' does not exist."),
        call("  - Line 5: Category 'nan/restaurant' does not exist."),
    ]


def test_read_boursorama_tx_ok(cfg):
    cfg.autocomplete.extend(
        (("expense", "food", "restaurant", True), r".*ROYAL PLAISANC.*")
    )
    cfg.category_set.add("food/restaurant")

    csv = cfg.root_dir / "2019-06.mhuang-BRS-CHQ.csv"
    csv.write_text(
        """\
dateOp,dateVal,Label,brsMainCategory,brsSubCategory,supplierFound,Amount,Type,mainCategory,subCategory,IsRegular
2019-06-26,2019-06-26,CARTE 25/06/19 93 ROYAL PLAISANC CB*1234,"Restaurants, bars, discothèques...",Loisirs,royal plaisance,-20.1,expense,food,restaurant,True
"""
    )
    actual_df = tx.read_boursorama_tx(csv, cfg)
    expected_df = pd.DataFrame(
        {
            "Date": pd.Timestamp("2019-06-26"),
            "ShortType": "Restaurants, bars, discothèques...",
            "LongType": "Loisirs",
            "Label": "CARTE 25/06/19 93 ROYAL PLAISANC CB*1234",
            "Amount": -20.1,
            "Type": "expense",
            "Category": "food",
            "SubCategory": "restaurant",
            "IsRegular": True,
        },
        index=[0],
    )
    assert_frame_equal(actual_df, expected_df)


@patch("builtins.print")
def test_read_boursorama_tx_validation_errors(mocked_print, cfg):
    """
    Ensure validation errors are handled correctly.
    """
    cfg.category_set.add("food/restaurant")

    csv = cfg.root_dir / "2019-06.mhuang-BRS-CHQ.csv"
    csv.write_text(
        """\
dateOp,dateVal,Label,brsMainCategory,brsSubCategory,supplierFound,Amount,Type,mainCategory,subCategory,IsRegular
2019-06-26,2019-06-26,myLabel,main,sub,supplier,-1.0,expense,food,restaurant,True
2019-06-26,2019-06-26,myLabel,main,sub,supplier,-2.0,expense,,,True
2019-06-26,2019-06-26,myLabel,main,sub,supplier,-3.0,expense,food,,True
2019-06-26,2019-06-26,myLabel,main,sub,supplier,-4.0,expense,,restaurant,True
"""
    )
    actual_df = tx.read_boursorama_tx(csv, cfg)
    expected_df = pd.DataFrame(
        {
            "Date": pd.Timestamp("2019-06-26"),
            "ShortType": "main",
            "LongType": "sub",
            "Label": "myLabel",
            "Amount": -1.0,
            "Type": "expense",
            "Category": "food",
            "SubCategory": "restaurant",
            "IsRegular": True,
        },
        index=[0],
    )
    assert_frame_equal(actual_df, expected_df)
    assert mocked_print.mock_calls == [
        call(f"{csv}:"),
        call("  - Line 3: Category 'nan/nan' does not exist."),
        call("  - Line 4: Category 'food/nan' does not exist."),
        call("  - Line 5: Category 'nan/restaurant' does not exist."),
    ]


def test_merge_bank_tx():
    df1 = pd.DataFrame(
        {
            "Date": pd.Timestamp("2019-06-26"),
            "ShortType": "aShortType",
            "LongType": "aLongType",
            "Label": "aLabel",
            "Amount": -20.1,
            "Type": "expense",
            "Category": "food",
            "SubCategory": "resto",
            "IsRegular": True,
        },
        index=[0],
    )
    df2 = pd.DataFrame(
        {
            "Date": pd.Timestamp("2019-06-27"),
            "ShortType": "aShortType",
            "LongType": "aLongType",
            "Label": "aLabel",
            "Amount": -20.1,
            "Type": "expense",
            "Category": "food",
            "SubCategory": "resto",
            "IsRegular": True,
        },
        index=[0],
    )

    actual_df = tx.merge_bank_tx([df1, df2])
    expected_df = pd.DataFrame(
        {
            "Date": [pd.Timestamp("2019-06-26"), pd.Timestamp("2019-06-27")],
            "ShortType": ["aShortType", "aShortType"],
            "LongType": ["aLongType", "aLongType"],
            "Label": ["aLabel", "aLabel"],
            "Amount": [-20.1, -20.1],
            "Type": ["expense", "expense"],
            "Category": ["food", "food"],
            "SubCategory": ["resto", "resto"],
            "IsRegular": [True, True],
        }
    )
    assert_frame_equal(actual_df, expected_df)


def test_merge_balances(cfg):
    cfg.accounts.extend(
        [
            BnpAccount("CHQ", "mhuang-BNP-CHQ", "****9413"),
            BnpAccount("CHQ", "mhuang-BRS-CHQ", "****7485"),
        ]
    )
    bnp = cfg.root_dir / "balance.mhuang-BNP-CHQ.csv"
    brs = cfg.root_dir / "balance.mhuang-BRS-CHQ.csv"
    bnp.write_text(
        """\
mainCategory,subCategory,accountNum,Date,Amount
Compte à Vue,Compte de chèques,****9413,2018-07-04,100.00
Compte à Vue,Compte de chèques,****9413,2019-07-04,100.00
"""
    )
    brs.write_text(
        """\
Date,Amount
2018-07-04,200.0
2019-07-04,200.0
"""
    )

    actual_df = tx.merge_balances([bnp, brs], cfg)
    cols = ["Date", "Account", "AccountId", "Amount", "AccountType"]
    data = [
        (pd.Timestamp("2018-07-04"), "mhuang-BNP-CHQ", "****9413", 100.0, "CHQ"),
        (pd.Timestamp("2018-07-04"), "mhuang-BRS-CHQ", "****7485", 200.0, "CHQ"),
        (pd.Timestamp("2019-07-04"), "mhuang-BNP-CHQ", "****9413", 100.0, "CHQ"),
        (pd.Timestamp("2019-07-04"), "mhuang-BRS-CHQ", "****7485", 200.0, "CHQ"),
    ]
    expected_df = pd.DataFrame(columns=cols, data=data)
    assert_frame_equal(actual_df, expected_df)


def test_validate_tx(cfg):
    cfg.category_set.add("food/workfood")

    err1 = tx.validate_tx(
        pd.Series(
            {
                "Date": pd.Timestamp("2018-04-30"),
                "ShortType": "aShortType",
                "LongType": "aLongType",
                "Label": "aLabel",
                "Amount": -4.95,
                "Type": "X",
                "Category": "food",
                "SubCategory": "workfood",
                "IsRegular": True,
            }
        ),
        cfg,
    )
    assert "Unknown transaction type: X" == err1

    err2 = tx.validate_tx(
        pd.Series(
            {
                "Date": pd.Timestamp("2018-04-30"),
                "ShortType": "aShortType",
                "LongType": "aLongType",
                "Label": "aLabel",
                "Amount": -4.95,
                "Type": "expense",
                "Category": "X",
                "SubCategory": "workfood",
                "IsRegular": True,
            }
        ),
        cfg,
    )
    assert "Category 'X/workfood' does not exist." == err2

    err3 = tx.validate_tx(
        pd.Series(
            {
                "Date": pd.Timestamp("2018-04-30"),
                "ShortType": "aShortType",
                "LongType": "aLongType",
                "Label": "aLabel",
                "Amount": -4.95,
                "Type": "expense",
                "Category": "food",
                "SubCategory": "X",
                "IsRegular": True,
            }
        ),
        cfg,
    )
    assert "Category 'food/X' does not exist." == err3

    err4 = tx.validate_tx(
        pd.Series(
            {
                "Date": pd.Timestamp("2018-04-30"),
                "ShortType": "aShortType",
                "LongType": "aLongType",
                "Label": "aLabel",
                "Amount": -4.95,
                "Type": "expense",
                "Category": "food",
                "SubCategory": "workfood",
                "IsRegular": "X",
            }
        ),
        cfg,
    )
    assert "Unknown regularity: X" == err4

    err5 = tx.validate_tx(
        pd.Series(
            {
                "Date": pd.Timestamp("2018-04-30"),
                "ShortType": "aShortType",
                "LongType": "aLongType",
                "Label": "aLabel",
                "Amount": -4.95,
                "Type": "expense",
                "Category": "food",
                "SubCategory": "workfood",
                "IsRegular": True,
            }
        ),
        cfg,
    )
    assert "" == err5


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


# ---------- Class: OctoberAccount ----------


def test_october_account_match():
    a = OctoberAccount("aType", "anId", "myLogin")
    assert a.match(Path("remboursements-myLogin.xlsx"))


# ---------- Class: AccountPipeline ----------


def test_create_pipeline(cfg):
    p1 = AccountPipeline.create_pipeline(
        BnpAccount("CHQ", "foo-BNP-CHQ", "****0001"), cfg
    )
    p2 = AccountPipeline.create_pipeline(
        BoursoramaAccount("CHQ", "foo-BNP-CHQ", "****0001"), cfg
    )
    p3 = AccountPipeline.create_pipeline(
        Account("unknown", "unknown", "unknown", "unknown"), cfg
    )

    assert isinstance(p1, BnpPipeline)
    assert isinstance(p2, BoursoramaPipeline)
    assert isinstance(p3, AccountPipeline)


# ---------- Class: BnpPipeline ----------


def test_bnp_pipeline_integrate(cfg):
    header = (
        "Date,bnpMainCategory,bnpSubCategory,Label,Amount,Type,"
        "mainCategory,subCategory,IsRegular\n"
    )

    (cfg.root_dir / "2018-08").mkdir()
    (cfg.root_dir / "2018-09").mkdir()

    # Given two existing CSVs for transactions
    tx08 = cfg.root_dir / "2018-08" / "2018-08.xxx.csv"
    tx09 = cfg.root_dir / "2018-09" / "2018-09.xxx.csv"
    with tx08.open("w") as f:
        f.write(header)
        f.write("2018-08-30,M,S,myLabel,-0.49,expense,main,sub,True\n")
    with tx09.open("w") as f:
        f.write(header)
        f.write("2018-09-01,M,S,myLabel,-1.49,expense,main,sub,True\n")

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
    summary = Summary(Path("/path/to/sources"))
    account = BnpAccount("CHQ", "xxx", "****1234")
    cfg.accounts.append(account)
    pipeline = BnpPipeline(account, cfg)
    pipeline.integrate(new_file, cfg.root_dir, summary)

    # Then the new lines are integrated
    expected_lines8 = [
        header,
        "2018-08-30,M,S,myLabel,-0.49,expense,main,sub,True\n",
        "2018-08-31,M,S,myLabel,-0.99,expense,,,\n",
    ]
    with tx08.open("r") as f:
        actual_lines8 = f.readlines()
    assert actual_lines8 == expected_lines8

    expected_lines9 = [
        header,
        "2018-09-01,M,S,myLabel,-1.49,expense,main,sub,True\n",
        "2018-09-02,M,S,myLabel,-2.49,expense,,,\n",
    ]
    with tx09.open("r") as f:
        actual_lines9 = f.readlines()
    assert actual_lines9 == expected_lines9

    # And the summary is correct
    assert new_file in summary.sources
    assert tx08 in summary.targets
    assert tx09 in summary.targets
    assert b in summary.targets


def test_bnp_pipeline_write_balances(cfg):
    # Given an existing CSV file with 2 rows
    csv = cfg.root_dir / "balance.xxx.csv"
    csv.write_text(
        """\
mainCategory,subCategory,accountNum,Date,Amount
main,sub,****1234,2018-08-02,724.37
main,sub,****1234,2018-07-04,189.29
"""
    )

    # When writing new row into the CSV file
    cols = ["mainCategory", "subCategory", "accountNum", "Date", "Amount"]
    data = [("main", "sub", "****1234", pd.Timestamp("2018-09-02"), 924.37)]
    new_lines = pd.DataFrame(columns=cols, data=data)
    BnpPipeline.write_balances(csv, new_lines)

    # Then rows are available and sorted
    assert (
        csv.read_text()
        == """\
mainCategory,subCategory,accountNum,Date,Amount
main,sub,****1234,2018-07-04,189.29
main,sub,****1234,2018-08-02,724.37
main,sub,****1234,2018-09-02,924.37
"""
    )


def test_bnp_pipeline_read_raw(location):
    # Given an existing CSV for BNP
    # When reading its content
    csv = location / "E1851234.csv"
    actual_balances, actual_transactions = BnpPipeline.read_raw(csv)

    # Then the balances DataFrame is read correctly
    b_cols = ["mainCategory", "subCategory", "accountNum", "Date", "Amount"]
    b_data = [
        (
            "Crédit immobilier",
            "Crédit immobilier",
            "****1234",
            pd.Timestamp("2019-07-03"),
            -123456.78,
        )
    ]
    expected_balances = pd.DataFrame(columns=b_cols, data=b_data)
    assert_frame_equal(actual_balances, expected_balances)

    # And the transactions DataFrame is read correctly
    t_cols = [
        "Date",
        "bnpMainCategory",
        "bnpSubCategory",
        "Label",
        "Amount",
        "Type",
        "mainCategory",
        "subCategory",
        "IsRegular",
    ]
    t_data = [
        (
            pd.Timestamp("2019-06-05"),
            "",
            "",
            "AMORTISSEMENT PRET 1234",
            67.97,
            "",
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
    cols = ["Label", "Type", "mainCategory", "subCategory", "IsRegular"]

    account = BnpAccount(cat, "xxx", "****1234")
    cfg.accounts.append(account)
    pipeline = BnpPipeline(account=account, cfg=cfg)
    raw = pd.DataFrame(columns=cols, data=[("Label", "", "", "", "")])
    expected = pd.DataFrame(columns=cols, data=[("Label", label, "", "", value)])
    actual = pipeline.guess_meta(raw)
    assert_frame_equal(actual, expected)


def test_bnp_pipeline_guess_meta_transaction_label(cfg):
    cols = ["Label", "Type", "mainCategory", "subCategory", "IsRegular"]
    raw = pd.DataFrame(
        columns=cols,
        data=[
            ("FOUJITA", "", "", "", False),  # find
            ("FOUJITA LEETCODE", "", "", "", False),  # find first
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
    actual = BnpPipeline(account, cfg).guess_meta(raw)
    assert_frame_equal(actual, expected)


def test_bnp_pipeline_append_tx_file_nonexistent_csv():
    df = pd.DataFrame(
        columns=[
            "Date",
            "bnpMainCategory",
            "bnpSubCategory",
            "Label",
            "Amount",
            "Type",
            "mainCategory",
            "subCategory",
            "IsRegular",
        ],
        data=[(pd.Timestamp("2019-08-01"), "m", "s", "myLabel", 10.0, "", "", "", "")],
    )
    with TemporaryDirectory() as root:
        csv = Path(root) / "my.csv"
        BnpPipeline.append_tx_file(csv, df)
        assert (
            csv.read_text()
            == """\
Date,bnpMainCategory,bnpSubCategory,Label,Amount,Type,mainCategory,subCategory,IsRegular
2019-08-01,m,s,myLabel,10.0,,,,
"""
        )


def test_bnp_pipeline_append_tx_file_existing_csv():
    df = pd.DataFrame(
        columns=[
            "Date",
            "bnpMainCategory",
            "bnpSubCategory",
            "Label",
            "Amount",
            "Type",
            "mainCategory",
            "subCategory",
            "IsRegular",
        ],
        data=[(pd.Timestamp("2019-08-01"), "m", "s", "myLabel", 10.0, "", "", "", "")],
    )
    with TemporaryDirectory() as root:
        csv = Path(root) / "my.csv"
        csv.write_text(
            """\
Date,bnpMainCategory,bnpSubCategory,Label,Amount,Type,mainCategory,subCategory,IsRegular
2019-08-01,m,s,myLabel,10.0,myType,main,sub,True
"""
        )
        BnpPipeline.append_tx_file(csv, df)
        assert (
            csv.read_text()
            == """\
Date,bnpMainCategory,bnpSubCategory,Label,Amount,Type,mainCategory,subCategory,IsRegular
2019-08-01,m,s,myLabel,10.0,myType,main,sub,True
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
            "bnpMainCategory",
            "bnpSubCategory",
            "Label",
            "Amount",
            "Type",
            "mainCategory",
            "subCategory",
            "IsRegular",
        ],
        data=[
            (pd.Timestamp("2019-08-01"), "m", "s", "myLabel", 10.0, "", "", "", ""),
            (pd.Timestamp("2019-08-01"), "m", "s", "myLabel", 11.0, "", "", "", ""),
        ],
    )
    with TemporaryDirectory() as root:
        csv = Path(root) / "my.csv"
        BnpPipeline.append_tx_file(csv, df)
        assert (
            csv.read_text()
            == """\
Date,bnpMainCategory,bnpSubCategory,Label,Amount,Type,mainCategory,subCategory,IsRegular
2019-08-01,m,s,myLabel,10.0,,,,
2019-08-01,m,s,myLabel,11.0,,,,
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
dateOp,dateVal,Label,brsMainCategory,brsSubCategory,supplierFound,Amount,Type,mainCategory,subCategory,IsRegular
2019-08-29,2019-08-29,VIR Virement interne depuis BOURSORA,Virements reçus de comptes à comptes,Mouvements internes créditeurs,virement interne depuis boursora,30.0,transfer,,,False
"""
    )
    tx09.write_text(
        """\
dateOp,dateVal,Label,brsMainCategory,brsSubCategory,supplierFound,Amount,Type,mainCategory,subCategory,IsRegular
2019-09-01,2019-09-01,VIR Virement interne depuis BOURSORA,Virements reçus de comptes à comptes,Mouvements internes créditeurs,virement interne depuis boursora,40.0,transfer,,,False
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
dateOp;dateVal;Label;category;categoryParent;supplierFound;Amount;accountNum;accountLabel;accountbalance
2019-08-30;2019-08-30;"VIR Virement interne depuis BOURSORA";"Virements reçus de comptes à comptes";"Mouvements internes créditeurs";"virement interne depuis boursora";10,00;00001234;"COMPTE SUR LIVRET";"1 000,00"
2019-09-02;2019-09-02;"VIR Virement interne depuis BOURSORA";"Virements reçus de comptes à comptes";"Mouvements internes créditeurs";"virement interne depuis boursora";11,00;00001234;"COMPTE SUR LIVRET";"1 000,00"
""",
        encoding="ISO-8859-1",
    )

    # When integrating new lines
    summary = Summary(Path("/path/to/sources"))
    account = BoursoramaAccount("LVR", "xxx", "****1234")
    cfg.accounts.append(account)
    BoursoramaPipeline(account, cfg).integrate(new_file, cfg.root_dir, summary)

    # Then the new lines are integrated
    assert (
        tx08.read_text()
        == """\
dateOp,dateVal,Label,brsMainCategory,brsSubCategory,supplierFound,Amount,Type,mainCategory,subCategory,IsRegular
2019-08-29,2019-08-29,VIR Virement interne depuis BOURSORA,Virements reçus de comptes à comptes,Mouvements internes créditeurs,virement interne depuis boursora,30.0,transfer,,,False
2019-08-30,2019-08-30,VIR Virement interne depuis BOURSORA,Virements reçus de comptes à comptes,Mouvements internes créditeurs,virement interne depuis boursora,10.0,transfer,,,False
"""
    )
    assert (
        tx09.read_text()
        == """\
dateOp,dateVal,Label,brsMainCategory,brsSubCategory,supplierFound,Amount,Type,mainCategory,subCategory,IsRegular
2019-09-01,2019-09-01,VIR Virement interne depuis BOURSORA,Virements reçus de comptes à comptes,Mouvements internes créditeurs,virement interne depuis boursora,40.0,transfer,,,False
2019-09-02,2019-09-02,VIR Virement interne depuis BOURSORA,Virements reçus de comptes à comptes,Mouvements internes créditeurs,virement interne depuis boursora,11.0,transfer,,,False
"""
    )

    # And the balance is correct
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


def test_boursorama_account_read_raw(location, cfg):
    csv = location / "export-operations-30-03-2019_08-50-51.csv"

    account = BoursoramaAccount("type1", "name1", "****1234")
    cfg.accounts.append(account)
    b, tx = BoursoramaPipeline(account, cfg).read_raw(csv)

    expected_b = pd.DataFrame(
        {"accountNum": "001234", "Amount": 370.0, "Date": pd.Timestamp("2019-03-29")},
        index=[0],
    )
    assert_frame_equal(expected_b, b)
    expected_tx = pd.DataFrame(
        columns=[
            "dateOp",
            "dateVal",
            "Label",
            "brsMainCategory",
            "brsSubCategory",
            "supplierFound",
            "Amount",
            "accountNum",
            "accountLabel",
            "accountBalance",
        ],
        data=[
            (
                pd.Timestamp("2019-03-12"),
                pd.Timestamp("2019-03-12"),
                "Prime Parrainage",
                "Virements reçus",
                "Virements reçus",
                "prime parrainage",
                80.0,
                "001234",
                "BOURSORAMA BANQUE",
                370.0,
            ),
            (
                pd.Timestamp("2019-03-12"),
                pd.Timestamp("2019-03-12"),
                "VIR VIREMENT CREATION COMPTE",
                "Virements reçus",
                "Virements reçus",
                "virement creation compte",
                300.0,
                "001234",
                "BOURSORAMA BANQUE",
                370.0,
            ),
            (
                pd.Timestamp("2019-03-12"),
                pd.Timestamp("2019-03-12"),
                "VIR VIREMENT CREATION COMPTE",
                "Virements émis de comptes à comptes",
                "Mouvements internes débiteurs",
                "virement creation compte",
                -10.0,
                "001234",
                "BOURSORAMA BANQUE",
                370.0,
            ),
        ],
    )
    assert_frame_equal(expected_tx, tx)


def test_boursorama_account_read_raw_account_2(location, cfg):
    csv = location / "export-operations-30-03-2019_08-50-51.csv"

    account = BoursoramaAccount("type2", "name2", "****3607")
    cfg.accounts.append(account)
    b, tx = BoursoramaPipeline(account, cfg).read_raw(csv)

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
            "dateOp": pd.Timestamp("2019-03-12"),
            "dateVal": pd.Timestamp("2019-03-12"),
            "Label": "VIR VIREMENT CREATION COMPTE",
            "brsMainCategory": "Virements reçus de comptes à comptes",
            "brsSubCategory": "Mouvements internes créditeurs",
            "supplierFound": "virement creation compte",
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
        BoursoramaPipeline.write_balances(csv, new_lines)

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
dateOp,dateVal,Label,brsMainCategory,brsSubCategory,supplierFound,Amount,Type,mainCategory,subCategory,IsRegular
2018-09-26,2018-09-26,CARTE 25/09/18 93 LABEL,"Restaurants, bars",Loisirs,My Restaurant,-20.1,expense,food,resto,True
"""
        )

        # When writing new row into the CSV file
        cols = [
            "dateOp",
            "dateVal",
            "Label",
            "brsMainCategory",
            "brsSubCategory",
            "supplierFound",
            "Amount",
            "Type",
            "mainCategory",
            "subCategory",
            "IsRegular",
        ]
        data = [
            (
                pd.Timestamp("2018-09-27"),
                pd.Timestamp("2018-09-27"),
                "L",
                "M",
                "S",
                "SP",
                -10.0,
                "expense",
                "M",
                "S",
                True,
            )
        ]
        new_lines = pd.DataFrame(columns=cols, data=data)
        BoursoramaPipeline.append_tx(csv, new_lines)

        # Then rows are available and sorted
        assert (
            csv.read_text()
            == """\
dateOp,dateVal,Label,brsMainCategory,brsSubCategory,supplierFound,Amount,Type,mainCategory,subCategory,IsRegular
2018-09-26,2018-09-26,CARTE 25/09/18 93 LABEL,"Restaurants, bars",Loisirs,My Restaurant,-20.1,expense,food,resto,True
2018-09-27,2018-09-27,L,M,S,SP,-10.0,expense,M,S,True
"""
        )


def test_boursorama_pipeline_append_tx_drop_duplicates():
    with TemporaryDirectory() as d:
        # Given an existing CSV
        csv = Path(d) / "my.csv"
        csv.write_text(
            """\
dateOp,dateVal,Label,brsMainCategory,brsSubCategory,supplierFound,Amount,Type,mainCategory,subCategory,IsRegular
2018-09-26,2018-09-26,myLabel,main,sub,supplier,-20.1,expense,food,resto,True
"""
        )

        # When writing new row into the CSV file
        cols = [
            "dateOp",
            "dateVal",
            "Label",
            "brsMainCategory",
            "brsSubCategory",
            "supplierFound",
            "Amount",
            "Type",
            "mainCategory",
            "subCategory",
            "IsRegular",
        ]
        data = [
            (
                pd.Timestamp("2018-09-26"),
                pd.Timestamp("2018-09-26"),
                "myLabel",
                "main",
                "sub",
                "supplier",
                -20.1,
                "expense",
                "food",
                "resto",
                True,
            )
        ]
        new_lines = pd.DataFrame(columns=cols, data=data)
        BoursoramaPipeline.append_tx(csv, new_lines)

        # Then rows has no duplicates
        assert (
            csv.read_text()
            == """\
dateOp,dateVal,Label,brsMainCategory,brsSubCategory,supplierFound,Amount,Type,mainCategory,subCategory,IsRegular
2018-09-26,2018-09-26,myLabel,main,sub,supplier,-20.1,expense,food,resto,True
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
    cols = ["Label", "Type", "mainCategory", "subCategory", "IsRegular"]

    account = BoursoramaAccount(cat, "xxx", "****1234")
    cfg.accounts.append(account)
    raw_df = pd.DataFrame(columns=cols, data=[("Label", "", "", "", "")])
    expected_df = pd.DataFrame(columns=cols, data=[("Label", label, "", "", False)])
    actual_df = BoursoramaPipeline(account, cfg).guess_meta(raw_df)
    assert_frame_equal(actual_df, expected_df)


def test_boursorama_account_guess_mata_transaction_label(cfg):
    cols = ["Label", "Type", "mainCategory", "subCategory", "IsRegular"]

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
    actual = BoursoramaPipeline(account, cfg).guess_meta(raw)
    assert_frame_equal(actual, expected)


# ---------- Class: Summary ----------


def test_summary_without_source_files():
    summary = Summary(Path("/path/to/sources"))
    assert (
        str(summary)
        == """\
$$$ Summary $$$
---------------
No CSV found in "/path/to/sources".
---------------
Finished."""
    )


def test_summary_with_source_files():
    s = Summary(Path("/path/to/sources"))
    s.add_source(Path("/path/to/sources/def"))
    s.add_source(Path("/path/to/sources/abc"))
    s.add_target(Path("/path/to/targets/456"))
    s.add_target(Path("/path/to/targets/123"))
    assert (
        str(s)
        == """\
$$$ Summary $$$
---------------
2 files copied.
---------------
Sources:
- /path/to/sources/abc
- /path/to/sources/def
Targets:
- /path/to/targets/123
- /path/to/targets/456
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


# ---------- Class: Configurator ----------


def test_configurator_load_categories_with_content():
    # language=yml
    cfg = yaml.safe_load(
        """\
categories:
  - a/1
  - a/2
  - b/1
  - b/2
"""
    )
    assert Configurator.load_categories(cfg["categories"]) == [
        "a/1",
        "a/2",
        "b/1",
        "b/2",
    ]


def test_configurator_load_categories_without_content():
    # language=yml
    cfg = yaml.safe_load(
        """\
categories:  # none
"""
    )
    assert Configurator.load_categories(cfg["categories"]) == []


def test_configurator_load_accounts_ok():
    # language=yml
    cfg = yaml.safe_load(
        """\
accounts:
  sstark-BNP-CHQ:
    company: BNP
    type: CHQ
    id: '****0001'
    label: Sansa Stark - BNP Paribas (Compte de Chèque)
  sstark-BNP-LVA:
    company: BNP
    type: LVA
    id: '****0002'
    label: Sansa Stark - BNP Paribas (Livret A)
  astark-BRS-CHQ:
    company: Boursorama
    type: CHQ
    id: '****0001'
    label: Arya Stark - Boursorama (Compte de Chèque)
  astark-DGR-STK:
    company: Degiro
    type: STK
    id: '****0002'
    label: Arya Stark - Degiro (Stock)
  astark-OCT-CWL:
    company: October
    type: CWL
    id: 'astark'
    label: Arya Stark - October (CrowdLending)
"""
    )
    # results are sorted by lexicographical order on symbolic name
    assert Configurator.load_accounts(cfg["accounts"]) == [
        BoursoramaAccount("CHQ", "astark-BRS-CHQ", "****0001"),
        DegiroAccount("STK", "astark-DGR-STK", "****0002"),
        OctoberAccount("CWL", "astark-OCT-CWL", "astark"),
        BnpAccount("CHQ", "sstark-BNP-CHQ", "****0001"),
        BnpAccount("LVA", "sstark-BNP-LVA", "****0002"),
    ]


@patch("builtins.print")
def test_configurator_load_accounts_expr(mocked_print):
    # language=yml
    cfg = yaml.safe_load(
        """\
accounts:
  sstark-BNP-CHQ:
    company: BNP
    type: CHQ
    id: '****0001'
    label: BNP Paribas has its own naming convention, field 'expr' is not accepted.
    expr: 'patter1\\.csv'
  astark-BRS-CHQ:
    company: Boursorama
    type: CHQ
    id: '****0002'
    expr: 'patter2\\.csv'
    label: Boursorama has its own naming convention, field 'expr' is not accepted.
  astark-OCT-CWL:
    company: October
    type: CWL
    id: 'astark'
    expr: 'pattern3\\.csv'
    label: Arya Stark - October (CrowdLending)
"""
    )
    assert Configurator.load_accounts(cfg["accounts"]) == [
        BoursoramaAccount("CHQ", "astark-BRS-CHQ", "****0002"),
        OctoberAccount("CWL", "astark-OCT-CWL", "astark"),
        BnpAccount("CHQ", "sstark-BNP-CHQ", "****0001"),
    ]
    assert mocked_print.mock_calls == [
        call(
            "BNP Paribas has its own naming convention for downloaded files,"
            " you cannot overwrite it: expr='patter1\\\\.csv'"
        ),
        call(
            "Boursorama has its own naming convention for downloaded files,"
            " you cannot overwrite it: expr='patter2\\\\.csv'"
        ),
        call(
            "October has its own naming convention for downloaded files,"
            " you cannot overwrite it: expr='pattern3\\\\.csv'"
        ),
    ]


@patch("builtins.print")
def test_configurator_load_accounts_unknown_account(mocked_print):
    # language=yml
    cfg = yaml.safe_load(
        """\
accounts:
  rstark-???-CHQ:
    company: unknown
    type: CHQ
    id: '****0001'
"""
    )
    assert Configurator.load_accounts(cfg["accounts"]) == []
    assert mocked_print.mock_calls == [
        call(
            "Unknown account: rstark-???-CHQ,"
            " fields={'company': 'unknown', 'type': 'CHQ', 'id': '****0001'}"
        )
    ]


def test_configurator_autocomplete_with_content():
    # language=yml
    cfg = yaml.safe_load(
        """\
auto-complete:
  - expr: '.*FLUNCH.*'
    type: expense
    cat: food/restaurant
    regular: True
    desc: Optional description about this matching pattern. We go to Flunch regularly.
  - expr: '.*FOUJITA.*'
    type: expense
    cat: food/restaurant
    regular: False
    desc: Foujita is a nice Japanese restaurant near l'Opéra, we go there sometimes.
  - expr: '.*FRANPRIX 5584.*'
    type: expense
    cat: food/restaurant
    regular: True
"""
    )
    assert Configurator.load_autocomplete(cfg["auto-complete"]) == [
        (("expense", "food", "restaurant", True), r".*FLUNCH.*"),
        (("expense", "food", "restaurant", False), r".*FOUJITA.*"),
        (("expense", "food", "restaurant", True), r".*FRANPRIX 5584.*"),
    ]


def test_configurator_autocomplete_without_content():
    # language=yml
    cfg = yaml.safe_load(
        """\
    auto-complete: # none
    """
    )
    assert Configurator.load_autocomplete(cfg["auto-complete"]) == []


def test_configurator_parse_yaml(sample):
    cfg = Configurator.parse_yaml(sample / "finance-tools.yml")
    assert cfg.accounts == [
        BnpAccount("CHQ", "astark-BNP-CHQ", "00000002"),
        DegiroAccount("STK", "astark-DGR-STK", "00000003"),
        OctoberAccount("CWL", "astark-OCT-CWL", "astark"),
        BnpAccount("LVA", "sstark-BNP-LVA", "00000001"),
    ]
