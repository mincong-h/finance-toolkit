from unittest.mock import patch, call

from pandas.testing import assert_frame_equal

from tx import tx
from tx.tx import *


# ---------- Top Level Functions ----------


def test_read_bnp_tx_ok(cfg):
    cfg.category_set.add("food/workfood")

    csv = cfg.root_dir / "2019-03.mhuang-CHQ.csv"
    csv.write_text(
        """\
Date,Label,Amount,Type,MainCategory,SubCategory,IsRegular
2018-04-30,DU 270418 MC DONALDS PARIS 18 CARTE 4974,-4.95,expense,food,workfood,True
"""
    )
    actual_df = tx.read_transactions(csv, cfg)
    expected_df = pd.DataFrame(
        {
            "Date": pd.Timestamp("2018-04-30"),
            "Label": "DU 270418 MC DONALDS PARIS 18 CARTE 4974",
            "Amount": -4.95,
            "Type": "expense",
            "MainCategory": "food",
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
Date,Label,Amount,Type,MainCategory,SubCategory,IsRegular
2018-04-30,myLabel,-1.0,expense,food,restaurant,True
2018-04-30,myLabel,-2.0,expense,,,True
2018-04-30,myLabel,-3.0,expense,food,,True
2018-04-30,myLabel,-4.0,expense,,restaurant,True
"""
    )
    actual_df = tx.read_transactions(csv, cfg)
    expected_df = pd.DataFrame(
        {
            "Date": pd.Timestamp("2018-04-30"),
            "Label": "myLabel",
            "Amount": -1.0,
            "Type": "expense",
            "MainCategory": "food",
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
Date,Label,Amount,Type,MainCategory,SubCategory,IsRegular
2019-06-26,CARTE 25/06/19 93 ROYAL PLAISANC CB*1234,-20.1,expense,food,restaurant,True
"""
    )
    actual_df = tx.read_transactions(csv, cfg)
    expected_df = pd.DataFrame(
        {
            "Date": pd.Timestamp("2019-06-26"),
            "Label": "CARTE 25/06/19 93 ROYAL PLAISANC CB*1234",
            "Amount": -20.1,
            "Type": "expense",
            "MainCategory": "food",
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
Date,Label,Amount,Type,MainCategory,SubCategory,IsRegular
2019-06-26,myLabel,-1.0,expense,food,restaurant,True
2019-06-26,myLabel,-2.0,expense,,,True
2019-06-26,myLabel,-3.0,expense,food,,True
2019-06-26,myLabel,-4.0,expense,,restaurant,True
"""
    )
    actual_df = tx.read_transactions(csv, cfg)
    expected_df = pd.DataFrame(
        {
            "Date": pd.Timestamp("2019-06-26"),
            "Label": "myLabel",
            "Amount": -1.0,
            "Type": "expense",
            "MainCategory": "food",
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
                "Label": "aLabel",
                "Amount": -4.95,
                "Type": "X",
                "MainCategory": "food",
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
                "Label": "aLabel",
                "Amount": -4.95,
                "Type": "expense",
                "MainCategory": "X",
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
                "Label": "aLabel",
                "Amount": -4.95,
                "Type": "expense",
                "MainCategory": "food",
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
                "Label": "aLabel",
                "Amount": -4.95,
                "Type": "expense",
                "MainCategory": "food",
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
                "Label": "aLabel",
                "Amount": -4.95,
                "Type": "expense",
                "MainCategory": "food",
                "SubCategory": "workfood",
                "IsRegular": True,
            }
        ),
        cfg,
    )
    assert "" == err5


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
  astark-FTN-CHQ:
    company: Fortuneo
    type: CHQ
    id: '12345'
    expr: 'patterFortuneo\\.csv'
    label: Fortuneo has its own naming convention, field 'expr' is not accepted.
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
        FortuneoAccount("CHQ", "astark-FTN-CHQ", "12345"),
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
            "Fortuneo has its own naming convention for downloaded files,"
            " you cannot overwrite it: expr='patterFortuneo\\\\.csv'"
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
        FortuneoAccount("CHQ", "astark-FTN-CHQ", "12345"),
        OctoberAccount("CWL", "astark-OCT-CWL", "astark"),
        BnpAccount("LVA", "sstark-BNP-LVA", "00000001"),
    ]
