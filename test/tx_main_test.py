"""Test the main() function from tx.py."""
import os
import sys
from pathlib import Path

import pytest
from src.tx import main


CURRENT_USAGE = """Usage:
  tx.py [options] (cat|categories) [<prefix>]
  tx.py [options] merge
  tx.py [options] move"""

CURRENT_HELP = f"""Finance Tools

{CURRENT_USAGE}

Arguments:
  cat|categories   Print all categories, or categories starting with the given prefix.
  merge            Merge staging data.
  move             Import data from $HOME/Downloads directory.

Options:
  --finance-root FOLDER    Folder where the configuration file is stored (default: $HOME/finances).
"""


def test_no_argument():
    # No arguments, the usage is printed
    with pytest.raises(SystemExit) as exc:
        main()
    assert str(exc.value) == CURRENT_USAGE


@pytest.mark.parametrize("arg", ["-h, --help"])
def test_help(capsys, arg):
    sys.argv[1:] = [arg]
    # docopt will print the help message on STDOUT and exit
    with pytest.raises(SystemExit):
        main()
    captured = capsys.readouterr()
    assert captured.out == CURRENT_HELP


def test_invalid_argument():
    # Usage is printed on invalid argument
    sys.argv[1:] = ["unknown"]
    with pytest.raises(SystemExit) as exc:
        main()
    assert str(exc.value) == CURRENT_USAGE


def test_config_path_from_envar_cat_without_prefix(capsys, sample):
    sys.argv[1:] = ["cat"]
    os.environ["FINANCE_ROOT"] = str(sample)
    try:
        main()
    finally:
        os.environ.pop("FINANCE_ROOT")

    captured = capsys.readouterr()
    assert (
        captured.out
        == """food/restaurant
food/supermarket
food/work
gouv/tax
"""
    )


def test_cat_without_prefix(capsys, sample):
    sys.argv[1:] = ["--finance-root", sample, "cat"]
    main()

    captured = capsys.readouterr()
    assert (
        captured.out
        == """food/restaurant
food/supermarket
food/work
gouv/tax
"""
    )


def test_categories_with_known_prefix(capsys, sample):
    sys.argv[1:] = ["--finance-root", sample, "categories", "food"]
    main()

    captured = capsys.readouterr()
    assert (
        captured.out
        == """food/restaurant
food/supermarket
food/work
"""
    )


def test_categories_with_empty_prefix(capsys, sample):
    sys.argv[1:] = ["--finance-root", sample, "categories", ""]
    main()

    captured = capsys.readouterr()
    assert (
        captured.out
        == """food/restaurant
food/supermarket
food/work
gouv/tax
"""
    )


def test_categories_with_unknown_prefix(capsys, sample):
    sys.argv[1:] = ["--finance-root", sample, "categories", "unknown"]
    main()

    captured = capsys.readouterr()
    assert captured.out == ""


def test_merge(tmpdir, capsys):
    d = Path(tmpdir.strpath)
    root_dir = d / "finance"
    download_dir = d / "download"

    root_dir.mkdir()
    download_dir.mkdir()

    # Given configuration
    with (root_dir / "finance-tools.yml").open("w") as f:
        # language=yml
        f.write(
            f"""\
accounts:
  userA-BNP-CHQ:
    company: BNP
    type: CHQ
    id: '****0001'
  userB-BRS-CHQ:
    company: Boursorama
    type: CHQ
    id: '****0002'

categories:
  - food/restaurant

auto-complete:

download-dir: {download_dir}
"""
        )

    # And two staging files to be merged
    (root_dir / "2019-08").mkdir()
    tx_bnp = root_dir / "2019-08" / "2019-08.userA-BNP-CHQ.csv"
    tx_brs = root_dir / "2019-08" / "2019-08.userB-BRS-CHQ.csv"
    tx_bnp.write_text(
        """\
Date,bnpMainCategory,bnpSubCategory,Label,Amount,Type,mainCategory,subCategory,IsRegular
2019-08-01,m,s,myLabel,-10.0,expense,food,restaurant,False
"""
    )
    tx_brs.write_text(
        """\
dateOp,dateVal,Label,brsMainCategory,brsSubCategory,supplierFound,Amount,Type,mainCategory,subCategory,IsRegular
2019-08-02,2019-08-02,myLabel,m,s,supplier,-11.0,transfer,,,False
"""
    )

    # When performing `merge` command to merge these files
    sys.argv[1:] = ["--finance-root", str(root_dir), "merge"]
    main()

    # Then the files are merged correctly
    tx_merged = root_dir / "total.csv"
    assert (
        tx_merged.read_text()
        == """\
Date,Account,ShortType,LongType,Label,Amount,Type,Category,SubCategory,IsRegular
2019-08-01,userA-BNP-CHQ,m,s,myLabel,-10.0,expense,food,restaurant,False
2019-08-02,userB-BRS-CHQ,m,s,myLabel,-11.0,transfer,,,False
"""
    )
    # And a summary is printed to standard output (stdout)
    captured = capsys.readouterr()
    assert (
        captured.out
        == f"""\
Merge done
"""
    )
    assert captured.err == ""


def test_move(tmpdir, capsys):
    d = Path(tmpdir.strpath)
    root_dir = d / "finance"
    download_dir = d / "download"

    root_dir.mkdir()
    download_dir.mkdir()

    # Given configuration
    with (root_dir / "finance-tools.yml").open("w") as f:
        # language=yml
        f.write(
            f"""\
accounts:
  credit-BNP-P15:
    company: BNP
    type: CDI  # credit
    id: '****1234'

categories:

auto-complete:

download-dir: {download_dir}
"""
        )

    # And a CSV file downloaded from BNP website
    csv = download_dir / "E1851234.csv"
    csv.write_text(
        """\
Crédit immobilier;Crédit immobilier;****1234;03/07/2019;;-123 456,78
05/06/2019;;; AMORTISSEMENT PRET 1234;67,97
""",
        encoding="ISO-8859-1",
    )

    # When performing `move` command to copy the file into finance root
    sys.argv[1:] = ["--finance-root", str(root_dir), "move"]
    main()

    # Then the data is copied
    assert (root_dir / "2019-06" / "2019-06.credit-BNP-P15.csv").exists()
    assert (root_dir / "balance.credit-BNP-P15.csv").exists()
    assert csv.exists()
    # And a summary is printed to standard output (stdout)
    captured = capsys.readouterr()
    assert (
        captured.out
        == f"""\
$$$ Summary $$$
---------------
1 files copied.
---------------
Sources:
- {tmpdir.strpath}/download/E1851234.csv
Targets:
- {tmpdir.strpath}/finance/2019-06/2019-06.credit-BNP-P15.csv
- {tmpdir.strpath}/finance/balance.credit-BNP-P15.csv
Finished.
"""
    )
    assert captured.err == ""
