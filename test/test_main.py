"""Test the main() function."""
import os
from pathlib import Path

import pytest
import sys

from finance_toolkit.__main__ import main

CURRENT_USAGE = """Usage:
  finance-toolkit [options] (cat|categories) [<prefix>]
  finance-toolkit [options] merge
  finance-toolkit [options] move"""

CURRENT_HELP = f"""\
Finance Toolkit, a command line interface (CLI) that helps you to better understand your personal
finance situation by collecting data from different companies.

{CURRENT_USAGE}

Arguments:
  cat|categories   Print all categories, or categories starting with the given prefix.
  merge            Merge staging data.
  move             Import data from $HOME/Downloads directory.

Options:
  --finance-root FOLDER    Folder where the configuration file is stored (default: $HOME/finances).
  -X --debug               Enable debugging logs. Default: false.
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

categories_to_rename:
  tax/income-tax: gouv/tax
  tax/property-tax: gouv/tax

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
Date,Label,Amount,Type,MainCategory,SubCategory,IsRegular
2019-08-01,myLabel,-10.0,expense,food,restaurant,False
"""
    )
    tx_brs.write_text(
        """\
Date,Label,Amount,Type,MainCategory,SubCategory
2019-08-02,myLabel,-11.0,transfer,,
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
Date,Month,Account,Label,Amount,Type,MainCategory,SubCategory
2019-08-01,2019-08,userA-BNP-CHQ,myLabel,-10.0,expense,food,restaurant
2019-08-02,2019-08,userB-BRS-CHQ,myLabel,-11.0,transfer,,
"""
    )
    # And a summary is printed to standard output (stdout)
    captured = capsys.readouterr()
    assert (
        captured.out
        == """\
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
  astark-FTN-CHQ:
    company: Fortuneo
    type: CHQ
    id: '12345'
    label: Arya Stark - Fortuneo (Compte de Chèque)

categories:

categories_to_rename:

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
    download_fortuneo = (
        download_dir / "HistoriqueOperations_12345_du_14_01_2019_au_14_12_2019.csv"
    )
    download_fortuneo.write_text(
        """\
Date opération;Date valeur;libellé;Débit;Crédit;
13/12/2019;13/12/2019;CARTE 12/12 FNAC METZ;-6,4;
13/12/2019;13/12/2019;CARTE 12/12 BRIOCHE DOREE METZ;-10,9;
13/12/2019;13/12/2019;CARTE 12/12 AMAZON EU SARL PAYLI2090401/;-45,59;
12/12/2019;12/12/2019;CARTE 11/12 LECLERC MARLY;-15,75;
30/04/2019;30/04/2019;VIR MALAKOFF MEDERIC PREVOYANCE;; 45;
"""
    )

    # When performing `move` command to copy the file into finance root
    sys.argv[1:] = ["--finance-root", str(root_dir), "move"]
    main()

    # Then the data is copied
    assert (root_dir / "2019-06" / "2019-06.credit-BNP-P15.csv").exists()
    assert (root_dir / "2019-04" / "2019-04.astark-FTN-CHQ.csv").exists()
    assert (root_dir / "2019-12" / "2019-12.astark-FTN-CHQ.csv").exists()
    assert (root_dir / "balance.credit-BNP-P15.csv").exists()
    assert csv.exists()
    assert download_fortuneo.exists()
    # And a summary is printed to standard output (stdout)
    captured = capsys.readouterr()
    assert (
        captured.out
        == f"""\
$$$ Summary $$$
---------------
2 files copied.
---------------
Sources:
- {tmpdir.strpath}/download/E1851234.csv
- {tmpdir.strpath}/download/HistoriqueOperations_12345_du_14_01_2019_au_14_12_2019.csv
Targets:
- {tmpdir.strpath}/finance/2019-04/2019-04.astark-FTN-CHQ.csv
- {tmpdir.strpath}/finance/2019-06/2019-06.credit-BNP-P15.csv
- {tmpdir.strpath}/finance/2019-12/2019-12.astark-FTN-CHQ.csv
- {tmpdir.strpath}/finance/balance.credit-BNP-P15.csv
Finished.
"""
    )
    assert captured.err == ""
