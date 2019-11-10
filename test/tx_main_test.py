"""Test the main() function from tx.py."""
import os
import sys
from pathlib import Path
from shutil import copyfile
from tempfile import TemporaryDirectory

import pytest
from src.tx import main

from .utils import get_test_file


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


@pytest.mark.parametrize('arg', ['-h, --help'])
def test_help(capsys, arg):
    sys.argv[1:] = [arg]
    # docopt will print the help message on STDOUT and exit
    with pytest.raises(SystemExit):
        main()
    captured = capsys.readouterr()
    assert captured.out == CURRENT_HELP


def test_invalid_argument():
    # Usage is printed on invalid argument
    sys.argv[1:] = ['unknown']
    with pytest.raises(SystemExit) as exc:
        main()
    assert str(exc.value) == CURRENT_USAGE


def test_config_path_from_envar_cat_without_prefix(capsys):
    file = get_test_file('../finance-tools.sample.yml')
    with TemporaryDirectory() as tmp:
        dst = Path(tmp) / 'finance-tools.yml'
        copyfile(file, dst)
        sys.argv[1:] = ['cat']
        os.environ['FINANCE_ROOT'] = tmp
        try:
            main()
        finally:
            os.environ.pop('FINANCE_ROOT')

    captured = capsys.readouterr()
    assert captured.out == """food/restaurant
food/supermarket
food/work
gouv/tax
"""


def test_cat_without_prefix(capsys):
    file = get_test_file('../finance-tools.sample.yml')
    with TemporaryDirectory() as tmp:
        dst = Path(tmp) / 'finance-tools.yml'
        copyfile(file, dst)
        sys.argv[1:] = ['--finance-root', tmp, 'cat']
        main()

    captured = capsys.readouterr()
    assert captured.out == """food/restaurant
food/supermarket
food/work
gouv/tax
"""


def test_categories_with_known_prefix(capsys):
    file = get_test_file('../finance-tools.sample.yml')
    with TemporaryDirectory() as tmp:
        dst = Path(tmp) / 'finance-tools.yml'
        copyfile(file, dst)
        sys.argv[1:] = ['--finance-root', tmp, 'categories', 'food']
        main()

    captured = capsys.readouterr()
    assert captured.out == """food/restaurant
food/supermarket
food/work
"""


def test_categories_with_empty_prefix(capsys):
    file = get_test_file('../finance-tools.sample.yml')
    with TemporaryDirectory() as tmp:
        dst = Path(tmp) / 'finance-tools.yml'
        copyfile(file, dst)
        sys.argv[1:] = ['--finance-root', tmp, 'categories', '']
        main()

    captured = capsys.readouterr()
    assert captured.out == """food/restaurant
food/supermarket
food/work
gouv/tax
"""


def test_categories_with_unknown_prefix(capsys):
    file = get_test_file('../finance-tools.sample.yml')
    with TemporaryDirectory() as tmp:
        dst = Path(tmp) / 'finance-tools.yml'
        copyfile(file, dst)
        sys.argv[1:] = ['--finance-root', tmp, 'categories', 'unknown']
        main()

    captured = capsys.readouterr()
    assert captured.out == ''


def test_merge():
    pytest.skip('TODO')


def test_move():
    pytest.skip('TODO')
