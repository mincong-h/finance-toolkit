"""\
Finance Toolkit, a command line interface (CLI) that helps you to better understand your personal
finance situation by collecting data from different companies.

Usage:
  finance-toolkit [options] (cat|categories) [<prefix>]
  finance-toolkit [options] convert
  finance-toolkit [options] convert-and-merge
  finance-toolkit [options] merge
  finance-toolkit [options] move

Arguments:
  cat|categories      Print all categories, or categories starting with the given prefix.
  move                Import data from $HOME/Downloads directory.
  convert             Convert data from one currency to another based on the exchange rates. The
                      base currency is euro (EUR) and cannot be changed for now.
  merge               Merge staging data.
  convert-and-merge   Running the 'convert' and 'merge' commands sequentially.

Options:
  --finance-root FOLDER    Folder where the configuration file is stored (default: $HOME/finances).
  -X --debug               Enable debugging logs. Default: false.

"""

import os
from pathlib import Path

from docopt import docopt

from .tx import Configurator, merge, move, convert

import logging


def main():
    args = docopt(__doc__)

    if "--debug" in args:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    home = Path.home()
    logging.debug(f"args={args}")
    logging.debug(f"home={home}")

    # Handle the finance folder
    finance_root = args["--finance-root"]
    if not finance_root:
        env = os.getenv("FINANCE_ROOT")
        logging.debug(
            f"User did not provide argument '--finance-root', check environment variable: FINANCE_ROOT={env}"  # noqa
        )
        if env:
            root = Path(env).expanduser()
        else:
            # Use the $HOME/finances folder by default
            root = home / "finances"
    else:
        logging.debug(f"User provided argument '--finance-root'")
        root = Path(finance_root).expanduser()

    logging.debug(f"finance-root={finance_root}")

    cfg_path = root / "finance-tools.yml"
    cfg = Configurator.load(cfg_path)

    if args["cat"] or args["categories"]:
        prefix = args["<prefix>"] or ""
        for c in cfg.categories(lambda s: s.startswith(prefix)):
            print(c)
    elif args["merge"]:
        merge(cfg)
    elif args["move"]:
        move(cfg)
    elif args["convert"]:
        convert(cfg)
    elif args["cm"] or args["convert-and-merge"]:
        convert(cfg)
        merge(cfg)


if __name__ == "__main__":
    main()
