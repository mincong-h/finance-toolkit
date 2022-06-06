"""Finance Tools

Usage:
  finance-toolkit [options] (cat|categories) [<prefix>]
  finance-toolkit [options] merge
  finance-toolkit [options] move

Arguments:
  cat|categories   Print all categories, or categories starting with the given prefix.
  merge            Merge staging data.
  move             Import data from $HOME/Downloads directory.

Options:
  --finance-root FOLDER    Folder where the configuration file is stored (default: $HOME/finances).
  -X --debug               Enable debugging logs. Default: false.

"""

import os
from pathlib import Path

from docopt import docopt

from .tx import Configurator, merge, move

import logging


def main():
    args = docopt(__doc__)

    if "--debug" in args:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    home = Path.home()
    logger.debug(f"args={args}")
    logger.debug(f"home={home}")

    # Handle the finance folder
    finance_root = args["--finance-root"]
    if not finance_root:
        env = os.getenv("FINANCE_ROOT")
        logger.debug(
            f"User did not provide argument '--finance-root', check environment variable: FINANCE_ROOT={env}"  # noqa
        )
        if env:
            root = Path(env).expanduser()
        else:
            # Use the $HOME/finances folder by default
            root = home / "finances"
    else:
        logger.debug(f"User provided argument '--finance-root'")
        root = Path(finance_root).expanduser()

    logger.debug(f"finance-root={finance_root}")

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


if __name__ == "__main__":
    main()
