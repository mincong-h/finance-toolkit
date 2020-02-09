"""Finance Tools

Usage:
  tx.py [options] (cat|categories) [<prefix>]
  tx.py [options] merge
  tx.py [options] move

Arguments:
  cat|categories   Print all categories, or categories starting with the given prefix.
  merge            Merge staging data.
  move             Import data from $HOME/Downloads directory.

Options:
  --finance-root FOLDER    Folder where the configuration file is stored (default: $HOME/finances).

"""

import os
from pathlib import Path

from docopt import docopt

from .tx import Configurator, merge, move


def main():
    args = docopt(__doc__)

    home = Path.home()

    # Handle the finance folder
    finance_root = args["--finance-root"]
    if not finance_root:
        # Check the envar
        env = os.getenv("FINANCE_ROOT")
        if env:
            root = Path(env).expanduser()
        else:
            # Use the $HOME/finances folder by default
            root = home / "finances"
    else:
        # Use the provided folder
        root = Path(finance_root).expanduser()

    cfg_path = root / "finance-tools.yml"
    cfg = Configurator.parse_yaml(cfg_path)

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
