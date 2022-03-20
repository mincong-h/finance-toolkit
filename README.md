# Finance Toolkit

Finance Toolkit helps you to understand your personal finance by collecting
transactions from different companies:

- BNP Paribas (https://mabanque.bnpparibas)
- Boursorama (https://www.boursorama.com)
- Degiro (https://www.degiro.com)
- Fortuneo (https://www.fortuneo.fr)
- October (https://october.eu)
- Revolut (https://www.revolut.com)

## Install

### Install With Docker

The Docker images are built by CI (GitHub Actions), to use the script with
Docker, you just need to run the commands directly with wrapper script
`docker-finance.sh`:

```sh
# Run Commands, such as:
./bin/docker-finance.sh move
./bin/docker-finance.sh merge
```

### Install Without Docker (Deprecated)

Ensure module `finance-tookit` is ready to be used:

```sh
# clone the project
git clone https://github.com/mincong-h/finance-toolkit.git

# go to project
cd finance-toolkit

# Create a new directory called "venv" as virtual environment
python3 -m venv venv

# Enable virtual environment
source venv/bin/activate

# Install requirements for tests
pip3 install -r requirements-tests.txt

# Install Finance Toolkit and its dependencies (the finance-toolkit executable will be created)
python setup.py install

# Disable virtual environment
deactivate
```

Create a new directory for storing your finance data. It's recommended to store
them into a Git repository so that data are versioned and changes can be
reverted in any case. For example, to store finance data in `~/finance-data`,
you need to edit `.bash_profile`, `.bashrc` or similar file to include the
following lines:

```sh
export FINANCE_ROOT="${HOME}/finance-data"
export PYTHONPATH="${HOME}/github/finance-toolkit/"
```

Create a new configuration file for configuring your accounts (do not forget to
update the sample to adapt your needs):

```sh
cp /path/to/finance-toolkit/finance-tools.sample.yml "${FINANCE_ROOT}/finance-tools.yml"
```

Modify the download directory inside the configuration file `finance-tools.yml`:

```yml
# Download Directory
# ------------------
# Download directory is the place where finance files are stored at the first
# place. Usually, this is download directory of your OS or your browser.
#   - macOS: ~/Downloads
#
download-dir: ~/Downloads
```

Download files from your banks or other supported companies. Then collect data
into your finance data directory by performing a `tx-move` command:

```
$ tx move
$$$ Summary $$$
---------------
2 files copied.
---------------
Sources:
- /path/to/download/E1851234.csv
- /path/to/download/HistoriqueOperations_12345_du_14_01_2019_au_14_12_2019.csv
Targets:
- /path/to/finance/2019-04/2019-04.astark-FTN-CHQ.csv
- /path/to/finance/2019-06/2019-06.credit-BNP-P15.csv
- /path/to/finance/2019-12/2019-12.astark-FTN-CHQ.csv
- /path/to/finance/balance.credit-BNP-P15.csv
Finished.
```

## Hacking

Install pre-commit for day-to-day development. It will ensure a good quality code before commiting anything.

```bash
python -m pip install -U --user pre-commit
pre-commit install
```

## Testing

Run unit tests (at root level of the project directory):

```bash
python -m pytest
```

## Revolut

### Download CSV File

https://community.revolut.com/t/data-export/76631

Follow the steps below to download CSV files:

* Open mobile application
* Go to "Accounts" tab
* Click "..." (more) and select "Statement"
* Enter parameters for export:
  - Format: Excel (actually CSV will be sent)
  - Start on: the start month
  - Ending on: the end month
* Click "Get statement" and you will be redirected to your email app. Finish the remaining steps there.
* An email will be sent with CSV attached
* Download the CSV file from that email
