# Finance Toolkit

Finance Toolkit is a command line interface (CLI) that helps you to better understand your personal finance situation by collecting
data from different companies:

| Company | Transaction | Balance | Description |
| :---: | :---: | :---: | :--- |
| [BNP Paribas](https://mabanque.bnpparibas) | Supported | Supported | You can download the CSV files from BNP Paribas' website and use Finance Toolkit to integrate the data. |
| [Boursorama](https://www.boursorama.com) | Supported | Supported | You can download the CSV files from Boursorama's website and use Finance Toolkit to integrate the data. |
| [Revolut](https://www.revolut.com) | Supported | Supported | You can download the CSV files from Revolut's website and use Finance Toolkit to integrate the data. |
| [Fortuneo](https://www.fortuneo.fr) | Supported | - | You can download the CSV files from Fortuneo's website and use Finance Toolkit to integrate the data. However, Fortuneo does not provide an account statement so Finance Toolkit does not know the balance of your accounts. |
| Other | - | Partially supported | Declare your account and enter the balance manually in Finance Toolkit. We use this approach for companies like [October](https://october.eu), [Degiro](https://www.degiro.com), and [E\*Trade](https://us.etrade.com/). |

:warning: Currently Finance Toolkit is still in alpha phase and cannot be used easily. You need to
clone the Git repository build from source to make it work. Please contact _mincong.h \[ at \] gmail.com_
if you want to try.

## Usage

```
$ finance-toolkit --help
Finance Toolkit, a command line interface (CLI) that helps you to better understand your personal
finance situation by collecting data from different companies.

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
```

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

## Currency

Currently, Finance Toolkit supports multiple currencies, such as euro (EUR) and US dollar (USD).
Currency is defined at account level -- each account can only have one single currency. In the
configuration file (`finance-tools.yml`), specify the currency field. For example, for user "Arya
Stark (astark)", her BNP account "Compte de Chèque (CHQ)" in euro should be declared as follows:

```yaml
accounts:
  astark-BNP-CHQ:
    company: BNP
    id: '****1234'
    category: my-category
    currency: EUR
    type: CHQ
    tags: [ ... ]
```

You can find out more world currency symbols here: <https://www.xe.com/en/symbols.php>

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

### Revolut Account Statement Format

File name format:

```
account-statement_{START_DATE}_{END_DATE}_undefined-undefined_{ACCOUNT_ID}.csv
```

It consists of 3 parameters: the start date (format: yyyy-MM-dd), the end date (format: yyyy-MM-dd),
and the account id in 6 hexadecimal digits.

Example:

```
account-statement_2021-01-01_2022-05-27_undefined-undefined_abc123.csv
```

Delimiter: `,`

Columns:

| Column         | Type              | Comment                         |
|:---------------|:------------------|:--------------------------------|
| Type           | String            | TOPUP, EXCHANGE, TRANSFER       |
| Product        | String            | Current                         |
| Started Date   | Date              | Format: `yyyy-MM-dd' 'hh:mm:ss` |
| Completed Date | Date              | Format: `yyyy-MM-dd' 'hh:mm:ss` |
| Description    | String            | Description of the statement    |
| Amount         | Float             |                                 |
| Fee            | Fee               |                                 |
| Currency       | String            | USD, EUR, ...                   |
| State          | String            | COMPLETED                       |
| Balance        | Optional\[Float\] | Balance of the account or empty |

### Download CSV File

https://www.revolut.com/en-US/help/my-accounts/managing-my-account/viewing-my-account-statements

Follow the steps below to download CSV files:

* Open mobile application
* Go to "Accounts" tab
* Click "..." (more) and select "Statement"
* Enter parameters for export:
  - Format: Excel (actually CSV will be sent)
  - Start on: the start month
  - Ending on: the end month
* Click "Get statement" and wait until the generation is complete
* Download the CSV file

Note that:

* You need to do this for each account. Different currency, such as EUR and USD, are considered as
  two different accounts.
* For commodities (such as gold), the account statement is not supported.
