# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build and Development Commands

```bash
# Create and activate virtual environment
python3 -m venv venv
source venv/bin/activate

# Install the package and dependencies
python setup.py install
pip3 install -r requirements-tests.txt

# Run tests
python -m pytest

# Run a single test file
python -m pytest test/test_bnp.py

# Run a specific test
python -m pytest test/test_bnp.py::test_function_name -vv

# Run tests with coverage
coverage run -m pytest -vv --strict
coverage report

# Lint with flake8
flake8

# Run with Docker (local build)
FTK_DOCKER_MODE=local bin/docker-finance.sh $cmd

# Build Docker image
bin/docker-build.sh
```

## Pre-commit Hooks

The project uses pre-commit with black (formatting), flake8 (linting), and standard hooks (trailing whitespace, end-of-file fixer, etc.). Install with:
```bash
python -m pip install -U --user pre-commit
pre-commit install
```

## Architecture

### Core Concepts

**Pipeline Pattern**: The codebase uses a pipeline architecture for processing bank data:
- `Pipeline` (abstract base) in `finance_toolkit/pipeline.py`
- `TransactionPipeline` - processes transaction CSV files, outputs monthly transaction files
- `BalancePipeline` - processes balance data from bank statements
- `PipelineFactory` in `finance_toolkit/pipeline_factory.py` - creates appropriate pipelines based on account type

**Bank-Specific Implementations**: Each supported bank has its own module with specialized Account and Pipeline classes:
- `bnp.py` - BNP Paribas (BnpAccount, BnpTransactionPipeline, BnpBalancePipeline)
- `boursorama.py` - Boursorama
- `fortuneo.py` - Fortuneo
- `revolut.py` - Revolut
- `caisse_epargne.py` - Caisse d'Epargne

**Adding a New Bank**: Create a new module following the pattern in existing bank modules:
1. Create `XxxAccount(Account)` class with file pattern matching regex
2. Create `XxxTransactionPipeline(TransactionPipeline)` implementing `read_new_transactions()`
3. Create `XxxBalancePipeline(BalancePipeline)` implementing `read_new_balances()` if applicable
4. Register in `PipelineFactory` and `Configurator.load_accounts()` in `tx.py`

### Key Files

- `finance_toolkit/__main__.py` - CLI entry point using docopt
- `finance_toolkit/tx.py` - `Configurator` class (parses YAML config), top-level commands (`move`, `merge`, `convert`)
- `finance_toolkit/models.py` - `Configuration`, `TxType`, `TxCompletion` (auto-complete rules), `Summary`
- `finance_toolkit/account.py` - Base `Account` class and generic account types (DegiroAccount, OctoberAccount)
- `finance_toolkit/pipeline.py` - Base pipeline classes and `AccountParser`
- `finance_toolkit/exchange_rate.py` - Currency conversion pipelines

### Data Flow

1. **move**: Downloads in `$DOWNLOAD_DIR` -> matched by account patterns -> processed by transaction/balance pipelines -> output to `$FINANCE_ROOT/{YYYY-MM}/` (transactions) and `$FINANCE_ROOT/balance.*.csv` (balances)
2. **convert**: Converts non-EUR balances to EUR using exchange rates
3. **merge**: Aggregates all monthly transaction files into `total.csv` and balance files into `balance.csv`

### Configuration

User configuration is in `$FINANCE_ROOT/finance-tools.yml` (see `finance-tools.sample.yml`). Key sections:
- `accounts`: Define bank accounts with company, id, type, currency
- `categories`: Valid expense categories (format: `main/sub`)
- `auto-complete`: Regex patterns to auto-categorize transactions
- `exchange-rate`: Watched currencies for conversion

### Testing

Tests are in `test/` directory. The `conftest.py` provides fixtures:
- `location` - path to test folder
- `sample` - temporary config folder with sample config
- `cfg` - Configuration instance with temporary directories
