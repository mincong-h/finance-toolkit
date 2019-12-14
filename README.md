# Tools

`tx` — A script for collecting transaction files (CSV) from different companies:
BNP Paribas, Boursorama, Degiro, October.

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
