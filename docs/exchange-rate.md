# Exchange Rate

The exchange rate information is downloaded from the Bank of France. The file format is

```
Webstat_Export_{YYYYMMDD}.csv
```

where `YYYY` is the year in 4 digits, the `MM` is the month in 2 digits, and `DD` is the day of the month in 2 digits. Finance Toolkit performs cleanup for the data internally and the final results are stored in CSV in the following structure:

```
Date,USD,CNY
2024-01-05,1.0921,7.813
2024-01-04,1.0953,7.833
2024-01-03,1.0919,7.8057
2024-01-02,1.0956,7.8264
2024-01-01,,
2023-12-31,,
2023-12-30,,
2023-12-29,1.105,7.8509
2023-12-28,1.1114,7.8941
```

where:

* The "Date" column is the date of the exchange rates. Note that the accuracy stops at the date level, we don't have precision on finer levels (hours, minutes, seconds).
* Other columns represent the exchange rate between the base currency Euro and the target currency `$CODE`, for example, the U.S. Dollar (USD) and the Chinese Yuan (CNY).
* If the exchange rate is missing on some dates, we use the forward-filling technique, meaning that we take the latest valid value for the current date.

## Operations

### Convert Balances

Converting the balances of all foreign accounts from a foreign currency to the base concurrency (EUR) is done via the `finance-toolkit convert` command.

```
(venv) ➜  finance-toolkit git:(exchange-rate|u=) finance-toolkit convert
$$$ Summary $$$
---------------
2 files done (action: convert).
---------------
Sources:
- /finance-data/balance.sstark-BNP-LVA.USD.csv
- /finance-data/balance.astark-BNP-CHQ.USD.csv
Targets:
- /Users/minconghuang/github/jimi-data/finance-data/balance.sstark-BNP-LVA.EUR.csv
- /Users/minconghuang/github/jimi-data/finance-data/balance.astark-BNP-CHQ.EUR.csv
Finished.
```

You can also combine the "convert" and "merge" commands into a single command via the command `finance-toolkit convert-and-merge` (available since v1.4.0).

### Convert Transactions

This is not yet supported.

### Update the Exchange-Rate Table

Updating the exchange-rate table `${FINANCE_DATA}/exchange-rate.csv` is done in two steps:
  1. Download the CSV file from the website of the Bank of France **manually**
  2. Run the command `finance-toolkit move` which scans the download directory and adds the data into the finance database.

### Watch a New Currency

Update the finance configuration `${FINANCE_DATA}/finance-tools.yml` under the `exchange-rate` section. Add a new currency code there.

## Reference

- <https://en.wikipedia.org/wiki/ISO_4217>
