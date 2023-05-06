# Delayed Transactions

_How to declare delayed transactions in the Finance Toolkit?_

Some transactions can be delayed and impact the reporting of our finance system. For example, the salary of April, which was supposed to be paid at the end of April, but it does not arrive until the beginning of May. This is called a delayed transaction. It affects the reporting, especially the analysis of the balance between our incomes and our expenses. To reduce its impact, we introduce a new concept called “delayed translation”.

“Delayed translation” is a pair of virtual transactions: one virtual transaction that cancels the actual transaction and the other virtual transaction that reports on the date on which we wish the transaction could have happened.
* `_cancel/delay/{actual_tx_date}: {actual_tx_label}`. This is the 1st virtual transaction which cancels (_cancel) the actual transaction. The amount of the transaction should be the opposite of the initial amount stated in the actual transaction. Other metadata should remain the same.
* `_delay/{actual_tx_date}: {actual_tx_label}`. This is the 2nd transaction which creates a virtual transaction on the date on which we wish the transaction we wish the translation could have happened.

It sounds a bit abstract. Let's see a concrete example. I received 2000€ on the 2 May, 2023 for the salary of April. This is frustrating because the income of April is reduced by 50% for the family (only one person’s salary is reported) and the income of May is increased by 50% (3 salaries are reported, one from April, two from May). Therefore, it makes the reporting confusing. To mitigate that, we declare delayed transactions.

The initial declaration of May 2023:

```
2023-05-02,My Salary for Apr 2023,2000,EUR,income,income,salary
```

The new declarations of April 2023:

```diff
+ 2023-04-30,_delay/2023-05-02: My Salary for Apr 2023,2000,EUR,income,income,salary
```

The new declarations of May 2023:

```diff
  2023-05-02,My Salary for Apr 2023,2000,EUR,income,income,salary
+ 2023-05-02,_cancel/delay/2023-05-02: My Salary for Apr 2023,-2000,EUR,income,income,salary
```

Thanks to the virtual transitions declared above:

1. The salary of April is now part of the income of April. And we also know on which exact date it is paid.
2. The salary of April received in May is canceled.
3. We preserve the actual transaction without deleting it. It facilitates the understanding and keep of the finance toolkit to merge new transactions and handle deduplication without any additional overhead.

Overall, this is an accounting technique to help report the data correctly.

## See Also

- [Reimbursement](reimbursement.md)
