# How to handle reimbursement?

Reimbursement is a special transaction, which is the act of compensating you for an out-of-pocket expense by giving you an amount of money equal to
what was spent. It always happens later than the initial expense. And it probably contains more than one expense.

To better categorize the expense, we introduced a new type of transaction in our system, called "virtual transaction". A virtual transaction never
happened in real, it is an accounting technique to split and clarify a reimbursement by declaring N additional virtual transactions, one per expense.
Therefore, you can compensate the amount spent for each category and better understand the expense distribution. In practice, there are two types
of virtual transaction, split transaction and reimbursed transaction:

* `_split/{split_date}: {initial_label}` for marking a transaction is split into reimbursement(s).
* `_reimbursed/{expense_date}: {expense_label}` for compensating a previous expense.

It sounds a bit abstract. Let's see a concrete example. I spent 50€ on October 2022 for my mobile (20€) and my internet (30€). In November 2022, my
employee gave me 50€ to reimburse them.

The declaration in Oct 2022:

```
2022-10-10, PRLV SEPA ORANGE SA MOBILE ...,-20.00,EUR,expense,util,telecom
2022-10-11, PRLV SEPA ORANGE SA INTERNET ...,-30.00,EUR,expense,util,telecom
```

The declaration in Nov 2022:

```diff
 2022-11-23,VIR SEPA RECU /DE ...,50.00,EUR,income,income,salary
+2022-11-23,_split/2022-11-23: VIR SEPA RECU /DE ...,-50.00,EUR,income,income,salary
+2022-11-23,_reimbursed/2022-10-10: PRLV SEPA ORANGE SA MOBILE ...,20.00,EUR,expense,util,telecom
+2022-11-23,_reimbursed/2022-10-11: PRLV SEPA ORANGE SA INTERNET ...,30.00,EUR,expense,util,telecom
```

Note:

1. The `_split/{date}` usually happens immediately after the reimbursement, and `date` refers the date when you received the reimbursement.
2. The amount of the split transaction should be the **opposite** to the reimbursement to compensate the it (because we need to split it into N items).
   In the example above, the opposite of "50€" is "-50€".
3. The `_reimbursed/{date}` refers to the date when the expense happened. In the example above, they happened in October 2022.
4. The amount of the reimbursed transactions should be the **opposite** to the initial expense.
5. The underscore `_` means that this is a private action, which is reserved for internal usage.

Recap: to successfully represent that you don't spent money on something (paid then reimbursed), we need the follow events:

1. The initial paiments (expense)
2. The reimbursement
3. Virtual split transaction to "cancel" the reimbursement
4. Virtual reimbursed transactions to categorize the reimbursement with fine granularity.

It's complex, but at least it's demystified 💰😌
