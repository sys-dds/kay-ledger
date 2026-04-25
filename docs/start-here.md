# Start Here

Kay-Ledger is easiest to understand as a progression from normal marketplace backend concerns into finance-grade correctness.

## First mental model

A normal marketplace asks:

> Can the buyer book and pay?

Kay-Ledger asks:

> Can the system prove what happened to the money after retries, provider disagreement, operator recovery, and financial close?

## High-level flow

1. Tenants, actors, and permissions define ownership.
2. Catalog and booking create business demand.
3. Payment intents model money collection.
4. Ledger postings represent financial truth.
5. Settlement creates payable balances.
6. Payouts move funds externally.
7. Refunds, disputes, and reversals compensate prior movements.
8. Provider callbacks are stored as evidence.
9. Reconciliation compares internal and provider truth.
10. Operators investigate and recover through controlled workflows.
11. Financial close locks accounting windows.
12. Evidence packs prove final state.

Back to [README](../README.md).
