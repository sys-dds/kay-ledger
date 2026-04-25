# Domain Boundaries

Kay-Ledger is split by business capability.

## Boundary map

| Boundary | Responsibility |
|---|---|
| Tenancy | Workspace ownership |
| Actor / Permission | Roles, scopes, operator boundary |
| Catalog | Offerings, pricing, availability |
| Booking | Reservations, holds, expiry |
| Idempotency | Duplicate-safe mutations |
| Ledger | Accounts, entries, postings |
| Payment | Intents and payment lifecycle |
| Settlement | Escrow and payable balances |
| Payout | External disbursement |
| Refund / Dispute | Compensating flows |
| Subscription | Recurring billing |
| Async | Outbox, inbox, replay |
| Provider | Callback evidence |
| Reconciliation | Mismatch detection and repair |
| Investigation | Searchable operator facts |
| Reporting | Statements and exports |
| Temporal | Durable workflows |
| Region | Ownership and failover |
| Approval | Maker-checker |
| Evidence | Audit packs |
| Merchant Events | External delivery truth |

Back to [README](../../README.md).
