# Ledger Api

Ledger APIs expose account and entry visibility. They should not become arbitrary money mutation endpoints.

## Design questions

- Is the action idempotent?
- Does it affect ledger truth?
- Does it require approval?
- Can it affect a closed period?
- Does it emit an event?
- Does it need evidence?

Back to [README](../../README.md).
