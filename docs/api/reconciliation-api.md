# Reconciliation Api

Reconciliation APIs import provider truth, run comparison, classify mismatches, and trigger repair.

## Design questions

- Is the action idempotent?
- Does it affect ledger truth?
- Does it require approval?
- Can it affect a closed period?
- Does it emit an event?
- Does it need evidence?

Back to [README](../../README.md).
