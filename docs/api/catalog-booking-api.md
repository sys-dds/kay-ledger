# Catalog Booking Api

Catalog and booking APIs cover offerings, pricing, availability, holds, expiry, and contention safety.

## Design questions

- Is the action idempotent?
- Does it affect ledger truth?
- Does it require approval?
- Can it affect a closed period?
- Does it emit an event?
- Does it need evidence?

Back to [README](../../README.md).
