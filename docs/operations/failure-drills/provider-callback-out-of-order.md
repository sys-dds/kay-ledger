# Failure Drill: Provider Callback Out Of Order

## Premise

Out-of-order callback must not corrupt state.

## What to prove

- financial truth remains safe
- no duplicate money movement occurs
- the failure is visible
- recovery is audited
- evidence can be generated

## Steps

1. Create baseline state.
2. Inject failure.
3. Observe response.
4. Check source truth.
5. Check derived views.
6. Run recovery if needed.
7. Confirm invariants.

Back to [README](../../../README.md).
