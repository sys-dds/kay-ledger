# ADR-0015: Use maker-checker approvals for sensitive finance actions

## Status

Accepted

## Context

Kay-Ledger is designed around financial correctness, auditability, recovery, and operational truth.

## Decision

Use maker-checker approvals for sensitive finance actions.

## Consequences

### Benefits

- makes financial behavior easier to explain
- reduces silent drift
- improves operator recovery
- strengthens testability
- improves public architecture clarity

### Costs

- adds design complexity
- requires stronger tests
- requires clear docs
- makes the project more detailed than CRUD

## Alternatives considered

- direct CRUD mutation
- manual database repair
- blind provider callback trust
- premature microservice decomposition
- logs as the main audit mechanism

## Backlinks

- [README](../../README.md)
- [System overview](../architecture/system-overview.md)
- [Financial invariants](../architecture/financial-invariants.md)
