# System Overview

Kay-Ledger is a modular monolith with explicit finance domain boundaries.

## Runtime components

- API controllers expose business operations.
- Application services enforce domain rules.
- Stores persist source truth.
- PostgreSQL stores durable financial state.
- Kafka carries asynchronous events.
- Outbox and inbox tables protect event delivery and deduplication.
- Temporal handles long-running operator workflows.
- OpenSearch powers investigation surfaces.
- MinIO/S3 stores exports and evidence.
- Regional simulation models ownership and failover.

## Main idea

Keep financial correctness close together while still making boundaries explicit.

Back to [README](../../README.md).
