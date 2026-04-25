# Local Development

Kay-Ledger is designed to run locally with Docker Compose plus the Java API.

## Local stack

The local environment includes:

- PostgreSQL
- Kafka
- Redis
- OpenSearch
- MinIO / S3-compatible object storage
- Temporal
- Temporal UI
- Region A API
- Region B API

## Typical workflow

1. Start infrastructure with Docker Compose.
2. Run the API from `apps/api`.
3. Use migrations to initialize schema.
4. Run focused tests.
5. Run broader integration tests when needed.

Back to [README](../../README.md).
