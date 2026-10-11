# PostgreMQ Go Examples

Runnable programs for the Go client. Each one is a `main` package.

| Directory | Shows |
|---|---|
| [basic](basic/main.go) | Creating a topic and queue, publishing, consuming with `Consume`, acking, and shutting down on SIGINT/SIGTERM. |
| [transactions](transactions/main.go) | `PublishWithTx`, `AckWithTx`, and a rolled-back publish, on a pool shared through `DialFromPool`. |
| [migration](migration/main.go) | Checking and applying the schema with `GetMigrationStatus` and `Migrate`. |
| [metrics](metrics/main.go) | OpenTelemetry client metrics with `WithMeterProvider`, exported to a local Collector. |

## Prerequisites

- PostgreSQL 15+.
- Go 1.25+. The repository's `go.work` and the `metrics` example module declare Go 1.26. With the default `GOTOOLCHAIN=auto`, `go` downloads that toolchain when it is needed.

## Setup

### 1. Install the schema

Choose one method. The paths below are relative to this directory.

```bash
# CLI
go run ../cmd/postgremq migrate --dsn "postgres://user:password@localhost:5432/dbname?sslmode=disable"

# psql
psql "postgres://user:password@localhost:5432/dbname" -v ON_ERROR_STOP=1 -f ../../mq/sql/latest.sql

# Go API (the migration example)
cd migration && go run .
```

### 2. Point the examples at your database

```bash
export DATABASE_URL="postgres://user:password@localhost:5432/dbname?sslmode=disable"
```

If `DATABASE_URL` is not set, `basic`, `transactions` and `migration` use `postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable`.

## Running

```bash
cd basic && go run .          # Ctrl+C to stop
cd transactions && go run .
cd migration && go run .
```

The `metrics` example expects the observability stack: a PostgreSQL database on port 55432 and an OpenTelemetry Collector on port 4318. Run it from `postgremq-go`:

```bash
docker compose -f ../observability/compose.yaml up -d
(cd examples/metrics && go run .)   # its own module; needs Go 1.26
```

It reads `DATABASE_URL` and `OTEL_EXPORTER_OTLP_ENDPOINT` if they are set. See [docs/observability.md](../../docs/observability.md).

## Common patterns

The package name is `postgremq`:

```go
import "postgremq.dev/postgremq-go"
```

Connect:

```go
cfg, err := pgxpool.ParseConfig(os.Getenv("DATABASE_URL"))
if err != nil {
	log.Fatal(err)
}
conn, err := postgremq.Dial(ctx, cfg)
if err != nil {
	log.Fatal(err)
}
defer conn.Close()
```

Publish:

```go
id, err := conn.Publish(ctx, "orders", json.RawMessage(`{"order_id": 1}`))
```

Consume:

```go
// Consume needs the queue's topic: create the queue on this Connection
// (CreateQueue is idempotent) or pass postgremq.WithTopic("orders").
consumer, err := conn.Consume("order-processor",
	postgremq.WithBatchSize(10),
	postgremq.WithVT(30))
if err != nil {
	log.Fatal(err)
}
defer consumer.Stop()

for msg := range consumer.Messages() {
	// process msg.Payload
	if err := msg.Ack(ctx); err != nil {
		log.Printf("ack: %v", err)
	}
}
```

The [client README](../README.md) covers the full API.

## Troubleshooting

**Connection refused.** Check that PostgreSQL is reachable: `psql "$DATABASE_URL" -c "SELECT version();"`.

**`schema "postgremq" does not exist` or `function postgremq.… does not exist`.** The schema is not installed in this database. See step 1.

**`ErrQueueNotFound` from `Consume`.** The `Connection` does not know the queue's topic. Call `CreateQueue` first or pass `WithTopic`. This error also means the queue does not exist.

**Permission denied.** The role that installs the schema needs `CREATE` on the database. A separate runtime role needs `USAGE` on the `postgremq` schema plus privileges on its tables, sequences and functions, for example:

```sql
GRANT USAGE ON SCHEMA postgremq TO app_user;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA postgremq TO app_user;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA postgremq TO app_user;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA postgremq TO app_user;
```

## More examples

- [Example tests](../example_test.go)
- [Integration tests](../integration_test.go)
- [TypeScript examples](../../postgremq-ts/examples/)
