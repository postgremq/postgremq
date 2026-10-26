# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

PostgreMQ is a PostgreSQL-based message queue system. Four independently
versioned components (see "Versioning, commits and releases" below):
- **mq**: Core PostgreSQL implementation (SQL functions and schema); semver, released as `mq/vX.Y.Z` (X.Y.Z = last migration, `mq/migrations/X.Y.Z.up.sql`), not a Go module
- **postgremq-go**: Go client library; module `postgremq.dev/postgremq-go`, package `postgremq`; the CLI (`migrate`, `status`, stdlib `flag`) is `postgremq-go/cmd/postgremq` in the same module
- **postgremq-ts**: TypeScript client library (npm `postgremq`)
- **postgremq-rs**: Rust client library (crate `postgremq`, sqlx 0.9 + Tokio)

## Architecture

### Core Concepts

**Message Visibility Timeout (vt)**: Messages use a visibility timeout instead of a traditional lock. When consumed, messages become invisible to other consumers until the timeout expires. This is called `vt` (visibility timeout) throughout the codebase.

**Exclusive vs Non-Exclusive Queues**:
- **Exclusive queues** (formerly "non-durable"): Temporary queues that require keep-alive extensions or they expire
- **Non-exclusive queues** (formerly "durable"): Persistent queues that don't expire

**Message Flow**:
1. Messages published to a topic via `publish_message()`
2. Distribution trigger automatically adds messages to all topic queues via `distribute_message()`
3. Consumers fetch messages via `consume_message()`, which sets the visibility timeout
4. Messages can be acknowledged (`ack_message`) or negatively acknowledged (`nack_message`)
5. Failed messages (final attempt) move to the dead letter queue: inline in `nack_message`, or via `pmq_maintenance_fast()` for expired final-attempt leases

### Database Schema

Located in `mq/sql/latest.sql` (fresh install; the Go tests read it from the repository):
- **topics**: Root table for message topics
- **queues**: Queue definitions with cascade delete from topics
- **messages**: Message payloads with `deliver_after` for delayed delivery
- **queue_messages**: Junction table tracking message state per queue
- **dead_letter_queue**: Failed messages that exceeded retry limits

Relationships cascade from topics down, except `dead_letter_queue`, whose FKs are ON DELETE RESTRICT (purge/requeue the DLQ before deleting).

### Client Architecture

Both clients (Go and TypeScript) share similar patterns:

**Connection**: Manages PostgreSQL pool, consumers, event listening via LISTEN/NOTIFY, and two connection-level background actors (keep-alive and vt-extension) that batch work across all of the connection's queues/consumers.
**Consumer**: Implements async message iteration; registers in-flight messages with the connection-level extender as it fetches them and deregisters them when they settle.
**Message**: Wraps queue messages with ack/nack/extend methods

**Event Listener**: Uses PostgreSQL LISTEN/NOTIFY on channels `pmq:t:<topic>` (publish) and `pmq:q:<queue>` (nack/release/requeue/group successor) with polling as the fallback.

**Auto-Extension (connection-level actor)**: One actor per connection coalesces every consumer's due visibility-timeout extensions into a single `set_vt_batch_multi` call per tick. Entries are keyed by the delivery identity `(queue, message_id, consumer_token)` (the same `message_id` is distributed to every queue on a topic, and every claim mints a new token). On lease loss (a requested row omitted from the result, or the confirmed deadline passing) the actor cancels the message's stop signal. The contract is docs/delivery-lifecycle.md. Known cross-client defects (not fixed) are listed in tmp/issues-found-during-open-source-prep.md. The schedule is a min-heap (Go) or a `Map` (TypeScript).

**Keep-Alive (connection-level actor)**: One actor per connection batches every exclusive queue's keep-alive into a single `extend_queue_keep_alive_multi` call per tick. A queue omitted from a successful result has failed permanently (gone or non-exclusive) and is dropped and treated as queue-fatal (no separate keep-alive callback exists); busy/transient errors are retried until the last confirmed deadline, then queue-fatal. Both actors outlive the consumer drain on shutdown (so in-flight messages keep extending and exclusive queues stay alive until consumers finish) and are stopped last.

## Development Commands

### TypeScript Client (postgremq-ts/)

```bash
# Build
npm run build

# Run tests (uses testcontainers for PostgreSQL)
npm test

# Run tests with coverage
npm run test:coverage

# Run integration tests only
npm run test:integration

# Run tests in watch mode
npm run test:watch
```

**Note**: TypeScript tests use testcontainers and run serially (`maxWorkers: 1`) to avoid database conflicts. Test timeout is 120 seconds to allow container start/pulls.

### Go Client (postgremq-go/)

```bash
# As published (CI does this; the module has no postgremq.dev dependencies)
GOWORK=off go test -v ./...

# Run tests with coverage
go test -v -coverprofile=coverage.out ./...

# Run specific test
go test -v -run TestConsumer_BasicFlow

# Run benchmarks
go test -bench=. -benchmem

# Run integration tests (a postgres:15 container, or POSTGREMQ_TEST_DATABASE_URL)
go test -v -tags=integration ./...
```

**Note**: Go tests use `POSTGREMQ_TEST_DATABASE_URL` when set; otherwise `internal/testserver` builds and runs a launcher (a separate module, `internal/testserver/launcher`, so testcontainers-go stays out of the library's go.mod) that starts a `postgres:15` container and removes it when its stdin closes. The first run may take longer due to the image pull.

### SQL/Core (mq/)

```bash
# Run Python tests for SQL functions
cd mq
pytest tests/tests.py -v

# With coverage
pytest tests/tests.py -v --cov
```

**Requirements**: PostgreSQL 15 or later (uses testcontainers)

## Testing Patterns

### Both clients use testcontainers
- PostgreSQL 15 container spins up automatically for tests (Go, TypeScript and Rust use the server of `POSTGREMQ_TEST_DATABASE_URL` instead when it is set, creating their own databases on it)
- Each test session creates a unique test database
- Schema loaded from `mq/sql/latest.sql`
- Tests are isolated and can be run in parallel (except TypeScript which runs serially)

### Test Organization

**Go** (`postgremq-go/`):
- `*_test.go`: Unit and integration tests
- `helpers_test.go`: Shared test utilities
- Uses testify/assert and testify/require

**TypeScript** (`postgremq-ts/src/__tests__/`):
- `*.test.ts`: Unit tests
- `integration/*.test.ts`: Integration tests
- `helpers.ts`: Shared test utilities
- `setup.ts`: Global test setup/teardown

## Common Patterns

### Creating a Consumer

**Go**:
```go
consumer := conn.Consume(ctx, "queue-name",
    postgremq.WithBatchSize(10),
    postgremq.WithVisibilityTimeout(30),
    postgremq.WithNoAutoExtension(false),
)
```

**TypeScript**:
```typescript
const consumer = connection.consume("queue-name", {
    batchSize: 10,
    visibilityTimeoutSec: 30,
    autoExtension: { enabled: true }
});
```

### Message Processing

Both clients implement async iteration:

**Go**:
```go
for msg := range consumer.Messages() {
    // process
    msg.Ack(ctx)
}
```

**TypeScript**:
```typescript
for await (const msg of consumer.messages()) {
    // process
    await msg.ack();
}
```

### Retry Configuration

Both clients support configurable retry with exponential backoff for transient database errors. Configured at connection level via `WithRetry()` (Go) or `retry` option (TypeScript).

## Key Files

- `mq/sql/latest.sql`: Complete schema and SQL functions (fresh install; guard + records the latest migration)
- `mq/migrations/`: `X.Y.Z.up.sql` migrations (upgrade path; released ones immutable, at most one pending)
- `Makefile`, `scripts/schema.py`: `make migration name=… bump=patch|minor|major`, `make sync` (pending migration, every client's numbered copy, latest.sql version), `make check`, `make test-*`; `schema.py check-title` (PR title vs schema level)
- `RELEASE.md`, `release-please-config.json`, `.github/workflows/release.yml` (also the manual publish retry), `publish.yml`, `packages.yml`, `release-pr.yml`: release process
- `.github/workflows/ci.yml` + `scripts/ci/changed_areas.py`: the one PR workflow; runs the reusable per-area test workflows the PR touches, final required `CI` job
- `scripts/release/`: release guard, consistency checks, mq releases (`mq_release.py`), Go module proxy, smoke tests
- `postgremq-go/connection.go`: Go connection management and pool handling
- `postgremq-go/actor.go`: generic connection-level background actor (shared select-loop skeleton; keep-alive and extender are `scheduler` instances)
- `postgremq-go/keepalive.go`: Go keep-alive scheduler (map of exclusive queues)
- `postgremq-go/extender.go`: Go vt-extension scheduler (composite-key min-heap)
- `postgremq-go/consumer.go`: Go single-loop consumer (fetch/deliver/track/drain)
- `postgremq-ts/src/connection.ts`: TypeScript connection management + both connection-level actors
- `postgremq-ts/src/consumer.ts`: TypeScript consumer (registers with the connection extender)
- `postgremq-rs/src/connection.rs`: Rust `Connection` (registry, close/drain, queue-fatal, settlement SQL)
- `postgremq-rs/src/scheduler.rs`: Rust background scheduler — a std-Mutex-guarded schedule driven by one task (not a message-passing actor); instances: `renewal.rs` (lease renewal), `keepalive.rs`
- `postgremq-rs/src/consumer.rs`: Rust consumer task + shared buffer (`Stream` / `next()`)
- `postgremq-rs/tests/`: integration tests (need `POSTGREMQ_TEST_DATABASE_URL`)

## Versioning, commits and releases

Full process: RELEASE.md; commit rules: CONTRIBUTING.md. Release Please
(`release-please-config.json`, `.release-please-manifest.json`,
`.github/workflows/release.yml`) versions the three clients independently and
keeps ONE combined release PR (`chore: release main`, branch
`release-please--branches--main`); merging it releases every client in it,
plus mq when the release commit carries a new migration:

| Component | Path | Tag |
|---|---|---|
| mq (SQL) | `mq/` (version = last migration, semver; not in Release Please) | `mq/vX.Y.Z` |
| Go client + CLI | `postgremq-go/` | `postgremq-go/vX.Y.Z` |
| Rust | `postgremq-rs/` (`Cargo.toml`) | `rust/vX.Y.Z` |
| TypeScript | `postgremq-ts/` (`package.json`) | `npm/vX.Y.Z` |

### Commit messages, PR titles and merges

- **Dev commits** (on a feature branch): Conventional Commits,
  `<type>(<scope>)[!]: <subject>`, subject lowercase, imperative, no period,
  under ~72 chars. Types: `feat` (minor release), `fix` (patch), `perf`,
  `revert`, `deps`; `docs`, `test`, `refactor`, `build`, `ci`, `chore` never
  release anything. Scopes: `sql`, `go`, `cli`, `ts`, `rs`, `dashboards`
  (`observability/dashboards/`, releases nothing), `docs`, `ci`, `release`, or
  several (`sql,go`). Body: what and why, wrapped at 72.
- **Breaking change**: `!` after the scope AND a `BREAKING CHANGE: <what
  breaks and how to migrate>` footer. Pre-1.0 it is a minor bump. API
  signature changes, renamed packages/modules, changed SQL contracts are
  breaking.
- **PR title = the squash commit on `main`** (PRs are squash-merged; CI checks
  the title). It is what Release Please reads, so it must carry the type,
  scope, `!` and describe the user-visible change. Put the `BREAKING CHANGE:`
  footer in the PR description so it lands in the squash commit body. The PR
  body: summary, what changed per component, how it was tested.
- **Merge commits**: none: squash only. When merging a PR locally/with `gh`,
  use `gh pr merge --squash` and keep the PR title as the subject.
- Release Please picks the components a commit releases from the PATHS it
  changes, not the scope. Keep a PR to one component when possible so each
  component's release note is accurate; a cross-cutting change touches each
  component's path and appears in each changelog.
- **Never** edit client versions, `.release-please-manifest.json`, client
  `CHANGELOG.md` files or a released migration in a feature PR. The release
  PR is generated: don't hand-edit it beyond changelog wording.
- Every client ALWAYS embeds every migration, in the same commit that
  changes them: run `make sync` (copies `mq/migrations` into
  `postgremq-{go,rs,ts}/migrations/` as `<stored version>_<X.Y.Z>.up.sql`,
  sets latest.sql's recorded version). CI fails otherwise. Never edit the
  copies by hand. A schema commit therefore touches every client and releases
  all of them; its PR title's level must cover its schema change (`fix`
  patch, `feat` minor, `!` major; exact when scoped `sql` alone; CI:
  `schema.py check-title`).
- Commits made by Claude end with the attribution lines the session asks for.

### SQL contract rules

- `postgremq.info()` → `{"schema_version", "protocol_major"}`. schema_version
  = version of the last migration applied, stored as major·10⁸ + minor·10⁴ +
  patch (0.1.0 = 10000; read from `postgremq.postgremq_migrations`; clients'
  `FormatSchemaVersion`/`format_schema_version`/`formatSchemaVersion` decode
  it). Clients check its mq compatibility LINE on connect (the major, or
  before 1.0 major.minor; `SupportedMQVersions()` / `SUPPORTED_MQ_VERSIONS`,
  now `["0.3"]`) and reject others, or a missing version,
  with a typed compatibility error. Change headers declare the kind: `patch`
  fix, `minor` additive, `major` breaking. Before 1.0 a `major` header bumps
  the mq MINOR and the others the patch (Cargo-style), so a breaking SQL change
  is made in place (no deprecated copies of old functions) and moves every
  client to the new line in the same commit; after 1.0 levels bump their own
  position. `protocol_major` is legacy,
  for clients ≤0.3.0: derived in info() as mq major + 1 since mq 0.1.1; never
  check it in new code. No capability lists or feature probes.
- A RELEASED migration (one in the newest `mq/v*` release in history) NEVER
  changes (CI: `schema.py check --base`). At most one migration is pending
  (the next mq release); every schema change is a section in it, opened by
  `-- ── change: <name> · <level> ──…`, and its version is the last release
  bumped by the highest section level. Schema/function changes are `make
  migration name=<snake_case> bump=<level>` (creates/joins/renames the pending
  migration and adds the header), the SQL below the header AND the same
  change in `mq/sql/latest.sql` (fresh install == migrations; tests check
  it), then `make sync`, all in one commit. After a rebase, `make sync` merges
  two pending migrations (main's sections first). Legacy names
  (`000001_*.sql`, tag `mq/v1`) are read from history as stored version N =
  0.0.1.

## Important Notes

- Message IDs are PostgreSQL BIGSERIAL (int64), not UUIDs
- Consumer tokens are `gen_random_uuid()` text, fresh per lease
- Keep-alive for exclusive queues must be extended periodically or queue expires
- All timestamps use TIMESTAMPTZ (PostgreSQL timezone-aware timestamps)

### Message groups (ordered delivery)

`publish_message(..., p_group_key)` allocates a dense per-(topic, group)
`group_seq` under the `message_groups` row lock (held to commit), and
`distribute_message` copies key/seq into `queue_messages`. `consume_message`
claims a grouped row only if no lower-seq row of its (queue, group) is
pending/processing (`idx_queue_messages_group_head`), so groups are claimed in
order, one at a time. Settling/removing a grouped row NOTIFYs `pmq:q:<queue>`
to wake the successor; `get_next_visible_in_ms` skips blocked successors.
Group rows are pruned by `prune_message_groups` (lock SKIP LOCKED, then a
separate fresh-snapshot DELETE — a single DELETE is unsafe under EPQ). Clients:
Go `WithGroupKey` / `Message.GroupKey`,`GroupSeq`; TS `{ groupKey }` /
`message.groupKey`,`groupSeq`. Contract: `mq/README.md` "Message groups".

### Queue-fatal teardown (consumer-cancel semantics)

When a queue a consumer depends on is **gone** the consumer is dead — modeled on
RabbitMQ's consumer-cancel / channel exception. Two detectors converge on the
connection's idempotent `queueFatal(queue, cause)`:

- `consume_message` raises **PMQ02** when the queue row is absent (deleted
  out-of-band) — it no longer silently returns zero rows for a missing queue
  (an existing-but-empty queue still returns zero rows, no error).
- The keep-alive actor failing an exclusive queue: omitted from
  `extend_queue_keep_alive_multi` (gone or non-exclusive), or transient errors
  that outlast the last *confirmed* keep-alive deadline (the queue has expired
  server-side by then). Transient errors within that deadline are only retried.

`queueFatal` deregisters keep-alive, tears down every consumer on the queue (the
normal drain cancels handlers and deregisters in-flight messages from the
extender), then signals. Both clients implement this, mirroring the RabbitMQ Go
client (`amqp091-go`): the delivery channel closes + a per-consumer close
notification carries the reason.

- **Go**: per-consumer `Consumer.NotifyClose(chan error)` (plus `Messages()`
  closing) and connection-wide `WithQueueFatalHandler`. The reason is a
  `*QueueFatalError` matching `errors.Is(err, ErrQueueGone)`.
- **TS**: per-consumer `Consumer.onClose((err?) => …)` / `HandlerConsumer.onClose`
  (plus the message iterator ending) and connection-wide `onQueueFatal` option /
  `'queueFatal'` event. The reason is a `QueueFatalError`.

The connection handler is the only signal for a producer-only exclusive queue
that has no consumer.

