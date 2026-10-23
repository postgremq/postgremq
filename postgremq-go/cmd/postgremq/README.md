# PostgreMQ CLI

`postgremq` is a command-line tool for installing and upgrading the PostgreMQ database schema. It has two commands: `migrate` applies the embedded SQL migrations and `status` reports the schema version.

All PostgreMQ objects live in the fixed `postgremq` schema. The CLI does not change application schemas or the connection's `search_path`.

## Installation

Requires Go 1.25 or later. The CLI is part of the Go client module and has
its version (tags `postgremq-go/vX.Y.Z`). From a release:

```bash
go install postgremq.dev/postgremq-go/cmd/postgremq@latest
```

Or build it from a clone:

```bash
git clone https://github.com/postgremq/postgremq.git
cd postgremq/postgremq-go
go build -o postgremq ./cmd/postgremq

# Optional: put it on your PATH
mv postgremq /usr/local/bin/
```

The migrations are embedded in the binary, so it needs no other files at
runtime. It applies the migrations of the Go client it is built with, which
always include the latest schema version of that release.

## Usage

```
Usage: postgremq <command> --dsn <connection-string>

Commands:
  migrate   Apply pending migrations up to the latest embedded version
  status    Show the migration status
```

`postgremq -h` prints this; `postgremq <command> -h` prints the command's
flags. Flags take one or two dashes (`-dsn` or `--dsn`).

### Connection string (`--dsn`)

`migrate` and `status` require `--dsn`. It accepts any connection string that pgx accepts, in URL or keyword/value form:

```bash
postgremq status --dsn "postgres://user:password@localhost:5432/mydb?sslmode=disable"
postgremq status --dsn "host=localhost port=5432 user=user dbname=mydb sslmode=require"
```

The CLI does not read a connection string from the environment itself. Settings missing from the DSN fall back to the standard libpq environment variables (`PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD`, `PGDATABASE`, `PGSSLMODE`, ...), so you can keep the password out of the command line:

```bash
export PGPASSWORD=secret
postgremq migrate --dsn "postgres://app@db.internal:5432/mydb?sslmode=require"

# or pass a URL kept in an environment variable
postgremq migrate --dsn "$DATABASE_URL"
```

## Commands

### `migrate`

Applies pending migrations, up to the latest version embedded in the CLI. It creates the `postgremq` schema if it does not exist yet. Migrations only go up; there is no target version.

```bash
postgremq migrate --dsn <connection-string>
```

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--dsn` | string | none (required) | Database connection string |

`migrate` first prints the current and latest versions. It then:

- prints `✓ Database is up to date` and exits if no migration is pending and the database is not dirty. This includes a database already at a newer version than the CLI's latest (migrated by a newer release), which is left unchanged;
- otherwise prints `Running migrations...`, waits for any migration another process is running (the version reads dirty while it does), creates the `postgremq` schema if needed, and applies the pending migrations up to the latest version. It exits with an error if the database is still dirty then (see [Dirty state](#dirty-state)).

```bash
postgremq migrate --dsn "postgres://postgres:postgres@localhost:5432/mydb"
```

Versions are mq versions, each followed by the number the database stores
(X×10⁸ + Y×10⁴ + Z; see [mq/README.md](../../../mq/README.md#versions-and-discovery)).
Output on a fresh database:

```
Current version: none (schema version 0)
Latest version:  0.1.0 (schema version 10000)
Running migrations...
✓ Migration completed successfully
```

Output when there is nothing to do:

```
Current version: 0.1.0 (schema version 10000)
Latest version:  0.1.0 (schema version 10000)
✓ Database is up to date
```

### `status`

Prints the migration status. It is read-only: it does not create the `postgremq` schema or the version table, and a database without them reports version 0.

```bash
postgremq status --dsn <connection-string>
```

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--dsn` | string | none (required) | Database connection string |

Migrations pending:

```
Current version: none (schema version 0)
Latest version:  0.1.0 (schema version 10000)
Dirty:           false

⚠ Migration needed: none (schema version 0) -> 0.1.0
```

Up to date:

```
Current version: 0.1.0 (schema version 10000)
Latest version:  0.1.0 (schema version 10000)
Dirty:           false

✓ Database is up to date
```

`status` exits with code 0 whenever it can read the status, including when migrations are pending or the database is dirty. The last line depends only on the version: a dirty database shows `Dirty:           true` and then the pending or up-to-date line as usual, so a dirty database at the latest version prints `✓ Database is up to date`. Check the `Dirty` line, or run `migrate`, which refuses a dirty database.

## Migration tracking

The version is stored in the single-row table `postgremq.postgremq_migrations`, which `migrate` creates:

| Column | Type | Meaning |
|--------|------|---------|
| `version` | `bigint` (primary key) | Last applied migration's mq version, as X×10⁸ + Y×10⁴ + Z |
| `dirty` | `boolean` | `true` if that migration started but did not finish |

Migrations are applied by the Go client's built-in runner from the SQL files embedded from its copy of `mq/migrations/` (`postgremq-go/migrations/`, under numbered names such as `000010000_0.1.0.up.sql`). It follows [golang-migrate](https://github.com/golang-migrate/migrate)'s protocol (this version table, its advisory lock and dirty flag), as the TypeScript and Rust clients do, so a database that golang-migrate or another client migrated can be upgraded by any of them.

## Required privileges

When the `postgremq` schema does not exist yet, `migrate` creates it, which needs the `CREATE` privilege on the database:

```sql
GRANT CREATE ON DATABASE mydb TO myuser;
```

Without it, `migrate` fails with:

```
Error: migration failed: failed to create queue schema: ERROR: permission denied for database mydb (SQLSTATE 42501)
```

Applying migrations to an existing schema needs the privileges the migrations' DDL needs (normally: own the `postgremq` schema). `status`, and `migrate` on an up-to-date database, only need to connect, use the `postgremq` schema, and read `postgremq.postgremq_migrations` (if the schema does not exist yet, connecting is enough). Without `USAGE` on an existing `postgremq` schema both commands fail with `failed to get migration status: ERROR: permission denied for schema postgremq (SQLSTATE 42501)`.

## Errors and exit codes

| Code | Meaning |
|------|---------|
| 0 | Success. For `status`, this includes pending migrations and a dirty database |
| 1 | An error: unparsable DSN, connection failure, dirty database (`migrate`), migration failure |
| 2 | A usage error: unknown command or flag, missing `--dsn`, extra arguments |

On an error, stderr gets `Error: <message>`; a usage error is followed by the
command's usage. For example:

```
Error: --dsn is required

Usage: postgremq status --dsn <connection-string>

Show the migration status.

Flags:
  -dsn string
    	Database connection string (required)
```

Common messages:

| Message | Cause |
|---------|-------|
| `--dsn is required` | `--dsn` missing |
| `failed to connect: cannot parse ...` | The DSN is malformed |
| `failed to get migration status: failed to connect to ...` | The server cannot be reached, or authentication failed |
| `failed to get migration status: ERROR: permission denied for schema postgremq ...` | The role cannot use the `postgremq` schema |
| `database is in dirty state - manual intervention required` | See [Dirty state](#dirty-state) |
| `postgremq: migration 0.2.0 (schema version 20000) failed at line 12, column 3: ERROR: ... (SQLSTATE ...)` | A migration failed and its version is left dirty (see [Dirty state](#dirty-state)). The line and column are in that migration's file, `mq/migrations/0.2.0.up.sql`, when PostgreSQL reports a position the client can map (errors in a function's definition included; not with a multibyte database or client encoding other than UTF8, such as EUC_JP). An `ERROR ... (SQLSTATE ...)` from the server means the migration was rolled back; after a lost connection or timeout, check what it changed. The server's `detail`, `hint`, the failing `query` inside a function and the `context` follow when it sends them |
| `migration failed: failed to create queue schema: ...` | The role lacks `CREATE` on the database (the `postgremq` schema is missing) or on the `postgremq` schema (its version table is missing) |
| `postgremq: database schema version N (mq X.Y.Z) is not one of this client's migrations, so no upgrade from it is known` | The recorded version is below this release's latest migration but is none of them, so no migration path from it is known; no migration is applied |

### Dirty state

If a migration fails, or `migrate` is killed or loses its connection while it runs, the database is left with `dirty = true` and `version` set to that migration. `migrate` refuses to run until you fix it:

1. Make sure no migration is still running. A killed `migrate`'s server session usually keeps running the migration file, which is one transaction, and commits it at the end; it rolls it back instead if it notices the lost client first (for example when a statement sends a notice). Either way the version stays dirty. The migrating session holds an advisory lock:

   ```sql
   SELECT a.pid, a.state, a.query_start, left(a.query, 60)
   FROM pg_locks l JOIN pg_stat_activity a USING (pid)
   WHERE l.locktype = 'advisory' AND l.granted AND a.datname = current_database();
   ```

   Wait for it to finish, or end it with `SELECT pg_terminate_backend(<pid>)`, which rolls the migration back.

2. Inspect the version row and the `postgremq` schema to see what the failed migration actually changed:

   ```sql
   SELECT version, dirty FROM postgremq.postgremq_migrations;
   ```

3. Bring the schema to a consistent state, then record that state:

   - If the failed migration's changes are fully in place, clear the flag:

     ```sql
     UPDATE postgremq.postgremq_migrations SET dirty = false;
     ```

   - If they are absent or have been rolled back, set the version to the previous migration's so the failed one runs again. Versions are not consecutive: take the number before the failed one from the migration file names (`postgremq-go/migrations/`, e.g. `000000001_0.0.1.up.sql` before `000010000_0.1.0.up.sql`). For the first migration, delete the row so the database reads as version 0:

     ```sql
     UPDATE postgremq.postgremq_migrations SET version = <previous migration's version>, dirty = false;
     -- for the first migration:
     DELETE FROM postgremq.postgremq_migrations;
     ```

4. Run `postgremq migrate` again.

Do not just clear the flag when the migration did not complete. `migrate` and `status` would then report the database as up to date while it is missing the schema changes.

## Common workflows

### Fresh database

```bash
createdb myapp_db
postgremq migrate --dsn "postgres://postgres:postgres@localhost:5432/myapp_db"
```

### CI/CD

```bash
#!/bin/bash
set -e

postgremq status --dsn "$DATABASE_URL"    # informational; exits 0 even if migrations are pending
postgremq migrate --dsn "$DATABASE_URL"   # exits 1 on failure or a dirty database

./myapp
```

## Migrating from Go code

To run migrations at application startup instead of through the CLI, use the same functions from the Go client:

```go
package main

import (
	"context"
	"log"

	"github.com/jackc/pgx/v5/pgxpool"
	"postgremq.dev/postgremq-go"
)

func main() {
	ctx := context.Background()

	pool, err := pgxpool.New(ctx, "postgres://...")
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	status, err := postgremq.GetMigrationStatus(pool)
	if err != nil {
		log.Fatal(err)
	}
	if status.NeedsMigration {
		if err := postgremq.Migrate(pool); err != nil {
			log.Fatal(err)
		}
	}

	conn, err := postgremq.DialFromPool(ctx, pool)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	// publish / consume with conn
}
```

See [`postgremq-go/examples/migration/`](../../postgremq-go/examples/migration/) for a complete example.

## See also

- [PostgreMQ Go client](../../postgremq-go/README.md)
- [Migration example](../../postgremq-go/examples/migration/)
