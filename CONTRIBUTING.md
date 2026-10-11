# Contributing to PostgreMQ

Thank you for your interest in contributing! Bug reports, fixes, documentation
and new features are all welcome.

This project follows a [Code of Conduct](./CODE_OF_CONDUCT.md). By
participating you agree to uphold it. Report security issues privately as
described in [SECURITY.md](./SECURITY.md), not in public issues.

## Project layout

| Path | Component |
|------|-----------|
| `mq/` | SQL schema and functions (`sql/latest.sql`, `migrations/`), SQL tests (`tests/`) |
| `postgremq-go/` | Go client; the CLI (migrations and status) in `postgremq-go/cmd/postgremq/` |
| `postgremq-ts/` | TypeScript client |
| `postgremq-rs/` | Rust client (crate `postgremq`) |
| `observability/` | Metrics contract, Collector config and end-to-end test |
| `docs/` | Architecture, delivery lifecycle and observability guides |

Start with [docs/architecture.md](./docs/architecture.md). The SQL contract is
in [mq/README.md](./mq/README.md), and the clients' shared behaviour and their
differences are in [docs/delivery-lifecycle.md](./docs/delivery-lifecycle.md).

## Prerequisites

- Docker. The test suites start PostgreSQL 15 with testcontainers.
- Go 1.25+ (the Go workspace and the metrics example need Go 1.26)
- Node.js 22+ (the test suite needs 22.22+)
- Rust 1.94+
- Python 3.10+ (SQL and observability tests, `scripts/schema.py`)
- make

## Building and testing

Each component has its own tests. CI runs them on every pull request that
touches the component. `make` lists the shortcuts (`make test`, `make
test-go`, ...); they run `make sync` first.

### SQL

```bash
cd mq
pip install -r tests/requirements.txt
pytest tests/tests.py -v
```

`mq/sql/latest.sql` is the fresh-install script and `mq/migrations/` the
upgrade path. Migrations carry the mq version they migrate to
(`mq/migrations/X.Y.Z.up.sql`), and at most one is pending: the next mq
release, which every schema change adds a section to. A released migration
never changes (CI checks it). A schema or function change is:

```bash
make migration name=add_queue_priority bump=minor   # patch: a fix; minor: additive; major: breaking
# write the SQL below the header it added, and the same change in mq/sql/latest.sql
make sync                                           # every client's copy, latest.sql's version
```

and one commit with all of it, in a PR titled at the same level (`fix(sql):`
for patch, `feat(sql):` for minor, `feat(sql)!:` for major; CI checks it).
`make migration` creates the pending migration, adds your section to it, or
renames it to a higher version when your change needs one, and prints where
to write. The SQL tests check that both paths produce the same schema.

Every client embeds every migration: `make sync` copies them into
`postgremq-go/migrations/`, `postgremq-rs/migrations/` and
`postgremq-ts/migrations/` under numbered names (never edit a copy), and sets
the version `latest.sql` records. CI fails when any of them is behind (`make
check`); `make hooks` installs a pre-commit hook that checks the same. The
TypeScript build and Jest generate `postgremq-ts/src/migrations.generated.ts`
from its copy.

After rebasing a schema PR on a `main` that also changed the schema, run
`make sync`: it merges two pending migrations into one and rewrites the
version `latest.sql` records (resolve a conflict on that line either way). A
database you migrated from your branch is then rebuilt, not upgraded.
RELEASE.md has the full rules.

### Go

```bash
cd postgremq-go
GOWORK=off go test -race ./...     # as CI runs it: the module on its own
go test -short ./...               # skips the long benchmarks
make check                         # postgremq-go/Makefile: gofmt, go vet, staticcheck
```

CI also runs `golangci-lint`. The CLI is part of the module
(`postgremq-go/cmd/postgremq`), so `go test ./...` covers it. The repository
root has a `go.work` that ties `postgremq-go` and
`postgremq-go/examples/metrics` together for local development.

### TypeScript

```bash
cd postgremq-ts
npm ci
npm run build
npx tsc --noEmit
npm test                  # Jest; parallel workers, one PostgreSQL container per worker
```

### Rust

```bash
cd postgremq-rs
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test                         # starts a postgres:15 container, or set
cargo test --all-features          # POSTGREMQ_TEST_DATABASE_URL to a server whose
                                   # user can create databases
```

### Observability (end-to-end)

```bash
pip install -r mq/tests/requirements.txt
pytest observability/test_collector.py -v
```

This runs PostgreSQL, an OpenTelemetry Collector and the metrics example of
each client, and checks the exported metrics.

## Making changes

1. For anything beyond a small fix, open an issue first to agree on the
   approach.
2. Keep pull requests focused on one change.
3. Add tests. Behaviour shared by the clients (settlement, renewal,
   keep-alive, shutdown, queue loss, message groups) should be tested in each
   client it affects, and a change to one client's behaviour usually needs
   the same change in the others.
4. Update the documentation that describes the behaviour you changed:
   READMEs, `docs/`, doc comments.
5. Do not edit versions or changelogs: release notes are generated from the
   squash commit (your PR title) when the component is released.

### Guidelines

- **SQL**: lowercase names; raise errors with the project's SQLSTATEs (`PMQ01`
  lease lost, `PMQ02` queue not found, `PMQ03` validation); keep functions
  safe under concurrency (state the locking argument in a comment).
- **Go**: `gofmt`; doc comments on exported identifiers; errors are wrapped,
  never ignored; contexts for cancellation.
- **TypeScript**: strict types, no `any` in public APIs; TSDoc on exports.
- **Rust**: `rustfmt`, clippy clean with `-D warnings`; doc comments with
  `# Errors` sections on public fallible functions; no lock held across
  `.await`.
- **Transactions**: functions that accept a caller's transaction or
  connection never begin, commit or roll it back.

### Commit messages and PR titles

PRs are squash-merged, and the PR title becomes the commit on `main`, so the
PR title must be a [Conventional Commit](https://www.conventionalcommits.org/)
(CI checks it):

```
<type>(<scope>)[!]: <subject, lowercase, imperative, no period>
```

- **Types**: `feat` (new behaviour: a minor release), `fix` (a patch
  release), `perf`, `revert`, `deps` (dependency updates that matter to
  users); `docs`, `test`, `refactor`, `build`, `ci`, `chore` do not appear in
  release notes and release nothing on their own.
- **Scopes**: `sql`, `go`, `cli`, `ts`, `rs`, `docs`, `ci`, `release`, or
  several (`sql,go`). The scope is for readers; Release Please decides which
  clients a commit releases from the paths it changes. A schema commit
  (scope `sql`) changes every client (`make sync`), so it releases all of
  them, and mq with them. Its type gives the level of its schema change
  (`fix` patch, `feat` minor, `!` major): exactly, when the scope is `sql`
  (or `mq`) alone; at least, when other scopes share the PR.
- **Breaking changes**: add `!` after the scope and a `BREAKING CHANGE:`
  footer in the PR description's squash commit body, saying what breaks and
  how to migrate. Before 1.0 a client's breaking change is a minor release,
  listed under "BREAKING CHANGES"; a breaking schema change bumps mq's major
  version.
- Commits on your branch can be anything; only the squash commit counts. Keep
  a PR to one component where you can, so each release note says what changed
  in that component.

Examples: `feat(go): add WithGroupKey publish option`,
`fix(sql): keep group order when a nack is delayed`,
`feat(ts)!: make connect() check the protocol major`,
`feat(sql): add queue priority` (a minor schema change, with `make sync`).

## Reporting bugs and requesting features

Use the issue templates. For bugs, include the component and version, the
PostgreSQL version, a minimal reproduction, and what you expected.

## License

By contributing, you agree that your contributions are licensed under the
[MIT License](./LICENSE).
