# Releasing PostgreMQ

The clients are versioned independently by
[Release Please](https://github.com/googleapis/release-please), running in
`.github/workflows/release.yml`; the SQL schema (mq) is versioned by its last
migration. Everything unreleased collects in **one release PR**; merging it
releases every component in it. Tags, GitHub releases and registry
publishing follow automatically.

## Components

| Component | Directory | Version | Tag | Published as |
|-----------|-----------|---------|-----|--------------|
| SQL schema (mq) | `mq/` | the last migration number | `mq/vN` | GitHub release with `latest.sql` and the migrations attached |
| Go client and CLI | `postgremq-go/` | the tag | `postgremq-go/vX.Y.Z` | Go module `postgremq.dev/postgremq-go` (package `postgremq`; CLI `postgremq.dev/postgremq-go/cmd/postgremq`) |
| Rust client | `postgremq-rs/` | `Cargo.toml` | `rust/vX.Y.Z` | crates.io crate `postgremq` |
| TypeScript client | `postgremq-ts/` | `package.json` | `npm/vX.Y.Z` | npm package `postgremq` |

`release-please-config.json` defines the three clients;
`.release-please-manifest.json` holds each one's last released version. The
release PR is rebuilt on top of `main` after every push (`always-update`),
so it is never behind `main` (the ruleset requires up-to-date branches) and
its checks run against exactly what would be released.
Release Please assigns commits to clients by the paths they change. Changes
outside these directories (docs, `observability/`, CI) release nothing.

## Versioning rules

- Clients: semantic versioning of their public language API. Versions need
  not match across clients.
  - After 1.0: incompatible API changes bump MAJOR, compatible features MINOR,
    fixes PATCH.
  - Before 1.0 (now): a breaking change bumps MINOR, as does a feature
    (`bump-minor-pre-major: true`, `bump-patch-for-minor-pre-major: false`); a
    fix bumps PATCH.
  - Breaking changes are listed under **⚠ BREAKING CHANGES** in the release
    notes, from the commit's `!` or `BREAKING CHANGE:` footer.
- mq: `mq/vN` is the schema at migration N, exactly what `postgremq.info()`
  reports as `schema_version`. Compatibility is the protocol major (below),
  not the version number.
- Published versions and tags are immutable. A correction is a new release.

## The SQL schema

### Protocol major and discovery

The protocol major is the contract clients rely on: SQL function signatures,
result types, error codes, ownership rules and delivery semantics. It is
currently **1**. Additive changes (a new function, an added optional
parameter with a default, an added result field) keep it; incompatible ones
need a new major.

```sql
SELECT postgremq.info();
-- {"schema_version": 7, "protocol_major": 1}
```

`schema_version` is the number of the last migration applied: the exact
schema and function state of the database. `info()` reads it from the version
table that every install path maintains (the migrators and `latest.sql`).

`postgremq.info()` and its two fields are stable across protocol majors;
fields may be added. Each client declares the majors it implements
(`SupportedProtocolMajors()` in Go, `SUPPORTED_PROTOCOL_MAJORS` in TypeScript
and Rust), checks `info()` when it connects, and rejects any other major with
a typed compatibility error that names the schema version, its major and the
supported majors (Go `*CompatibilityError` / `ErrIncompatibleSchema`,
TypeScript `CompatibilityError`, Rust `Error::Incompatible`). A database
without `info()` gets the same error saying it needs an installation or
upgrade; connection and permission errors keep their real cause. Within a
supported major, clients call SQL functions directly: a function missing from
an older installation fails with the database's own error (SQLSTATE 42883),
through normal error handling. A client may declare a second major only when
it implements and tests both contracts.

### Migrations, latest.sql and the clients

- `mq/migrations/` holds [golang-migrate](https://github.com/golang-migrate/migrate)
  migrations, numbered 1..N without gaps. A migration's number is a schema
  version, so **a migration never changes once it is on `main`**: a schema or
  function change adds a new migration. CI fails a PR that modifies, deletes
  or renames an existing migration
  (`scripts/release/check_migrations_immutable.sh`); until the first mq
  release, `000001_initial_schema` may still be edited.
- The protocol major is a literal in `info()`; only a migration that breaks
  the client contract redefines `info()` with a new major (a breaking change
  for every client).
- `mq/sql/latest.sql` is the fresh-install script: everything the migrations
  produce, in one file. A guard at the top refuses to run when the version
  table `postgremq.postgremq_migrations` exists (an existing installation is
  upgraded with the migrations), and the end records the last migration's
  number.
- **Every client always embeds every migration.** At any commit the Go client
  carries a copy of `mq/migrations` (`postgremq-go/migrations/`; a Go module
  cannot embed files outside its directory), and the Rust and TypeScript
  clients pin the last migration number (`[package.metadata.postgremq]
  mq-schema` in `postgremq-rs/Cargo.toml`, `"postgremq": {"mq-schema"}` in
  `postgremq-ts/package.json`) and embed migrations 1..N from `mq/migrations`
  at build time. `make sync` writes all of these and the version `latest.sql`
  records; CI (`scripts/schema.py check`) refuses a commit where any of them
  is behind.
- The SQL test suite (`mq/tests/tests.py`) checks that a fresh `latest.sql`
  install and a database migrated through every migration have identical
  schema dumps, version rows and `info()`; that `info()` reports the last
  migration; that every migration at an `mq/v*` tag in the branch's history is
  unchanged; and that upgrading from each such release's `latest.sql` equals
  a fresh install.

A schema change is one commit (one PR):

```bash
make migration name=add_queue_priority   # creates mq/migrations/00000N_add_queue_priority.up.sql
# write the migration, and the same change in mq/sql/latest.sql
make sync                                # Go copy, Rust/TS pins, latest.sql's version
make test-sql                            # optional locally; CI runs everything
git add -A && git commit -m "feat(sql): add queue priority"
```

`make hooks` installs a pre-commit hook that refuses a commit while the
clients are out of sync (it checks the working tree; CI checks the commit). Because `make sync` changes a file in every client,
the commit releases every client with the new schema, and their changelogs
list it.

If two open PRs add the same migration number, the second one fails CI after
the first merges (`schema: two migrations numbered N`): rename its migration
to the next number and run `make sync`. Requiring branches to be up to date
before merging (repository settings) catches this before the merge.

## Day-to-day development

- PRs are squash-merged. The PR title becomes the commit message on `main` and
  must be a [Conventional Commit](https://www.conventionalcommits.org/)
  (checked by `.github/workflows/pr-title.yml`); see CONTRIBUTING.md for the
  types and scopes.
- Feature PRs never change client versions, `.release-please-manifest.json`,
  the client changelogs, or existing migrations.
- `feat` and `fix` (and `perf`, `revert`, `deps`) appear in release notes;
  `docs`, `test`, `ci`, `refactor`, `build` and `chore` do not, and do not
  trigger a release on their own. A migration is a user-visible change: commit
  it as `feat` or `fix` (or `perf`). Otherwise it waits, and a release PR that
  brings it without releasing every client fails *Release PR consistency*
  (each client must ship the new schema): add a `feat`/`fix` commit for the
  missing clients.

## The release pipeline

On every push to `main`, `release.yml`:

1. **Validates** the pushed commit with the reusable test workflows (SQL with
   the schema sync check, Go with `GOWORK=off`, Rust, TypeScript,
   observability) and `packages.yml` (the Go module and CLI installed from an
   external module through a module proxy, the packed npm package, the
   packaged crate, each run against a fresh database). The `Validated` job
   passes only if all of them do.
2. **Guards** tagging (`scripts/release/check_pending_releases.py`): a merged
   release PR still labelled `autorelease: pending` must have a merge commit
   that passed validation (this run, or an earlier run's `Validated` job on
   that commit) and must pass `verify_release.py --all` at that commit.
   Otherwise nothing is tagged and the run fails (see *Correcting a pending
   release*). A release PR merged after the run's own commit is left to its
   own run: this run then tags nothing, without failing.
3. **Tags** the merge commit for each client in the release PR and creates
   their GitHub releases with the changelog sections as notes (Release
   Please, `skip-github-pull-request`).
4. **Releases mq** (`scripts/release/mq_release.py release`): if the newest
   client release commit carries a migration newer than the last `mq/v*` tag,
   it creates `mq/vN` there, with notes from the commits that changed `mq/`
   since the previous mq release.
5. **Opens or updates** the release PR (`chore: release main`) for every
   client with unreleased `feat`/`fix` changes: version bumps, `CHANGELOG.md`
   entries, manifest (Release Please, `skip-github-release`). A comment on the
   PR says whether merging it also releases mq, with the mq notes
   (`mq_release.py comment`).
6. **Publishes** each new tag through `publish.yml` (a reusable workflow
   only `release.yml` calls).

A failed run tags nothing and opens no PRs. Release boundaries come from the
last release tags, so the next passing run includes every unreleased change;
if a run fails after tagging the clients, the next run releases mq.

### Publishing (`publish.yml`)

For a tag it checks out the tag, verifies it (`verify_release.py --tag-exists`:
manifest, version files, changelog and schema sync match the tag), smoke-tests
the package built from the tag, skips a version already in its registry, and
publishes:

- `rust/v*`: `cargo publish`;
- `npm/v*`: `npm publish --provenance` (pre-release versions under the `next`
  dist-tag);
- `postgremq-go/v*`: the Go module is published by the tag itself (which is
  why Go validation precedes tagging); the job asks `proxy.golang.org` to
  fetch the version;
- `mq/v*`: attaches `postgremq-vN.sql` (`latest.sql`) and
  `postgremq-migrations-vN.tar.gz` to the GitHub release.

Until the repository variable `PUBLISH_ENABLED` is `true`, the registry steps
are dry runs (`cargo publish --dry-run`, `npm publish --dry-run`, no proxy
request).

**Manual retry**: Actions → Release → Run workflow with the existing tag (e.g.
`npm/v0.2.0` or `mq/v3`). It runs only the publication of that tag (not the
validation of `main`), re-runs the component's tests on the tag first, and
skips a version that is already published. Retries go through `release.yml`
rather than `publish.yml` directly because registry trusted publishing checks
the calling workflow's file name (and npm allows one trusted publisher per
package).

### Releasing

1. Wait for the release PR (`chore: release main`). It lists each client's
   version and notes; the mq comment says whether mq is released with it. Edit
   nothing in it except, if needed, the changelog wording; Release Please
   rewrites the branch when new releasable commits reach `main`.
2. Make sure its checks pass (*Release PR consistency*, from
   `release-pr.yml`, runs `verify_release.py --all`).
3. Squash-merge it. The next `release.yml` run on that merge commit tags the
   clients and mq, creates the GitHub releases, and publishes.

The release PR releases every client with releasable changes; there is no
switch to leave one out. Hold a change back by not merging it to `main`.

### Correcting a pending release

If the guard refuses a merged release PR because its merge commit failed
validation:

- If the failure was spurious, re-run the failed jobs of that commit's
  `release.yml` run. When its `Validated` job succeeds, the next run on `main`
  tags it (push any commit, or re-run the latest run).
- If the commit is broken, do not tag it: remove the `autorelease: pending`
  label from the release PR, revert the release commit on `main`, and fix
  forward. Release Please then proposes a fresh release PR.

If it refuses because the release is inconsistent, fix `main` the same way and
let Release Please propose the release again.

## First release

Versioning starts from the baseline in `.release-please-manifest.json`: every
client at `0.1.0` (npm `postgremq` 0.1.0 and the crates.io `postgremq` 0.0.1
placeholder exist from before; nothing else was published), with
`bootstrap-sha` limiting history to commits after the switch. The first
release notes' compare link points at the never-created `<component>/v0.1.0`
tag; edit it out of the release PR's changelog if you like. The first release
of each client is `0.2.0` (its commits include `feat`s), and `mq/v1` is
released with it. Its notes include the history before this layout (for
example a breaking-change note naming the removed `postgremq.dev/mq` module);
edit the `mq/v1` GitHub release afterwards if you like.

## One-time setup

- **GitHub App for release PRs.** Release PRs opened with the default
  `GITHUB_TOKEN` trigger no workflows, so their checks never run. Create a
  GitHub App with *Contents*, *Pull requests* and *Issues* read/write and
  *Actions* read, install it on this repository, and set the variable
  `RELEASE_APP_ID` and the secret `RELEASE_APP_PRIVATE_KEY` (set at
  organization level, they serve every repository the app is installed on).
  Without them release.yml falls back to `GITHUB_TOKEN` (with a warning); then
  also enable *Settings → Actions → General → Allow GitHub Actions to create
  and approve pull requests*.
- **Merging** (*Settings → General → Pull Requests*): allow squash merging
  only, with *Default commit message: Pull request title and description*
  (the description carries a `BREAKING CHANGE:` footer into the squash
  commit); always suggest updating branches; automatically delete head
  branches.
- **Branch ruleset for `main`** (*Settings → Rules → Rulesets*): target the
  default branch; restrict deletions, block force pushes, require linear
  history, require a pull request (0 approvals for a single maintainer;
  squash only), and require status checks with *Require branches to be up to
  date before merging* (catches two PRs adding the same migration number):
  `CI`, `Conventional Commit title` and `Release PR consistency`. All three
  run on every pull request: `ci.yml` runs the tests of the areas the PR
  touches (`scripts/ci/changed_areas.py`) and its final `CI` job passes only
  if none of them failed (on the release PR it runs no tests: the PR only
  changes versions and changelogs, `main` is validated on every push, and the
  merge commit is validated again before tagging); `pr-title.yml`; `release-pr.yml` (skipped, which
  counts as passing, except on the release PR). The per-area workflows
  (`sql-tests.yml`, `go-tests.yml`, ...) are reusable only, called by
  `ci.yml`, `release.yml` and `publish.yml`.
- **Tag ruleset for releases**: target `mq/v*`, `postgremq-go/v*`, `rust/v*`
  and `npm/v*`; restrict creations, updates and deletions and block force
  pushes, with the release GitHub App on the bypass list. Only the release
  workflow then creates release tags, and nobody moves or deletes them.
- **Actions and security**: workflow permissions *Read repository contents*
  (jobs request what they need); private vulnerability reporting
  (SECURITY.md) and Dependabot alerts on.
- **Registries.** Trusted publishing (OIDC): on npm (package `postgremq` →
  Settings → Trusted Publisher → GitHub Actions) and crates.io (crate
  `postgremq` → Settings → Trusted Publishing), register owner `postgremq`,
  repository `postgremq` and workflow **`release.yml`** (the calling
  workflow, not `publish.yml`), no environment. Then set the variables
  `NPM_TRUSTED_PUBLISHING=true` and `CRATES_IO_TRUSTED_PUBLISHING=true`, and
  after the first publish set the npm package to *Require two-factor
  authentication and disallow tokens*. Without trusted publishing, add scoped
  tokens as the secrets `NPM_TOKEN` and `CARGO_REGISTRY_TOKEN`.
- **Go module path.** `postgremq.dev/postgremq-go` resolves through
  `go-import` discovery on postgremq.dev (the website repository): every path
  under the domain answers `?go-get=1` with `<meta name="go-import"
  content="postgremq.dev git https://github.com/postgremq/postgremq">`. The
  prefix is the domain (the repository root), so the go command finds the
  module in the `postgremq-go/` subdirectory with `postgremq-go/vX.Y.Z` tags.
  `scripts/release/go-proxy.sh` builds a local module proxy from a tag or the
  working tree for the smoke tests.
- **Enable publishing**: set the variable `PUBLISH_ENABLED=true`.

## Tools

| Command | Purpose |
|---------|---------|
| `make migration name=…`, `make sync`, `make check`, `make hooks`, `make test-*` | Schema changes and checks (`scripts/schema.py`); `make` lists them |
| `scripts/release/check_migrations_immutable.sh [BASE]` | Fails if a migration already on BASE changed (run on PRs) |
| `scripts/release/verify_release.py <tag>` / `--all` | Checks a release's consistency |
| `scripts/release/check_pending_releases.py` | The release guard |
| `scripts/release/mq_release.py pending\|notes\|comment\|release` | mq releases |
| `scripts/release/go-proxy.sh DIR VERSION` | Builds a GOPROXY tree with the Go module (from its tag when released) |
| `scripts/release/smoke-go.sh`, `smoke-npm.sh`, `smoke-rust.sh` | Installs the packages outside the repository and runs them (`SMOKE_ADMIN_URL`) |

## Security releases

Security fixes follow SECURITY.md: the fix is developed in a private GitHub
security advisory, released as a PATCH of each affected component, and the
advisory is published with the release.

## Yanking a bad release

Published versions are never reused. To withdraw one:

- npm: `npm deprecate postgremq@X.Y.Z "<reason>"`;
- crates.io: `cargo yank --version X.Y.Z postgremq`;
- Go: add a `retract` directive for the version to the module's `go.mod` and
  release a new version;
- mq: a schema release cannot be withdrawn (its migration is on `main` and in
  every client); fix it with a new migration.

Then release a fixed version.
