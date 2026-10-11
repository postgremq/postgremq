#!/usr/bin/env python3
"""Keeps every client pointing at the latest schema migration.

    scripts/schema.py new NAME   # add mq/migrations/NNNNNN_NAME.up.sql (next number)
    scripts/schema.py sync       # bring the clients and latest.sql up to the last migration
    scripts/schema.py check      # fail if `sync` would change anything (CI)

Usually run through make (`make migration name=...`, `make sync`,
`make check`). mq/migrations is the source; `sync` writes, for its last
migration N:

- postgremq-go/migrations/: a copy of every mq/migrations/*.up.sql (a Go
  module cannot embed files outside its directory);
- postgremq-rs/Cargo.toml `[package.metadata.postgremq] mq-schema = N` and
  postgremq-ts/package.json `"postgremq": {"mq-schema": N}` (both embed
  migrations 1..N from mq/migrations);
- mq/sql/latest.sql: the version it records for a fresh install.

The pins live in each client's directory, so the commit that adds a migration
also changes every client and Release Please releases each of them with it.
Migration numbers must run 1..N without gaps or duplicates.
"""

from __future__ import annotations

import argparse
import re
import shutil
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MIGRATIONS = ROOT / 'mq' / 'migrations'
GO_COPY = ROOT / 'postgremq-go' / 'migrations'
CARGO_TOML = ROOT / 'postgremq-rs' / 'Cargo.toml'
PACKAGE_JSON = ROOT / 'postgremq-ts' / 'package.json'
LATEST_SQL = ROOT / 'mq' / 'sql' / 'latest.sql'

UP = re.compile(r'^(\d{6})_([a-z0-9_]+)\.up\.sql$')
NAME = re.compile(r'^[a-z0-9_]+$')
# Each pin pattern: (text before)(number)(text after).
RUST_PIN = re.compile(r'^(\[package\.metadata\.postgremq\]\s*\nmq-schema = )(\d+)()$', re.M)
TS_PIN = re.compile(r'("postgremq":\s*\{\s*"mq-schema":\s*)(\d+)()')
LATEST_RECORD = re.compile(
    r'^(INSERT INTO postgremq\.postgremq_migrations \(version, dirty\) VALUES \()(\d+)(, false\);)$', re.M)


class SchemaError(Exception):
    pass


def migrations() -> list[tuple[int, str]]:
    """mq/migrations' up migrations as (number, file name), checked to run 1..N."""
    found = []
    for path in sorted(MIGRATIONS.iterdir()):
        if path.name.endswith('.down.sql') or path.name.startswith('.'):
            continue
        match = UP.match(path.name)
        if not match:
            raise SchemaError(f'mq/migrations/{path.name}: not NNNNNN_<snake_case_name>.up.sql')
        found.append((int(match.group(1)), path.name))
    if not found:
        raise SchemaError('mq/migrations has no migrations')
    for expected, (number, name) in enumerate(found, start=1):
        if number != expected:
            others = [n for num, n in found if num == number and n != name]
            if others:
                raise SchemaError(f'two migrations numbered {number}: {others[0]} and {name}; renumber yours')
            raise SchemaError(f'mq/migrations: expected migration {expected}, found {name}')
    return found


def latest() -> int:
    return migrations()[-1][0]


def pinned(pattern: re.Pattern[str], path: Path) -> int | None:
    match = pattern.search(path.read_text())
    return int(match.group(2)) if match else None


def plan() -> list[tuple[str, callable]]:
    """What `sync` would change, as (description, apply) pairs."""
    found = migrations()
    number = found[-1][0]
    changes: list[tuple[str, callable]] = []

    wanted = {name for _, name in found}
    present = {path.name for path in GO_COPY.glob('*.sql')} if GO_COPY.exists() else set()
    for _, name in found:
        source, target = MIGRATIONS / name, GO_COPY / name
        if not target.exists() or target.read_bytes() != source.read_bytes():
            changes.append((f'postgremq-go/migrations/{name}: copy from mq/migrations',
                            lambda s=source, t=target: (t.parent.mkdir(exist_ok=True), shutil.copyfile(s, t))))
    for name in sorted(present - wanted):
        changes.append((f'postgremq-go/migrations/{name}: remove (not in mq/migrations)',
                        lambda t=GO_COPY / name: t.unlink()))

    for path, pattern, label in ((CARGO_TOML, RUST_PIN, '[package.metadata.postgremq] mq-schema'),
                                 (PACKAGE_JSON, TS_PIN, '"postgremq": {"mq-schema"}'),
                                 (LATEST_SQL, LATEST_RECORD, 'recorded migration')):
        current = pinned(pattern, path)
        relative = path.relative_to(ROOT)
        if current is None:
            raise SchemaError(f'{relative}: cannot find its {label}')
        if current != number:
            changes.append((f'{relative}: {label} {current} -> {number}',
                            lambda p=path, pat=pattern: p.write_text(
                                pat.sub(rf'\g<1>{number}\g<3>', p.read_text(), count=1))))
    return changes


def cmd_new(name: str) -> int:
    if not name or not NAME.match(name):
        print('usage: make migration name=<snake_case_name>', file=sys.stderr)
        return 2
    number = latest() + 1
    path = MIGRATIONS / f'{number:06d}_{name}.up.sql'
    path.write_text(
        f'-- Migration {number}: {name.replace("_", " ")}.\n'
        '--\n'
        '-- Make the same change in mq/sql/latest.sql (a fresh install must equal\n'
        '-- the migrations), then run `make sync`. Once merged to main this file\n'
        '-- never changes.\n'
    )
    print(f'Created {path.relative_to(ROOT)}.')
    print('Next: write the migration, make the same change in mq/sql/latest.sql, then run `make sync`.')
    return 0


def cmd_sync() -> int:
    changes = plan()
    for description, apply in changes:
        apply()
        print(f'  {description}')
    print(f'Clients and latest.sql are at migration {latest()}.' if changes else
          f'Already in sync at migration {latest()}.')
    return 0


def cmd_check() -> int:
    changes = plan()
    if changes:
        print(f'Not in sync with the latest migration ({latest()}); run `make sync` and commit the result:',
              file=sys.stderr)
        for description, _ in changes:
            print(f'  - {description}', file=sys.stderr)
        return 1
    print(f'Clients and latest.sql are in sync at migration {latest()}.')
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    commands = parser.add_subparsers(dest='command', required=True)
    new = commands.add_parser('new', help='add the next migration')
    new.add_argument('name', help='snake_case name, e.g. add_queue_priority')
    commands.add_parser('sync', help='bring the clients and latest.sql up to the last migration')
    commands.add_parser('check', help='fail if sync would change anything')
    commands.add_parser('latest', help='print the last migration number')
    args = parser.parse_args()
    try:
        if args.command == 'new':
            return cmd_new(args.name)
        if args.command == 'sync':
            return cmd_sync()
        if args.command == 'check':
            return cmd_check()
        print(latest())
        return 0
    except SchemaError as error:
        print(f'schema: {error}', file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())
