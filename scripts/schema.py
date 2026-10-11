#!/usr/bin/env python3
"""mq schema versions: semver migrations, the pending release, client copies.

    scripts/schema.py new NAME LEVEL                  # start a schema change (patch, minor or major)
    scripts/schema.py sync                            # bring the client copies and latest.sql up to date
    scripts/schema.py check [--base REF]              # fail if sync would change anything or a rule is broken
    scripts/schema.py check-title TITLE --base REF    # the PR title states its schema change's level
    scripts/schema.py export DIR                      # the migrations under numbered names, for migration tools
    scripts/schema.py latest                          # print the last migration's version

Usually run through make (`make migration name=... bump=...`, `make sync`,
`make check`).

Versions. mq/migrations/X.Y.Z.up.sql migrates the schema to mq version
X.Y.Z. The database stores the version as one number, X*10^8 + Y*10^4 + Z
(0.1.0 is 10000; minor and patch stay below 10000): the version table
golang-migrate and the other clients order migrations by, and what
postgremq.info() reports as schema_version.

Released and pending. A migration is released once it is part of the newest
mq/v* release in HEAD's history; released migrations never change. At most
one migration is pending (not in that release; `sync` renames it above the
release): the next mq release. Each schema change appends a section to it, opened by a header that
names the change and its level:

    -- ── change: add_priority · minor ───────────────────────────────────

The pending migration's version is the last release bumped by the highest
level among its sections. `new` creates it, joins it, or renames it to a
higher version; `sync` merges two pending migrations (two branches each
started one) and renames it when its sections call for another version.

Clients. `sync` copies mq/migrations/*.up.sql into postgremq-go/migrations,
postgremq-rs/migrations and postgremq-ts/migrations under numbered names
(000010000_0.1.0.up.sql), which each client embeds, and sets the version
mq/sql/latest.sql records. The copies live in each client's directory, so the
commit that changes the schema also changes every client, and Release Please
releases each of them.
"""

from __future__ import annotations

import argparse
import re
from collections import Counter
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

ROOT = Path(__file__).resolve().parents[1]
MIGRATIONS_DIR = 'mq/migrations'
MIGRATIONS = ROOT / MIGRATIONS_DIR
COPIES = [ROOT / 'postgremq-go' / 'migrations', ROOT / 'postgremq-rs' / 'migrations',
          ROOT / 'postgremq-ts' / 'migrations']
LATEST_SQL = ROOT / 'mq' / 'sql' / 'latest.sql'
MAIN = 'origin/main'

LEVELS = ('patch', 'minor', 'major')
FILE = re.compile(r'^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)\.(up|down)\.sql$')
# Before mq versions were semver (mq/v1 and its 000001_initial_schema.up.sql):
# NNNNNN_<name>.<direction>.sql and mq/vN, N being the stored version. Read
# from history only, to compare against migrations released under those names.
LEGACY_FILE = re.compile(r'^(\d{6})_[a-z0-9_]+\.(up|down)\.sql$')
TAG = re.compile(r'^mq/v(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)$')
LEGACY_TAG = re.compile(r'^mq/v(\d+)$')
NAME = re.compile(r'^[a-z][a-z0-9_]*$')
HEADER = re.compile(r'^-- ── change: (?P<name>[a-z][a-z0-9_]*) · (?P<level>major|minor|patch) ─+$')
HEADER_WIDTH = 78
LATEST_RECORD = re.compile(
    r'^(INSERT INTO postgremq\.postgremq_migrations \(version, dirty\) VALUES \()(\d+)(, false\);)$', re.M)

# PR titles (Conventional Commits): the level each type releases with. Other
# types (docs, chore, ...) release nothing.
SUBJECT = re.compile(r'^(?P<type>\w+)(?:\((?P<scope>[^)]*)\))?(?P<bang>!)?: (?P<subject>.+)$')
TYPE_LEVELS = {'feat': 'minor', 'fix': 'patch', 'perf': 'patch', 'revert': 'patch', 'deps': 'patch'}
# A title scoped only to these is about the schema alone: its level must match exactly.
SCHEMA_SCOPES = {'sql', 'mq'}


class SchemaError(Exception):
    pass


@dataclass(frozen=True, order=True)
class Version:
    major: int
    minor: int
    patch: int

    def __str__(self) -> str:
        return f'{self.major}.{self.minor}.{self.patch}'

    @classmethod
    def parse(cls, text: str) -> Version:
        match = re.fullmatch(r'(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)', text)
        if not match:
            raise SchemaError(f'not a version: {text!r}')
        return cls(*map(int, match.groups()))

    def encode(self) -> int:
        """The number the database stores."""
        if self.minor >= 10_000 or self.patch >= 10_000:
            raise SchemaError(f'{self}: minor and patch must stay below 10000')
        return self.major * 100_000_000 + self.minor * 10_000 + self.patch

    @classmethod
    def decode(cls, number: int) -> Version:
        return cls(number // 100_000_000, number // 10_000 % 10_000, number % 10_000)

    def bump(self, level: str) -> Version:
        if level == 'major':
            return Version(self.major + 1, 0, 0)
        if level == 'minor':
            return Version(self.major, self.minor + 1, 0)
        return Version(self.major, self.minor, self.patch + 1)

    def level_from(self, released: Version) -> str | None:
        """The level this version is a valid bump of `released` by, if any."""
        return next((level for level in LEVELS if released.bump(level) == self), None)


ZERO = Version(0, 0, 0)


def rank(level: str | None) -> int:
    return -1 if level is None else LEVELS.index(level)


def highest(levels) -> str | None:
    return max(levels, key=rank, default=None)


def copy_name(version: Version, direction: str = 'up') -> str:
    return f'{version.encode():09d}_{version}.{direction}.sql'


def header(name: str, level: str) -> str:
    text = f'-- ── change: {name} · {level} '
    return text + '─' * max(3, HEADER_WIDTH - len(text))


# --- git -------------------------------------------------------------------

def git(*args: str, check: bool = True) -> str:
    result = subprocess.run(['git', '-C', str(ROOT), *args], capture_output=True, encoding='utf-8')
    if check and result.returncode != 0:
        raise SchemaError(f'git {" ".join(args)}: {result.stderr.strip()}')
    return result.stdout if result.returncode == 0 else ''


def fetch_tags() -> bool:
    """Best effort: release tags decide what is released."""
    try:
        return subprocess.run(['git', '-C', str(ROOT), 'fetch', '--quiet', '--tags', 'origin'],
                              capture_output=True, timeout=60).returncode == 0
    except (OSError, subprocess.TimeoutExpired):
        return False


def tag_version(tag: str) -> Version | None:
    if match := TAG.match(tag):
        return Version(*map(int, match.groups()))
    if match := LEGACY_TAG.match(tag):
        return Version.decode(int(match.group(1)))
    return None


def released_tag(ref: str = 'HEAD') -> tuple[Version, str] | None:
    """The newest mq release in REF's history, as (version, tag)."""
    tags = [(version, tag) for tag in git('tag', '--merged', ref, '--list', 'mq/v*').split()
            if (version := tag_version(tag)) is not None]
    return max(tags, default=None)


def released(ref: str = 'HEAD') -> Version | None:
    found = released_tag(ref)
    return found[0] if found else None


def versions_at(ref: str) -> set[Version]:
    """The up migrations' versions at REF, legacy names included."""
    return {parsed[0] for name in git('ls-tree', '--name-only', f'{ref}:{MIGRATIONS_DIR}', check=False).split()
            if (parsed := parse_name(name, legacy=True)) and parsed[1] == 'up'}


def parse_name(name: str, legacy: bool = False) -> tuple[Version, str] | None:
    """(version, direction) of a migration file name; legacy names only when asked."""
    if match := FILE.match(name):
        return Version(*map(int, match.groups()[:3])), match.group(4)
    if legacy and (match := LEGACY_FILE.match(name)):
        return Version.decode(int(match.group(1))), match.group(2)
    return None


def migrations_at(ref: str) -> dict[tuple[Version, str], str]:
    """mq/migrations at REF: {(version, direction): content}, legacy names included."""
    found = {}
    for name in git('ls-tree', '--name-only', f'{ref}:{MIGRATIONS_DIR}', check=False).split():
        if parsed := parse_name(name, legacy=True):
            found[parsed] = git('show', f'{ref}:{MIGRATIONS_DIR}/{name}')
    return found


def last_version_at(ref: str) -> Version:
    versions = [version for version, direction in migrations_at(ref) if direction == 'up']
    if not versions:
        raise SchemaError(f'{ref} has no migrations')
    return max(versions)


# --- sections ----------------------------------------------------------------

def sections(text: str, where: str) -> list[tuple[str, str]]:
    """The (name, level) headers of a pending migration, checked: every
    statement is in a section and names are unique."""
    found: list[tuple[str, str]] = []
    for number, line in enumerate(text.splitlines(), start=1):
        if match := HEADER.match(line):
            if any(name == match['name'] for name, _ in found):
                raise SchemaError(f'{where}:{number}: a second change named {match["name"]}')
            found.append((match['name'], match['level']))
        elif line.startswith('-- ──') or line.startswith('-- change:'):
            raise SchemaError(f'{where}:{number}: malformed change header; write it as {header("name", "minor")!r}')
        elif not found and line.strip() and not line.lstrip().startswith('--'):
            raise SchemaError(f'{where}:{number}: SQL before the first change header')
    if not found:
        raise SchemaError(f'{where}: a pending migration needs a change header, e.g. {header("name", "minor")!r}')
    return found


def all_sections(text: str) -> list[tuple[str, str]]:
    """Headers without the pending-migration checks (released files, history)."""
    return [(m['name'], m['level']) for m in map(HEADER.match, text.splitlines()) if m]


# --- the working tree --------------------------------------------------------

@dataclass
class Tree:
    released: Version | None
    # The migrations the last release has. Released means in the release, not
    # merely older than it: a branch's pending migration rebased past a
    # release keeps its old version but is still pending (`sync` renames it).
    released_versions: frozenset[Version]
    ups: dict[Version, Path]
    downs: dict[Version, Path]

    @classmethod
    def load(cls) -> Tree:
        ups: dict[Version, Path] = {}
        downs: dict[Version, Path] = {}
        for path in sorted(MIGRATIONS.iterdir()):
            if path.name.startswith('.'):
                continue
            parsed = parse_name(path.name)
            if parsed is None:
                raise SchemaError(f'{MIGRATIONS_DIR}/{path.name}: name it X.Y.Z.up.sql (`make migration` does)')
            version, direction = parsed
            target = ups if direction == 'up' else downs
            if version in target:
                raise SchemaError(f'{MIGRATIONS_DIR}: {target[version].name} and {path.name} are the same version')
            target[version] = path
        if not ups:
            raise SchemaError(f'{MIGRATIONS_DIR} has no migrations')
        for version, path in downs.items():
            if version not in ups:
                raise SchemaError(f'{MIGRATIONS_DIR}/{path.name}: no up migration {version}')
        found = released_tag()
        tree = cls(found[0] if found else None, frozenset(versions_at(found[1]) if found else ()), ups, downs)
        if tree.released is not None and tree.released not in ups:
            raise SchemaError(f'mq/v{tree.released} is released, but {MIGRATIONS_DIR} has no {tree.released}.up.sql')
        for version in tree.pending():
            if version in downs:
                raise SchemaError(f'{MIGRATIONS_DIR}/{downs[version].name}: migrations only go up; '
                                  'a pending migration has no down migration')
        return tree

    @property
    def base(self) -> Version:
        return self.released or ZERO

    def pending(self) -> list[Version]:
        return sorted(v for v in self.ups if v not in self.released_versions)

    def last(self) -> Version:
        return max(self.ups)

    def pending_level(self, version: Version) -> str:
        return highest(level for _, level in sections(self.text(version), self.relative(version)))

    def text(self, version: Version) -> str:
        return self.ups[version].read_text(encoding='utf-8')

    def relative(self, version: Version) -> str:
        return f'{MIGRATIONS_DIR}/{self.ups[version].name}'


Change = tuple[str, Callable[[], None]]


def main_files(paths: list[Path]) -> list[Path]:
    """Which of PATHS main has, as of the main commit this branch is on (the
    merge base with origin/main, not origin/main itself, which may have moved
    on since)."""
    base = git('merge-base', MAIN, 'HEAD', check=False).strip()
    if not base:
        raise SchemaError(f'cannot find where this branch left {MAIN}; fetch it (git fetch origin)')
    present = set(git('ls-tree', '--name-only', f'{base}:{MIGRATIONS_DIR}', check=False).split())
    return [path for path in paths if path.name in present]


def pending_plan(tree: Tree) -> list[Change]:
    """Merging two pending migrations into one, or renaming the pending one to
    the version its sections call for."""
    pending = tree.pending()
    if not pending:
        return []
    if tree.released is None and len(tree.ups) > 1:
        raise SchemaError(f'no mq release in this history, so every migration in {MIGRATIONS_DIR} '
                          f'is pending; fetch the mq/v* tags (git fetch --tags)')
    levels = {version: tree.pending_level(version) for version in pending}
    target = tree.base.bump(highest(levels.values()))
    if len(pending) == 1:
        [version] = pending
        if version == target:
            return []
        source, destination = tree.ups[version], MIGRATIONS / f'{target}.up.sql'
        return [(f'{tree.relative(version)} -> {destination.name}: its changes make it {target}',
                 lambda: source.rename(destination))]
    # Main's sections first: they were merged before this branch's.
    from_main = main_files([tree.ups[v] for v in pending])
    if len(from_main) != 1:
        raise SchemaError(f'{len(pending)} pending migrations ({", ".join(map(str, pending))}), and '
                          f'{"none" if not from_main else "several"} of them on {MAIN}: merge them into one by '
                          f'hand (main\'s sections first) and run `make sync`')
    ordered = sorted(pending, key=lambda v: (tree.ups[v] not in from_main, v))
    names = [name for version in ordered for name, _ in sections(tree.text(version), tree.relative(version))]
    if len(names) != len(set(names)):
        raise SchemaError(f'the pending migrations {", ".join(map(str, ordered))} share a change name; rename one')
    paths = [tree.ups[version] for version in ordered]
    destination = MIGRATIONS / f'{target}.up.sql'

    def merge() -> None:
        texts = [path.read_text(encoding='utf-8').rstrip('\n') + '\n' for path in paths]
        for path in paths:
            path.unlink()
        destination.write_text('\n'.join(texts), encoding='utf-8')

    return [(f'merge the pending migrations {", ".join(p.name for p in paths)} into {destination.name} '
             f'(main\'s first)', merge)]


def copies_plan(tree: Tree) -> list[Change]:
    changes: list[Change] = []
    wanted = {copy_name(version): path for version, path in tree.ups.items()}
    for directory in COPIES:
        relative = directory.relative_to(ROOT)
        if directory.is_symlink():
            changes.append((f'{relative}: replace the symlink with a directory', directory.unlink))
            present = set()
        else:
            present = {path.name for path in directory.glob('*.sql')} if directory.exists() else set()
        for name, source in wanted.items():
            target = directory / name
            if directory.is_symlink() or not target.exists() or target.read_bytes() != source.read_bytes():
                changes.append((f'{relative}/{name}: copy of {MIGRATIONS_DIR}/{source.name}',
                                lambda s=source, d=directory, t=target: (
                                    d.mkdir(exist_ok=True), t.write_bytes(s.read_bytes()))))
        for name in sorted(present - wanted.keys()):
            changes.append((f'{relative}/{name}: remove (not in {MIGRATIONS_DIR})',
                            lambda t=directory / name: t.unlink()))

    number = tree.last().encode()
    text = LATEST_SQL.read_text(encoding='utf-8')
    match = LATEST_RECORD.search(text)
    if not match:
        raise SchemaError('mq/sql/latest.sql: cannot find the INSERT recording its version')
    if int(match.group(2)) != number:
        changes.append((f'mq/sql/latest.sql: record version {number} ({tree.last()}), not {match.group(2)}',
                        lambda: LATEST_SQL.write_text(
                            LATEST_RECORD.sub(rf'\g<1>{number}\g<3>', LATEST_SQL.read_text(encoding='utf-8'), count=1),
                            encoding='utf-8')))
    return changes


def plan() -> list[Change]:
    """What `sync` would change. A change to the pending migrations comes
    alone: the copies are planned again after it."""
    tree = Tree.load()
    return pending_plan(tree) or copies_plan(tree)


def immutable_errors(base: str) -> list[str]:
    """Released migrations (by this tree's last release) that differ from BASE."""
    tree = Tree.load()
    if tree.released is None:
        return []
    errors = []
    current = {**{(v, 'up'): p for v, p in tree.ups.items()}, **{(v, 'down'): p for v, p in tree.downs.items()}}
    merge_base = git('merge-base', base, 'HEAD').strip()
    for (version, direction), content in sorted(migrations_at(merge_base).items()):
        if version not in tree.released_versions:
            continue
        path = current.get((version, direction))
        if path is None:
            errors.append(f'{version}.{direction}.sql (released) was deleted')
        elif path.read_text(encoding='utf-8') != content:
            errors.append(f'{MIGRATIONS_DIR}/{path.name} (released) was changed')
    return errors


# --- commands ----------------------------------------------------------------

def cmd_new(name: str, level: str) -> int:
    if not NAME.match(name or '') or level not in LEVELS:
        print('usage: make migration name=<snake_case_name> bump=<patch|minor|major>', file=sys.stderr)
        return 2
    if not fetch_tags():
        print('schema: could not fetch tags from origin; using the local mq/v* tags', file=sys.stderr)
    tree = Tree.load()
    pending = tree.pending()
    if len(pending) > 1 or (pending and pending_plan(tree)):
        raise SchemaError('the pending migrations need `make sync` first')
    if pending:
        [version] = pending
        path = tree.ups[version]
        existing = sections(path.read_text(encoding='utf-8'), tree.relative(version))
        if any(existing_name == name for existing_name, _ in existing):
            raise SchemaError(f'{tree.relative(version)} already has a change named {name}')
        target = tree.base.bump(highest([level, *(lvl for _, lvl in existing)]))
        text = path.read_text(encoding='utf-8').rstrip('\n') + '\n\n'
        if target != version:
            path.rename(MIGRATIONS / f'{target}.up.sql')
            print(f'Renamed {tree.relative(version)} to {target}.up.sql: a {level} change makes the '
                  f'next release {target}.')
    else:
        target, text = tree.base.bump(level), ''
    destination = MIGRATIONS / f'{target}.up.sql'
    destination.write_text(f'{text}{header(name, level)}\n\n', encoding='utf-8')
    print(f'Write the change below its header, at the end of {MIGRATIONS_DIR}/{destination.name} '
          f'(the next mq release, {target}).')
    prefix = {'patch': 'fix:', 'minor': 'feat:', 'major': 'feat!:'}[level]
    print(f'Make the same change in mq/sql/latest.sql, run `make sync`, and give the PR a {level}-level '
          f'title ({prefix} ...).')
    return 0


def cmd_sync() -> int:
    tree = Tree.load()
    if len(tree.pending()) > 1:
        # Merging decides what is pending: do it with the current tags only.
        if not fetch_tags():
            raise SchemaError('two pending migrations, and the mq/v* tags could not be fetched to tell '
                              'which are released; fetch them (git fetch --tags) and run `make sync` again')
    applied = 0
    for _ in range(5):
        changes = plan()
        if not changes:
            break
        for description, apply in changes:
            apply()
            print(f'  {description}')
            applied += 1
    else:
        raise SchemaError('sync did not settle; run `make check` to see what keeps changing')
    last = Tree.load().last()
    print(f'Clients and latest.sql are at {last} (stored as {last.encode()}).' if applied else
          f'Already in sync at {last}.')
    if applied:
        print('Stage the result: git add -A mq postgremq-go/migrations postgremq-rs/migrations postgremq-ts/migrations')
    return 0


def cmd_check(base: str | None) -> int:
    errors = [description for description, _ in plan()]
    if errors:
        print('Not in sync; run `make sync` and commit the result:', file=sys.stderr)
        for description in errors:
            print(f'  - {description}', file=sys.stderr)
        return 1
    if base:
        if errors := immutable_errors(base):
            print('Released migrations never change. Restore them (git checkout <the mq/v* tag> -- <file>) '
                  'and put the change in the pending migration (`make migration`):', file=sys.stderr)
            for error in errors:
                print(f'  - {error}', file=sys.stderr)
            return 1
    tree = Tree.load()
    pending = tree.pending()
    state = f'pending {pending[0]}' if pending else 'nothing pending'
    print(f'Schema consistent: released {tree.released or "nothing"}, {state}; clients and latest.sql in sync.')
    return 0


def title_level(title: str) -> tuple[str | None, set[str]]:
    """The level a PR title releases with, and its scopes."""
    match = SUBJECT.match(title.strip())
    if not match:
        raise SchemaError(f'not a Conventional Commit title: {title!r}')
    scopes = {scope.strip() for scope in (match['scope'] or '').split(',') if scope.strip()}
    if match['bang']:
        return 'major', scopes
    return TYPE_LEVELS.get(match['type']), scopes


def cmd_check_title(title: str, base: str) -> int:
    level, scopes = title_level(title)
    merge_base = git('merge-base', base, 'HEAD').strip()
    if not (git('diff', '--name-only', merge_base, '--', MIGRATIONS_DIR).strip() or
            git('ls-files', '--others', '--exclude-standard', '--', MIGRATIONS_DIR).strip()):
        print('No schema change in this PR.')
        return 0
    tree = Tree.load()
    for version in tree.pending():
        sections(tree.text(version), tree.relative(version))  # well-formed
    # Headers this PR adds (or re-levels): counted, as a released migration
    # may have used the same name.
    before = Counter(section for content in migrations_at(merge_base).values() for section in all_sections(content))
    after = Counter(section for version in tree.ups for section in all_sections(tree.text(version)))
    added = list((after - before).elements())
    needed = highest(lvl for _, lvl in added)
    errors = []
    if level is None:
        errors.append('it changes mq/migrations, which every client ships, so the title must release them: '
                      'fix: (patch), feat: (minor) or a breaking feat!: (major)')
    elif rank(level) < rank(needed):
        errors.append(f'it adds a {needed} schema change ({", ".join(n for n, l in added if l == needed)}), '
                      f'but the title says {level}')
    elif scopes and scopes <= SCHEMA_SCOPES and needed and level != needed:
        errors.append(f'a title scoped to the schema says what kind of schema change it is: this one adds '
                      f'a {needed} change, but the title says {level}')
    if errors:
        print(f'PR title {title!r} does not match its schema change:', file=sys.stderr)
        for error in errors:
            print(f'  - {error}', file=sys.stderr)
        print('Levels: fix:/perf: patch, feat: minor, ! (e.g. feat!:) major.', file=sys.stderr)
        return 1
    print(f'PR title matches its schema change ({needed or "no new change header"}).')
    return 0


def cmd_export(directory: str) -> int:
    tree = Tree.load()
    out = Path(directory)
    out.mkdir(parents=True, exist_ok=True)
    for kind, files in (('up', tree.ups), ('down', tree.downs)):
        for version, path in files.items():
            (out / copy_name(version, kind)).write_bytes(path.read_bytes())
    print(f'Exported {len(tree.ups) + len(tree.downs)} migrations to {out}.')
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    commands = parser.add_subparsers(dest='command', required=True)
    new = commands.add_parser('new', help='start a schema change in the pending migration')
    new.add_argument('name', help='snake_case name, e.g. add_queue_priority')
    new.add_argument('level', help='patch, minor or major')
    commands.add_parser('sync', help='bring the pending migration, the client copies and latest.sql up to date')
    check = commands.add_parser('check', help='fail if sync would change anything or a rule is broken')
    check.add_argument('--base', help='also check that migrations released by then are unchanged since BASE')
    title = commands.add_parser('check-title', help='the PR title matches the level of its schema changes')
    title.add_argument('title')
    title.add_argument('--base', required=True)
    export = commands.add_parser('export', help='write the migrations under numbered names into DIR')
    export.add_argument('directory')
    commands.add_parser('latest', help="print the last migration's version")
    args = parser.parse_args()
    try:
        if args.command == 'new':
            return cmd_new(args.name, args.level)
        if args.command == 'sync':
            return cmd_sync()
        if args.command == 'check':
            return cmd_check(args.base)
        if args.command == 'check-title':
            return cmd_check_title(args.title, args.base)
        if args.command == 'export':
            return cmd_export(args.directory)
        print(Tree.load().last())
        return 0
    except SchemaError as error:
        print(f'schema: {error}', file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())
