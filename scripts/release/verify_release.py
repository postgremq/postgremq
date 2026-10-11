#!/usr/bin/env python3
"""Check that the checked-out source is a consistent release.

    scripts/release/verify_release.py rust/v0.2.0 [--tag-exists]   # one release tag
    scripts/release/verify_release.py --all                          # everything at HEAD

Used on release PRs (--all), by the release guard before tags are created
(--all), and by the publish workflow on each release tag. Checks:

- every client points at the last migration (scripts/schema.py check: the Go
  copy, the Rust and TypeScript pins, the version latest.sql records);
- a client's version agrees across the Release Please manifest and its own
  files (Cargo.toml/Cargo.lock, package.json/package-lock.json; the Go module
  is versioned by its tag only), and its CHANGELOG.md has an entry for it;
- mq/vN: N is the last migration; with --all, when the release brings a new
  mq version, every client is part of it (else a client would keep shipping
  an older schema while mq/vN is released);
- the Go module requires no other module of this repository and has a LICENSE;
- with --tag-exists, the tag exists and points at HEAD.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'scripts'))
import schema  # noqa: E402

# client component (tag prefix) -> package path in release-please-config.json
CLIENTS = {
    'postgremq-go': 'postgremq-go',
    'rust': 'postgremq-rs',
    'npm': 'postgremq-ts',
}


def git(*args: str) -> str:
    result = subprocess.run(['git', '-C', str(ROOT), *args], capture_output=True, text=True)
    return result.stdout if result.returncode == 0 else ''


class Checker:
    def __init__(self) -> None:
        self.errors: list[str] = []

    def expect(self, ok: bool, message: str) -> None:
        if not ok:
            self.errors.append(message)

    def tag_at_head(self, tag: str) -> None:
        head = git('rev-parse', 'HEAD').strip()
        target = git('rev-parse', '-q', '--verify', f'refs/tags/{tag}^{{commit}}').strip()
        self.expect(target == head, f'tag {tag} does not point at HEAD ({target or "missing"} != {head})')

    def schema(self) -> None:
        try:
            changes = schema.plan()
        except schema.SchemaError as error:
            self.errors.append(f'schema: {error}')
            return
        for description, _ in changes:
            self.errors.append(f'not in sync with the last migration (run `make sync`): {description}')

    def client(self, component: str, version: str) -> None:
        path = CLIENTS[component]
        manifest = json.loads((ROOT / '.release-please-manifest.json').read_text())
        self.expect(manifest.get(path) == version,
                    f'.release-please-manifest.json has {path} = {manifest.get(path)!r}, not {version!r}')
        changelog = (ROOT / path / 'CHANGELOG.md').read_text()
        self.expect(re.search(rf'^##\s+\[?{re.escape(version)}\]?[\s(]', changelog, re.M) is not None,
                    f'{path}/CHANGELOG.md has no entry for {version}')
        getattr(self, component.replace('-', '_'))(version)

    def postgremq_go(self, version: str) -> None:
        # The Go module proxy adds the repository's LICENSE to a nested module
        # without one, so a local zip (and its go.sum hash) would differ.
        self.expect((ROOT / 'postgremq-go' / 'LICENSE').exists(), 'postgremq-go/LICENSE is missing')
        go_mod = (ROOT / 'postgremq-go' / 'go.mod').read_text()
        others = re.findall(r'^\s*(?:require\s+)?(postgremq\.dev/\S+) v', go_mod, re.M)
        self.expect(not others, f'postgremq-go/go.mod requires {others}: the Go module must build on its own')

    def rust(self, version: str) -> None:
        cargo = (ROOT / 'postgremq-rs' / 'Cargo.toml').read_text()
        declared = re.search(r'^version = "([^"]+)"', cargo, re.M)
        self.expect(declared is not None and declared.group(1) == version,
                    f'postgremq-rs/Cargo.toml version is {declared and declared.group(1)!r}, not {version!r}')
        lock = (ROOT / 'postgremq-rs' / 'Cargo.lock').read_text()
        self.expect(f'name = "postgremq"\nversion = "{version}"' in lock,
                    f'postgremq-rs/Cargo.lock does not record postgremq {version}')

    def npm(self, version: str) -> None:
        package = json.loads((ROOT / 'postgremq-ts' / 'package.json').read_text())
        self.expect(package.get('version') == version,
                    f'postgremq-ts/package.json version is {package.get("version")!r}, not {version!r}')
        lock = json.loads((ROOT / 'postgremq-ts' / 'package-lock.json').read_text())
        self.expect(lock.get('version') == version and lock.get('packages', {}).get('', {}).get('version') == version,
                    f'postgremq-ts/package-lock.json does not record {version}')

    def every_client_with_new_schema(self, manifest: dict[str, str]) -> None:
        """A release that brings mq/vN must release every client: they all
        embed the new migration (scripts/schema.py), and a client left out
        would keep shipping the previous schema. A client is in the release
        when its manifest version has no tag yet."""
        try:
            last = schema.latest()
        except schema.SchemaError:
            return  # reported by schema()
        released = [int(t[4:]) for t in git('tag', '--merged', 'HEAD', '--list', 'mq/v*').split()
                    if re.fullmatch(r'mq/v\d+', t)]
        if released and last <= max(released):
            return
        for component, path in CLIENTS.items():
            tag = f'{component}/v{manifest[path]}'
            self.expect(not git('rev-parse', '-q', '--verify', f'refs/tags/{tag}').strip(),
                        f'this release brings mq v{last} but not {component} (still {tag}), which would keep '
                        f'shipping the previous schema: add a feat/fix commit for it (the migration commit '
                        f'was probably not feat/fix)')

    def mq(self, version: str) -> None:
        try:
            last = schema.latest()
        except schema.SchemaError:
            return  # reported by schema()
        self.expect(version == str(last), f'mq/v{version}: the last migration is {last}, not {version}')


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    target = parser.add_mutually_exclusive_group(required=True)
    target.add_argument('tag', nargs='?', help='a release tag, e.g. rust/v0.2.0 or mq/v3')
    target.add_argument('--all', action='store_true', help='every client at its manifest version, and mq')
    parser.add_argument('--tag-exists', action='store_true', help='also require the tag to point at HEAD')
    args = parser.parse_args()

    checker = Checker()
    checker.schema()
    if args.all:
        manifest = json.loads((ROOT / '.release-please-manifest.json').read_text())
        for component, path in CLIENTS.items():
            checker.client(component, manifest[path])
        checker.every_client_with_new_schema(manifest)
        label = 'all components'
    else:
        component, sep, version = args.tag.rpartition('/v')
        if not sep or component not in [*CLIENTS, 'mq']:
            parser.error(f'not a release tag: {args.tag} '
                         f'(want mq/vN or one of {", ".join(f"{c}/vX.Y.Z" for c in CLIENTS)})')
        if component == 'mq':
            checker.mq(version)
        else:
            checker.client(component, version)
        if args.tag_exists:
            checker.tag_at_head(args.tag)
        label = args.tag

    if checker.errors:
        print(f'{label}: not a consistent release:', file=sys.stderr)
        for error in checker.errors:
            print(f'  - {error}', file=sys.stderr)
        return 1
    print(f'{label}: consistent release')
    return 0


if __name__ == '__main__':
    sys.exit(main())
