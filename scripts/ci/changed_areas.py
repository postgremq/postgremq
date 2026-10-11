#!/usr/bin/env python3
"""Which test areas a pull request touches, for .github/workflows/ci.yml.

    scripts/ci/changed_areas.py BASE_SHA HEAD_SHA

Prints `<area>=true|false` lines (appended to $GITHUB_OUTPUT when set) for
every area below. An area runs when a changed file matches one of its
patterns (`**` spans directories, `*` does not). A change to the CI wiring
itself (ci.yml or this script) runs every area.
"""

from __future__ import annotations

import os
import re
import subprocess
import sys

AREAS: dict[str, list[str]] = {
    'sql': ['mq/**', 'postgremq-go/migrations/**', 'postgremq-rs/Cargo.toml', 'postgremq-ts/package.json',
            'scripts/schema.py', 'scripts/release/check_migrations_immutable.sh', 'Makefile',
            '.github/workflows/sql-tests.yml'],
    'go': ['postgremq-go/**', 'mq/**', 'go.work', 'observability/**', 'scripts/release/**',
           '.github/workflows/go-tests.yml'],
    'rust': ['postgremq-rs/**', 'mq/sql/**', 'mq/migrations/**', 'observability/**', 'docs/observability.md',
             '.github/workflows/rust-tests.yml'],
    'typescript': ['postgremq-ts/**', 'mq/sql/**', 'mq/migrations/**', 'observability/**',
                   '.github/workflows/typescript-tests.yml'],
    'observability': ['observability/**', 'mq/**', 'postgremq-go/**', 'postgremq-ts/**', 'postgremq-rs/**',
                      'docs/observability.md', '.github/workflows/observability-tests.yml'],
    'packages': ['mq/**', 'postgremq-go/**', 'postgremq-rs/**', 'postgremq-ts/**', 'scripts/release/**',
                 'release-please-config.json', '.release-please-manifest.json', '.github/workflows/packages.yml'],
}
EVERYTHING = ['.github/workflows/ci.yml', 'scripts/ci/**']


def pattern(glob: str) -> re.Pattern[str]:
    regex = ''.join('.*' if part == '**' else '[^/]*' if part == '*' else re.escape(part)
                    for part in re.split(r'(\*\*|\*)', glob))
    return re.compile(f'^{regex}$')


def matches(files: list[str], globs: list[str]) -> bool:
    patterns = [pattern(g) for g in globs]
    return any(p.match(f) for f in files for p in patterns)


def main() -> int:
    base, head = sys.argv[1], sys.argv[2]
    files = subprocess.run(['git', 'diff', '--name-only', f'{base}...{head}'],
                           check=True, capture_output=True, text=True).stdout.split()
    print(f'{len(files)} changed file(s)', file=sys.stderr)
    everything = matches(files, EVERYTHING)
    lines = [f'{area}={"true" if everything or matches(files, globs) else "false"}' for area, globs in AREAS.items()]
    print('\n'.join(lines))
    if 'GITHUB_OUTPUT' in os.environ:
        with open(os.environ['GITHUB_OUTPUT'], 'a') as out:
            out.write('\n'.join(lines) + '\n')
    return 0


if __name__ == '__main__':
    sys.exit(main())
