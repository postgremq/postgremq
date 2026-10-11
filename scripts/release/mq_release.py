#!/usr/bin/env python3
"""Releases of mq, the SQL schema: mq/vN, where N is the last migration.

mq is not a Release Please component (its version is not computed from
commits). It is released together with the clients: when a release PR is
merged and the last migration on that commit is newer than the latest mq/v*
tag, mq/vN is tagged on the same commit. Release notes come from the
conventional commits that touched mq/ since the previous mq release.

    mq_release.py pending [REF]              # print N if mq/vN is still to be released at REF
    mq_release.py notes [REF]                # the release notes mq/vN would get
    mq_release.py comment --branch main      # keep a comment on the open release PR saying what mq release it brings
    mq_release.py release [--sha SHA]        # create mq/vN and its GitHub release, if pending

`release` uses the newest client release commit in HEAD's history (after
fetching tags) unless --sha is given, so a run that failed after tagging the
clients is repaired by the next one. `comment` and `release` need `gh`
(GH_TOKEN) and GITHUB_REPOSITORY; `release` writes `tag=mq/vN` (or `tag=`) to
$GITHUB_OUTPUT when set. Release assets (latest.sql and the migrations) are
uploaded by publish.yml.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
UP = re.compile(r'^(\d+)_(.+)\.up\.sql$')
SUBJECT = re.compile(r'^(?P<type>\w+)(?:\((?P<scope>[^)]*)\))?(?P<bang>!)?: (?P<subject>.+)$')
BREAKING = re.compile(r'^BREAKING[ -]CHANGE: (.+?)(?=^\S[\w-]*: |\Z)', re.M | re.S)
SECTIONS = [('feat', 'Features'), ('fix', 'Bug Fixes'), ('perf', 'Performance Improvements'),
            ('revert', 'Reverts'), ('deps', 'Dependencies')]
MARKER = '<!-- postgremq:mq-release -->'
CLIENT_TAGS = ('postgremq-go', 'rust', 'npm')


def git(*args: str) -> str:
    return subprocess.run(['git', '-C', str(ROOT), *args], check=True, capture_output=True, text=True).stdout


def gh(*args: str) -> str:
    return subprocess.run(['gh', *args], check=True, capture_output=True, text=True).stdout


def last_migration(ref: str) -> int:
    names = git('ls-tree', '--name-only', f'{ref}:mq/migrations').split()
    return max(int(m.group(1)) for m in map(UP.match, names) if m)


def previous_release(ref: str) -> tuple[int, str] | None:
    """The newest mq/vN tag in REF's history, as (N, tag)."""
    tags = [t for t in git('tag', '--merged', ref, '--list', 'mq/v*').split() if re.fullmatch(r'mq/v\d+', t)]
    return max(((int(t[4:]), t) for t in tags), default=None)


def pending(ref: str) -> int | None:
    number = last_migration(ref)
    previous = previous_release(ref)
    return number if previous is None or number > previous[0] else None


def since(ref: str) -> str:
    """Where this release's history starts: the previous mq tag, else the
    Release Please bootstrap commit."""
    previous = previous_release(ref)
    if previous:
        return previous[1]
    return json.loads((ROOT / 'release-please-config.json').read_text())['bootstrap-sha']


def notes(ref: str) -> str:
    number = last_migration(ref)
    start = since(ref)
    repo = os.environ.get('GITHUB_REPOSITORY')
    log = git('log', '--format=%H%x1f%s%x1f%b%x1e', f'{start}..{ref}', '--', 'mq/')
    sections: dict[str, list[str]] = {}
    breaking: list[str] = []
    for record in filter(str.strip, log.split('\x1e')):
        sha, subject, body = record.strip('\n').split('\x1f')
        match = SUBJECT.match(subject)
        if not match:
            continue
        link = f'[{sha[:7]}](https://github.com/{repo}/commit/{sha})' if repo else sha[:7]
        scope = f'**{match["scope"]}:** ' if match['scope'] else ''
        entry = f'* {scope}{match["subject"]} ({link})'
        footer = BREAKING.search(body)
        if footer or match['bang']:
            breaking.append(f'* {scope}{" ".join((footer.group(1) if footer else match["subject"]).split())}')
        if match['type'] in dict(SECTIONS):
            sections.setdefault(match['type'], []).append(entry)

    added = [name for name in git('ls-tree', '--name-only', f'{ref}:mq/migrations').split()
             if (m := UP.match(name)) and int(m.group(1)) > (previous_release(ref) or (0, ''))[0]]
    lines = [f'Schema version **{number}**: what `postgremq.info()` reports as `schema_version` after '
             f'installing or upgrading to this release. Every client released with it embeds migrations 1..{number}.',
             '',
             'New migrations: ' + ', '.join(f'`{name}`' for name in added) + '.']
    if breaking:
        lines += ['', '### ⚠ BREAKING CHANGES', '', *breaking]
    for kind, title in SECTIONS:
        if kind in sections:
            lines += ['', f'### {title}', '', *sections[kind]]
    return '\n'.join(lines) + '\n'


def set_output(key: str, value: str) -> None:
    if 'GITHUB_OUTPUT' in os.environ:
        with open(os.environ['GITHUB_OUTPUT'], 'a') as out:
            out.write(f'{key}={value}\n')


def cmd_comment(branch: str) -> int:
    repo = os.environ['GITHUB_REPOSITORY']
    head = f'release-please--branches--{branch}'
    prs = json.loads(gh('pr', 'list', '--repo', repo, '--head', head, '--state', 'open', '--json', 'number,headRefOid'))
    if not prs:
        print(f'No open release PR ({head}).')
        return 0
    pr = prs[0]
    # Tags too: this run may have just released mq.
    git('fetch', '-q', '--tags', 'origin', f'+refs/heads/{head}:refs/remotes/origin/{head}')
    ref = f'origin/{head}'
    number = pending(ref)
    if number is None:
        body = (f'{MARKER}\nNo mq release: the last migration ({last_migration(ref)}) '
                'is already released.\n')
    else:
        body = (f'{MARKER}\n## mq v{number}\n\nMerging this PR also releases **mq v{number}** '
                f'(tag `mq/v{number}`) on the same commit.\n\n{notes(ref)}')
    comments = json.loads(gh('api', f'repos/{repo}/issues/{pr["number"]}/comments?per_page=100'))
    existing = next((c for c in comments if c['body'].startswith(MARKER)), None)
    if existing and existing['body'].strip() == body.strip():
        print(f'#{pr["number"]}: mq comment is current.')
    elif existing:
        gh('api', '-X', 'PATCH', f'repos/{repo}/issues/comments/{existing["id"]}', '-f', f'body={body}')
        print(f'#{pr["number"]}: updated the mq comment.')
    else:
        gh('pr', 'comment', str(pr['number']), '--repo', repo, '--body', body)
        print(f'#{pr["number"]}: added the mq comment.')
    return 0


def newest_client_release() -> str | None:
    """The commit of the newest client release tag in HEAD's history."""
    tags = [tag for prefix in CLIENT_TAGS for tag in git('tag', '--merged', 'HEAD', '--list', f'{prefix}/v*').split()]
    if not tags:
        return None
    commits = sorted({line.strip() for line in git('rev-parse', *(f'{tag}^{{commit}}' for tag in tags)).split()})
    # The commits that are no other's ancestor: one on a linear main.
    newest = git('merge-base', '--independent', *commits).split()
    if len(newest) != 1:
        raise SystemExit(f'mq: cannot tell the newest client release among {newest}')
    return newest[0]


def cmd_release(sha: str | None) -> int:
    repo = os.environ['GITHUB_REPOSITORY']
    if sha is None:
        git('fetch', '-q', '--tags', 'origin')
        sha = newest_client_release()
        if sha is None:
            print('mq: no client release yet; nothing to release.')
            set_output('tag', '')
            return 0
    number = pending(sha)
    if number is None:
        print(f'mq: nothing to release at {sha[:12]} (migration {last_migration(sha)} is already released).')
        set_output('tag', '')
        return 0
    tag = f'mq/v{number}'
    # The release API ignores --target for an existing tag: never attach the
    # release to a tag made elsewhere (another branch, by hand).
    if git('ls-remote', '--tags', 'origin', f'refs/tags/{tag}').strip():
        raise SystemExit(f'mq: {tag} already exists but is not in the history of {sha[:12]}; '
                         'delete the stray tag (and its release) and re-run')
    with tempfile.NamedTemporaryFile('w', suffix='.md', delete=False) as file:
        file.write(notes(sha))
    gh('release', 'create', tag, '--repo', repo, '--target', sha, '--title', f'mq: v{number}',
       '--notes-file', file.name)
    print(f'Released {tag} at {sha[:12]}.')
    set_output('tag', tag)
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    commands = parser.add_subparsers(dest='command', required=True)
    for name in ('pending', 'notes'):
        sub = commands.add_parser(name)
        sub.add_argument('ref', nargs='?', default='HEAD')
    sub = commands.add_parser('comment')
    sub.add_argument('--branch', required=True)
    sub = commands.add_parser('release')
    sub.add_argument('--sha', help='the release commit (default: the newest client release)')
    args = parser.parse_args()

    if args.command == 'pending':
        number = pending(args.ref)
        if number is not None:
            print(number)
        return 0
    if args.command == 'notes':
        print(notes(args.ref), end='')
        return 0
    if args.command == 'comment':
        return cmd_comment(args.branch)
    return cmd_release(args.sha)


if __name__ == '__main__':
    sys.exit(main())
