#!/usr/bin/env python3
"""Release guard: run before Release Please creates tags and GitHub releases.

Release Please tags the merge commit of every merged release PR still
labelled `autorelease: pending` (normally the one combined release PR), and
the release workflow then tags mq/vX.Y.Z on the same commit. A tag publishes (the
Go module is published by the tag alone), so each such commit must already
have passed validation, and must be a consistent release
(scripts/release/verify_release.py --all). A later passing commit must not
authorize an earlier failed one.

    check_pending_releases.py --branch main --validated-sha "$GITHUB_SHA"

--validated-sha is the commit this workflow run validated. Any other merge
commit counts as validated only if an earlier run of this workflow
(--workflow, default release.yml) finished the `Validated` job successfully on
it. A merge commit newer than --validated-sha belongs to its own, later run:
this run then tags nothing (Release Please would tag every pending PR) and
leaves it to that run. Needs `gh` authenticated (GH_TOKEN) and
GITHUB_REPOSITORY. Writes `tag=true|false` to $GITHUB_OUTPUT when set.
Exits 1 if a pending release must not be tagged.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
VALIDATED_JOB = 'Validated'


def run(*args: str, cwd: Path = ROOT) -> str:
    return subprocess.run(args, cwd=cwd, check=True, capture_output=True, text=True).stdout


def gh_json(*args: str):
    return json.loads(run('gh', *args))


def newer_than(sha: str, validated: str) -> bool:
    """Whether `sha` is not in the history of `validated` (a later commit)."""
    if subprocess.run(['git', '-C', str(ROOT), 'cat-file', '-e', f'{sha}^{{commit}}'],
                      capture_output=True).returncode != 0:
        return True  # not fetched: pushed after this run's checkout
    return subprocess.run(['git', '-C', str(ROOT), 'merge-base', '--is-ancestor', sha, validated],
                          capture_output=True).returncode != 0


def set_output(tag: bool) -> None:
    if 'GITHUB_OUTPUT' in os.environ:
        with open(os.environ['GITHUB_OUTPUT'], 'a') as out:
            out.write(f'tag={"true" if tag else "false"}\n')


def validated_elsewhere(repo: str, workflow: str, sha: str, wait_seconds: int) -> bool:
    """Whether a run of `workflow` on `sha` passed its Validated job. While
    such a run is still in progress (an earlier release PR merged moments
    before this run's commit), waits up to `wait_seconds` for it."""
    deadline = time.monotonic() + wait_seconds
    while True:
        runs = gh_json('api', f'repos/{repo}/actions/workflows/{workflow}/runs?head_sha={sha}&per_page=100')
        pending = False
        for workflow_run in runs.get('workflow_runs', []):
            jobs = gh_json('api', f'repos/{repo}/actions/runs/{workflow_run["id"]}/jobs?per_page=100&filter=all')
            validated = [job for job in jobs.get('jobs', []) if job['name'] == VALIDATED_JOB]
            if any(job['conclusion'] == 'success' for job in validated):
                return True
            if workflow_run['status'] != 'completed' and not any(job['status'] == 'completed' for job in validated):
                pending = True
        if not pending or time.monotonic() >= deadline:
            return False
        print(f'Waiting for the validation of {sha[:12]} to finish...', flush=True)
        time.sleep(30)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--branch', required=True, help='the release branch (Release Please target)')
    parser.add_argument('--validated-sha', required=True, help='the commit this run validated')
    parser.add_argument('--workflow', default='release.yml', help='the workflow whose Validated job counts')
    parser.add_argument('--wait', type=int, default=45 * 60,
                        help='seconds to wait for an earlier commit\'s validation still in progress')
    args = parser.parse_args()
    repo = os.environ['GITHUB_REPOSITORY']

    prs = gh_json('pr', 'list', '--repo', repo, '--base', args.branch, '--state', 'merged',
                  '--label', 'autorelease: pending', '--json', 'number,title,headRefName,mergeCommit,url')
    if not prs:
        print('No merged release PRs are waiting to be tagged.')
        set_output(True)
        return 0

    problems = []
    deferred = []
    for pr in prs:
        sha = (pr.get('mergeCommit') or {}).get('oid')
        label = f'#{pr["number"]} ({pr["title"]}, {sha and sha[:12]})'
        if not sha:
            problems.append(f'{label}: cannot tell its merge commit')
            continue
        if sha != args.validated_sha and newer_than(sha, args.validated_sha):
            deferred.append(f'{label}: merged after this run\'s commit; its own run validates and tags it')
            continue
        if sha != args.validated_sha and not validated_elsewhere(repo, args.workflow, sha, args.wait):
            problems.append(
                f'{label}: its merge commit has not passed validation. If the failure was spurious, '
                f're-run that commit\'s {args.workflow} run; when its {VALIDATED_JOB} job succeeds, the next '
                f'run tags it. If the commit is broken, do not tag it: remove the "autorelease: pending" '
                f'label from {pr["url"]}, revert the release commit, and fix forward; Release Please then '
                f'proposes a new release PR.')
            continue
        with tempfile.TemporaryDirectory() as tmp:
            worktree = Path(tmp) / 'source'
            run('git', 'worktree', 'add', '--detach', str(worktree), sha)
            try:
                check = subprocess.run([sys.executable, 'scripts/release/verify_release.py', '--all'],
                                       cwd=worktree, capture_output=True, text=True)
                print(check.stdout, end='')
                if check.returncode != 0:
                    problems.append(f'{label}: not a consistent release:\n{check.stderr}')
                else:
                    print(f'{label}: validated and consistent; will be tagged')
            finally:
                run('git', 'worktree', 'remove', '--force', str(worktree))

    if problems:
        print('Release guard: not tagging. Correct the pending release(s) first:', file=sys.stderr)
        for problem in problems:
            print(f'  - {problem}', file=sys.stderr)
        set_output(False)
        return 1
    if deferred:
        print('Release guard: not tagging in this run (a later run will):')
        for item in deferred:
            print(f'  - {item}')
        set_output(False)
        return 0
    set_output(True)
    return 0


if __name__ == '__main__':
    sys.exit(main())
