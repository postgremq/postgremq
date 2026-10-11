#!/usr/bin/env bash
# Fails if this branch modifies, deletes or renames a migration that already
# exists on BASE (default origin/main). A migration's number is the schema
# version postgremq.info() reports, so once merged it must never change:
# change the schema with a new migration instead.
#
#   scripts/release/check_migrations_immutable.sh [BASE]
#
# Until the first mq release (no mq/v* tag in BASE's history), the initial
# migration 000001 may still be edited in place.
set -euo pipefail
base=${1:-origin/main}
root=$(git rev-parse --show-toplevel)
cd "$root"

changed=$(git diff --name-status --diff-filter=MDR "$base"...HEAD -- mq/migrations)
if [ -z "$(git tag --merged "$base" --list 'mq/v*')" ]; then
  changed=$(grep -v $'\tmq/migrations/000001_' <<<"$changed" || true)
fi
if [ -n "$changed" ]; then
  echo "Migrations already on $base must not change; add a new migration instead:" >&2
  sed 's/^/  /' <<<"$changed" >&2
  exit 1
fi
echo "No existing migration changed (base $base)."
