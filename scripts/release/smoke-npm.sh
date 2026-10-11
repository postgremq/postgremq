#!/usr/bin/env bash
# Packs the npm package (or takes a tarball), installs it into a project
# outside the repository, type-checks a consumer with --strict against the
# published declarations, and runs it against a fresh database:
#
#   scripts/release/smoke-npm.sh [PACKAGE.tgz]
set -euo pipefail
root=$(git rev-parse --show-toplevel)
source "$root/scripts/release/smoke-lib.sh"

tarball=${1:-}
if [ -z "$tarball" ]; then
  (cd "$root/postgremq-ts" && npm run build >/dev/null && npm pack --pack-destination "$SMOKE_WORK" >/dev/null)
  tarball=$(ls "$SMOKE_WORK"/postgremq-*.tgz)
fi
# A file, not a pipe: grep -q would close it early and fail tar under pipefail.
tar -tzf "$tarball" > "$SMOKE_WORK/files"
grep -qx 'package/dist/migrations.generated.js' "$SMOKE_WORK/files" ||
  { echo "smoke-npm: $tarball lacks the embedded migrations" >&2; exit 1; }

mkdir -p "$SMOKE_WORK/app"
cd "$SMOKE_WORK/app"
npm init -y >/dev/null
npm install --no-audit --no-fund "$tarball" typescript@5 @types/node@22 >/dev/null
cp "$root/scripts/release/smoke/ts/smoke.ts" .
npx tsc --strict --module commonjs --target es2022 --esModuleInterop --skipLibCheck false \
  --types node --outDir dist smoke.ts
node -e "require('postgremq'); console.log('require ok')"
smoke_database pmq_smoke_npm
node dist/smoke.js
