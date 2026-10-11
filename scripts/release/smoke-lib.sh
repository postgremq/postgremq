# Shared by the smoke-*.sh scripts. SMOKE_ADMIN_URL is a PostgreSQL 15+
# server URL whose user can create databases (default: a local postgres).
SMOKE_ADMIN_URL=${SMOKE_ADMIN_URL:-postgres://postgres:postgres@localhost:5432/postgres}

# smoke_database NAME_PREFIX -> creates a fresh database; sets SMOKE_DATABASE_URL
# and drops the database on exit.
smoke_database() {
  local name="$1_$(date +%s)_$RANDOM"
  psql "$SMOKE_ADMIN_URL" -qc "CREATE DATABASE $name"
  SMOKE_DATABASE_URL="${SMOKE_ADMIN_URL%/*}/$name"
  export SMOKE_DATABASE_URL
  SMOKE_DROP+=("$name")
}

SMOKE_DROP=()
smoke_cleanup() {
  for name in "${SMOKE_DROP[@]}"; do
    psql "$SMOKE_ADMIN_URL" -qc "DROP DATABASE IF EXISTS $name WITH (FORCE)" || true
  done
  [ -z "${SMOKE_WORK:-}" ] || rm -rf "$SMOKE_WORK"
}
trap smoke_cleanup EXIT

# Work outside the repository, so nothing resolves through sibling directories.
SMOKE_WORK=$(mktemp -d "${RUNNER_TEMP:-${TMPDIR:-/tmp}}/postgremq-smoke.XXXXXX")
