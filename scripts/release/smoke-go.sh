#!/usr/bin/env bash
# Installs the Go client and its CLI as an external module would, with
# GOWORK=off from a module proxy (scripts/release/go-proxy.sh), and runs them
# against a fresh database:
#
#   scripts/release/smoke-go.sh [VERSION]
#
# VERSION (default v0.0.0-smoke, built from this tree) is the client version,
# e.g. v0.2.0 at its tag.
set -euo pipefail
root=$(git rev-parse --show-toplevel)
source "$root/scripts/release/smoke-lib.sh"
version=${1:-v0.0.0-smoke}

while IFS='=' read -r key value; do
  export "$key=$value"
done < <("$root/scripts/release/go-proxy.sh" "$SMOKE_WORK/proxy" "$version")

mkdir -p "$SMOKE_WORK/app"
cp "$root/scripts/release/smoke/go/main.go" "$SMOKE_WORK/app/"
cd "$SMOKE_WORK/app"
go mod init example.com/postgremq-smoke >/dev/null 2>&1
go get "postgremq.dev/postgremq-go@$version" github.com/jackc/pgx/v5
go mod tidy
smoke_database pmq_smoke_go
go run .

GOBIN="$SMOKE_WORK/bin" go install "postgremq.dev/postgremq-go/cmd/postgremq@$version"
smoke_database pmq_smoke_cli
"$SMOKE_WORK/bin/postgremq" migrate --dsn "$SMOKE_DATABASE_URL"
"$SMOKE_WORK/bin/postgremq" status --dsn "$SMOKE_DATABASE_URL" > "$SMOKE_WORK/status"
cat "$SMOKE_WORK/status"
grep -q "Database is up to date" "$SMOKE_WORK/status"
echo "go cli smoke ok: postgremq.dev/postgremq-go/cmd/postgremq@$version"
