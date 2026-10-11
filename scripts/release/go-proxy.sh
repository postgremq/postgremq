#!/usr/bin/env bash
# Builds a GOPROXY file tree holding the Go client module
# (postgremq.dev/postgremq-go), so an external module resolves it with
# GOWORK=off and without the vanity domain:
#
#   scripts/release/go-proxy.sh OUT_DIR VERSION
#
# If the release tag postgremq-go/VERSION exists in this branch's history, the
# module is built from that tag (its go.sum hash is the published one).
# Otherwise it is built from the working tree's files that git tracks or would
# track. Prints the env to use.
set -euo pipefail

root=$(git rev-parse --show-toplevel)
out=$(mkdir -p "$1" && cd "$1" && pwd)
version=$2
dir=postgremq-go
module=postgremq.dev/postgremq-go
tag="$dir/$version"

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
src="$work/src"
mkdir -p "$src"
if git -C "$root" rev-parse -q --verify "refs/tags/$tag" >/dev/null &&
  git -C "$root" merge-base --is-ancestor "$tag" HEAD; then
  git -C "$root" archive "$tag" "$dir" | tar -x -C "$src" --strip-components=1
  echo "go-proxy: $module@$version from tag $tag" >&2
else
  (cd "$root" && git ls-files -z --cached --others --exclude-standard -- "$dir" | while IFS= read -r -d '' file; do
    [ -e "$file" ] || continue
    mkdir -p "$src/$(dirname "${file#"$dir"/}")"
    cp "$file" "$src/${file#"$dir"/}"
  done)
  echo "go-proxy: $module@$version from the working tree (unreleased)" >&2
fi

(cd "$root/scripts/release/gomodproxy" && GOWORK=off go run . -out "$out" "$module@$version=$src")

# The module cache treats a version as immutable, but an unreleased version
# built from the working tree changes, and a cached dev build could shadow a
# later tag of the same version. Drop this repository's modules from it so
# the proxy's copies are used (they are small).
modcache=$(go env GOMODCACHE)
for cached in "$modcache/postgremq.dev" "$modcache/cache/download/postgremq.dev"; do
  if [ -e "$cached" ]; then
    chmod -R u+w "$cached"
    rm -rf "$cached"
  fi
done

echo "GOPROXY=file://$out,https://proxy.golang.org,direct"
echo "GONOSUMDB=postgremq.dev"
echo "GOWORK=off"
