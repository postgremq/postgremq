# PostgreMQ development tasks. `make` lists them.
#
# Schema changes: `make migration name=<snake_case> bump=<patch|minor|major>`,
# write the SQL below the change header it adds to the pending migration
# (mq/migrations/X.Y.Z.up.sql, the next mq release), make the same change in
# mq/sql/latest.sql, then `make sync` and commit everything in one commit.
# CI runs `make check`.

.DEFAULT_GOAL := help
PYTHON ?= python3
# Base branch for the "released migrations are unchanged" check.
BASE ?= origin/main

.PHONY: help migration sync check hooks test test-sql test-go test-rs test-ts

## migration name=<snake_case> bump=<patch/minor/major>: start a schema change in the pending migration
migration:
	@$(PYTHON) scripts/schema.py new "$(name)" "$(bump)"

## sync: settle the pending migration; copy migrations into every client; set latest.sql's version
sync:
	@$(PYTHON) scripts/schema.py sync

## check: schema consistent and in sync, released migrations unchanged since BASE (default origin/main)
check:
	@$(PYTHON) scripts/schema.py check --base $(BASE)

## hooks: install a pre-commit hook that refuses commits with the schema out of sync
hooks:
	git config core.hooksPath .githooks
	@echo "pre-commit hook installed (.githooks/pre-commit)"

## test: sync, then run every component's tests
test: test-sql test-go test-ts test-rs

## test-sql: SQL tests (testcontainers; needs Docker)
test-sql: sync
	cd mq && $(PYTHON) -m pytest tests/tests.py -v

## test-go: Go client and CLI tests (testcontainers; needs Docker)
test-go: sync
	cd postgremq-go && GOWORK=off go test ./...

## test-ts: TypeScript client tests (testcontainers; needs Docker)
test-ts: sync
	cd postgremq-ts && npm test

## test-rs: Rust client tests (integration tests need POSTGREMQ_TEST_DATABASE_URL)
test-rs: sync
	cd postgremq-rs && cargo test

help:
	@echo "PostgreMQ development tasks:"
	@sed -n 's/^## \([^:]*\): \(.*\)/  \1|\2/p' $(MAKEFILE_LIST) | column -t -s '|'
