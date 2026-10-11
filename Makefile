# PostgreMQ development tasks. `make` lists them.
#
# Schema changes: `make migration name=<snake_case>`, write the migration and
# the same change in mq/sql/latest.sql, then `make sync` and commit everything
# in one commit. CI runs `make check`.

.DEFAULT_GOAL := help
PYTHON ?= python3
# Base branch for the "released migrations are unchanged" check.
BASE ?= origin/main

.PHONY: help migration sync check hooks test test-sql test-go test-rs test-ts

## migration name=<snake_case>: add mq/migrations/NNNNNN_<name>.up.sql with the next number
migration:
	@$(PYTHON) scripts/schema.py new "$(name)"

## sync: point every client at the last migration (Go copy, Rust/TS pins, latest.sql)
sync:
	@$(PYTHON) scripts/schema.py sync

## check: schema in sync and migrations on BASE (default origin/main) unchanged, as CI checks
check:
	@$(PYTHON) scripts/schema.py check
	@scripts/release/check_migrations_immutable.sh $(BASE)

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
