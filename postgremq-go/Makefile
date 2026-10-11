.PHONY: test test-coverage test-short check lint lint-fix fmt vet clean help build-cli

# Default target
.DEFAULT_GOAL := help

## test: Run all tests
test:
	go test -v ./...

## test-coverage: Run tests with coverage
test-coverage:
	go test -v -race -coverprofile=coverage.out -covermode=atomic ./...
	go tool cover -html=coverage.out -o coverage.html

## test-short: Run short tests only
test-short:
	go test -short -v ./...

## check: Run basic checks using built-in Go tools (no external tools needed)
check:
	@echo "Running go fmt..."
	@test -z "$$(gofmt -l .)" || (echo "Code is not formatted. Run 'make fmt' to fix." && gofmt -l . && exit 1)
	@echo "Running go vet..."
	@go vet ./...
	@echo "Running staticcheck (if available)..."
	@which staticcheck > /dev/null && staticcheck ./... || echo "staticcheck not installed, skipping (optional)"
	@echo "✓ All checks passed!"

## lint: Run golangci-lint (requires golangci-lint to be installed)
lint:
	@which golangci-lint > /dev/null || (echo "golangci-lint not installed. Run 'brew install golangci-lint' or use 'make check' instead." && exit 1)
	golangci-lint run --timeout=5m

## lint-fix: Run golangci-lint and auto-fix issues
lint-fix:
	@which golangci-lint > /dev/null || (echo "golangci-lint not installed. Run 'brew install golangci-lint' or use 'make fmt' instead." && exit 1)
	golangci-lint run --fix --timeout=5m

## fmt: Format code
fmt:
	go fmt ./...

## vet: Run go vet
vet:
	go vet ./...

## clean: Clean build artifacts and test files
clean:
	rm -f coverage.out coverage.html
	go clean -testcache

## build-cli: Build the postgremq CLI tool
build-cli:
	@echo "Building postgremq CLI..."
	@go build -o bin/postgremq ./cmd/postgremq
	@echo "✓ CLI built at bin/postgremq"

## help: Show this help message
help:
	@echo "PostgreMQ Go Client - Available targets:"
	@echo ""
	@sed -n 's/^##//p' ${MAKEFILE_LIST} | column -t -s ':' | sed -e 's/^/ /'
	@echo ""
	@echo "Note: 'make check' uses built-in Go tools (no installation needed)"
	@echo "      'make lint' requires golangci-lint (brew install golangci-lint)"
