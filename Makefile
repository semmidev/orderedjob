.PHONY: test bench cover run run-01 run-02 format lint all

GOLANGCI_LINT_CMD ?= $(shell command -v golangci-lint 2>/dev/null || echo "go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest")

test:
	go test ./... -v -race -count=1

bench:
	go test ./... -bench=. -benchmem -benchtime=2s

cover:
	go test ./... -coverprofile=coverage.out
	go tool cover -html=coverage.out -o coverage.html

format:
	goimports -w .

lint:
	$(GOLANGCI_LINT_CMD) run ./...

all: test bench format lint
