.PHONY: test fmt fmt-check architecture lint vuln secure fuzz build check

# A parent go.work at ../go.work does not list this repo, so every go/gofmt
# command must run with GOWORK=off (workspace mode also disables -mod=vendor).
export GOWORK := off

# Build from the vendored dependency tree: offline, reproducible, auditable
# (every dep's source lives in vendor/ and shows up in review diffs). Export
# -mod=vendor so a stray global GOFLAGS can't switch off the vendored tree.
export GOFLAGS := -mod=vendor

# This module's own package dirs, excluding vendor/ and any nested modules
# (go list ./... stops at module boundaries and skips vendor). Scopes gofmt/gosec.
# GOWORK=off is inlined here because make's `export` applies only to recipes,
# not to this parse-time $(shell); the parent go.work omits this repo, so a bare
# `go list` would fail and leave GO_DIRS empty.
GO_DIRS := $(shell GOWORK=off go list -f '{{.Dir}}' ./...)

test:
	go test -race ./...

fmt:
	gofmt -w $(GO_DIRS)

fmt-check:
	@unformatted=$$(gofmt -l $(GO_DIRS)); \
	if [ -n "$$unformatted" ]; then \
		echo "gofmt needed (run 'make fmt'):"; echo "$$unformatted"; exit 1; \
	fi

build:
	CGO_ENABLED=0 go build -trimpath ./...

architecture:
	go test . -run TestCoreHasNoConcreteBackendDependencies

lint: fmt-check
	go vet ./...
	go tool staticcheck ./...
	# gosec is not module-aware (its ./... is a filesystem walk that would descend
	# into nested modules like the future pkg/nats under -mod=vendor); scope it to
	# this module's own dirs via GO_DIRS. vet/staticcheck are module-aware.
	go tool gosec $(GO_DIRS)

vuln:
	go mod verify
	go tool govulncheck ./...

secure: lint vuln

fuzz:
	@echo "Usage: go test -fuzz=FuzzXxx ./path/to/pkg -fuzztime=30s"

check: architecture secure build test
