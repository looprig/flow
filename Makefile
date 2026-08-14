.PHONY: test fmt fmt-check architecture lint vuln secure fuzz build check

# This module does not vendor. go.mod pins exact versions and go.sum verifies
# their content hashes, which is what makes a build reproducible; a vendor tree
# adds only offline builds and source-level dependency diffs. It also actively
# misleads: a stale vendor/ is ignored under a go.work but silently satisfies a
# GOWORK=off build, so standalone verification tests the vendored copy rather
# than the version go.mod actually pins — which is precisely what standalone
# verification exists to check.
#
# Every target therefore runs standalone: ../go.work does list this repo, but
# these checks deliberately resolve dependencies from the versions go.mod pins
# rather than from sibling working copies. This module has no replace
# directives, so nothing here needs the workspace.
export GOWORK := off

# This module's own package dirs (go list ./... stops at nested module
# boundaries). GO_DIRS scopes gosec, which takes package dirs. Never hand
# GO_DIRS to gofmt: gofmt recurses into directory operands, and for a module
# with a root package GO_DIRS contains the module root, so gofmt would walk the
# entire tree. GO_FILES expands to each package dir's own .go files (including
# platform-specific ones go list omits for the host) without descending.
# GOWORK=off is inlined here because make's `export` applies only to recipes,
# not to this parse-time $(shell).
GO_DIRS := $(shell GOWORK=off go list -f '{{.Dir}}' ./...)
GO_FILES := $(foreach dir,$(GO_DIRS),$(wildcard $(dir)/*.go))

test:
	go test -race ./...

fmt:
	gofmt -w $(GO_FILES)

fmt-check:
	@unformatted=$$(gofmt -l $(GO_FILES)); \
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
	# into nested modules such as store/); scope it to this module's own dirs via
	# GO_DIRS. vet/staticcheck are module-aware.
	go tool gosec $(GO_DIRS)

vuln:
	go mod verify
	go tool govulncheck ./...

secure: lint vuln

fuzz:
	@echo "Usage: go test -fuzz=FuzzXxx ./path/to/pkg -fuzztime=30s"

check: architecture secure build test
