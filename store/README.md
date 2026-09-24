# flowstore

`github.com/looprig/flow/store` (package `flowstore`) is the optional adapter
from the neutral `storage.Ledger` interface to Flow's durable
`flow.CheckpointStore`. It persists a versioned, bounded JSON envelope under the
canonical ledger name `flow/runs/<graph-run-id>` and translates definite ledger
conflicts into Flow `*flow.RevisionConflictError`s. Other failures are returned
as `*flow.StoreError`.

It is a nested module inside the `flow` repository, released on its own cadence
at `store/vX.Y.Z` tags; its version is independent of the root `flow` module's.

## Install

```sh
go get github.com/looprig/flow/store@latest
```

## Usage

Construction is backend-neutral:

```go
import flowstore "github.com/looprig/flow/store"

checkpoints := flowstore.New(ledger) // any storage.Ledger; returns nil for a nil ledger
runner, err := g.Compile(entryID, finishID, flow.WithStore(checkpoints))
```

The caller supplies the concrete ledger: for example an `fsstore` ledger for a
local deployment, or any other backend that conforms to the `storage` contract.
This module does not import or recommend a particular backend.

The adapter rejects unknown JSON fields, trailing data, oversized records
(over 1 MiB), excessive nesting, run-identity mismatches, and
sequence/revision mismatches.

## Where it sits

Tier 2 of the Looprig workspace. Production dependencies: `flow` and
`storage`. `core` and `fsstore` are test-only; `fsstore` is used only by the
`integration`-tagged test.

## Development

The Go baseline is 1.26.8. Run from this directory:

```sh
GOWORK=off go test ./...
GOWORK=off go test -tags integration ./...   # adds the fsstore round-trip test
make check    # fmt-check, vet, test, race, staticcheck, gosec, govulncheck, build
```

## License

Apache License 2.0. See [LICENSE](LICENSE).
