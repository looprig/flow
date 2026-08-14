# flowstore

`github.com/looprig/flow/store` is the optional adapter from the neutral
`storage.Ledger` interface to Flow's durable `CheckpointStore`. It persists a
versioned, bounded JSON envelope under the canonical ledger name
`flow/runs/<graph-run-id>` and translates definite ledger conflicts into Flow
revision conflicts.

Construction is backend-neutral:

```go
checkpoints := flowstore.New(ledger)
```

Policy53 supplies an `fsstore.Ledger`; a distributed deployment may supply a
different conforming ledger. This module does not import or recommend NATS.

The adapter rejects unknown JSON fields, trailing data, oversized records,
excessive nesting, run-identity mismatches, and sequence/revision mismatches.
