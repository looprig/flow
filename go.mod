module github.com/looprig/flow/store

go 1.26.4

tool (
	github.com/securego/gosec/v2/cmd/gosec
	golang.org/x/vuln/cmd/govulncheck
	honnef.co/go/tools/cmd/staticcheck
)

require (
	github.com/looprig/core v0.5.0
	github.com/looprig/flow v0.0.0
	github.com/looprig/fsstore v0.0.0
	github.com/looprig/storage v0.3.0
	github.com/securego/gosec/v2 v2.28.0
	golang.org/x/vuln v1.6.0
	honnef.co/go/tools v0.7.0
)

replace (
	github.com/looprig/flow => ..
	github.com/looprig/fsstore => ../../fsstore
	github.com/looprig/storage => ../../storage
)
