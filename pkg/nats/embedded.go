package nats

import (
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

// This file provides a small helper to run an in-process JetStream server (design
// §18.4 "local = embedded"): a single process can run durably without a separate
// nats-server, and every integration test in this module boots one. It is the
// ONLY responsibility of this file — option plumbing + boot + a connection seam +
// shutdown. Connecting to an EXTERNAL server (Tier C) is the Connect helper.

// defaultReadyTimeout bounds how long Embedded waits for the server to accept
// connections before failing (fail-secure: a server that never becomes ready
// must abort, not hang). It is generous enough for a cold JetStream file store.
const defaultReadyTimeout = 30 * time.Second

// EmbeddedConfigError reports an invalid embedded-server configuration (e.g. a
// non-positive readiness timeout). It is returned BEFORE any server is started,
// so a misconfiguration fails fast rather than booting a misbehaving server.
type EmbeddedConfigError struct{ Reason string }

// Error names the configuration problem.
func (e *EmbeddedConfigError) Error() string {
	return "flow/nats: invalid embedded server config: " + e.Reason
}

// EmbeddedStartError reports that the in-process JetStream server failed to start
// or did not become ready within the configured timeout. It wraps the underlying
// cause when there is one.
type EmbeddedStartError struct {
	Reason string
	Err    error
}

// Error names the start failure and any underlying cause.
func (e *EmbeddedStartError) Error() string {
	if e.Err != nil {
		return "flow/nats: embedded server failed to start: " + e.Reason + ": " + e.Err.Error()
	}
	return "flow/nats: embedded server failed to start: " + e.Reason
}

// Unwrap returns the underlying start cause so errors.Is/As can inspect it.
func (e *EmbeddedStartError) Unwrap() error { return e.Err }

// embeddedConfig holds the resolved options for an embedded server. It is
// unexported: callers configure it only through EmbeddedOption functions.
type embeddedConfig struct {
	storeDir     string        // JetStream file store dir; empty => a fresh temp dir
	readyTimeout time.Duration // bound on waiting for ReadyForConnections
}

// defaultEmbeddedConfig returns the baseline config (default readiness timeout,
// no explicit store dir — Embedded allocates a temp dir when storeDir is empty).
func defaultEmbeddedConfig() embeddedConfig {
	return embeddedConfig{readyTimeout: defaultReadyTimeout}
}

// validate rejects a config that would produce an unsafe or never-ready server.
func (c embeddedConfig) validate() error {
	if c.readyTimeout <= 0 {
		return &EmbeddedConfigError{Reason: "readiness timeout must be positive"}
	}
	return nil
}

// EmbeddedOption configures an embedded JetStream server.
type EmbeddedOption func(*embeddedConfig)

// WithStoreDir sets the JetStream file-store directory. When unset, Embedded
// creates a fresh temp dir (the caller can find it via the server, and it is the
// server's responsibility to write only within it).
func WithStoreDir(dir string) EmbeddedOption {
	return func(c *embeddedConfig) { c.storeDir = dir }
}

// WithReadyTimeout overrides how long Embedded waits for the server to become
// ready. Must be positive; a non-positive value is rejected by validate().
func WithReadyTimeout(d time.Duration) EmbeddedOption {
	return func(c *embeddedConfig) { c.readyTimeout = d }
}

// EmbeddedServer is a handle to a running in-process JetStream server. It exposes
// only what callers need: a client URL, an in-process (no-TCP) connection seam,
// and a clean shutdown. It owns the server's lifetime.
type EmbeddedServer struct {
	srv *server.Server
}

// Embedded starts an in-process JetStream server and waits (bounded) for it to be
// ready, returning a handle. The server listens on an ephemeral port (Port: -1)
// but the preferred connection path is InProcessConn (no TCP). It is fail-secure:
// a config error or a server that never becomes ready returns an error instead of
// a half-up handle.
func Embedded(opts ...EmbeddedOption) (*EmbeddedServer, error) {
	cfg := defaultEmbeddedConfig()
	for _, opt := range opts {
		opt(&cfg)
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	srv, err := server.NewServer(&server.Options{
		JetStream: true,
		Port:      -1, // ephemeral; we prefer the in-process conn anyway
		StoreDir:  cfg.storeDir,
		NoLog:     true,
		NoSigs:    true,
	})
	if err != nil {
		return nil, &EmbeddedStartError{Reason: "NewServer failed", Err: err}
	}

	srv.Start()
	if !srv.ReadyForConnections(cfg.readyTimeout) {
		// Tear the half-started server down so we never leak its goroutine.
		srv.Shutdown()
		srv.WaitForShutdown()
		return nil, &EmbeddedStartError{Reason: "server not ready within timeout"}
	}
	return &EmbeddedServer{srv: srv}, nil
}

// ClientURL returns the URL a TCP client would dial (Tier-C external connect path
// also uses URLs). In-process tests should prefer InProcessConn.
func (e *EmbeddedServer) ClientURL() string { return e.srv.ClientURL() }

// InProcessConn connects to the embedded server WITHOUT TCP, via
// nats.InProcessServer. This is the preferred path for tests and single-process
// durable runs: it avoids ephemeral-port flakiness under -race and parallelism.
func (e *EmbeddedServer) InProcessConn(opts ...nats.Option) (*nats.Conn, error) {
	opts = append([]nats.Option{nats.InProcessServer(e.srv)}, opts...)
	nc, err := nats.Connect("", opts...)
	if err != nil {
		return nil, &EmbeddedStartError{Reason: "in-process connect failed", Err: err}
	}
	return nc, nil
}

// Close shuts the server down and blocks until it is fully down, so no server
// goroutine outlives the handle. It is safe to defer.
func (e *EmbeddedServer) Close() {
	e.srv.Shutdown()
	e.srv.WaitForShutdown()
}

// Connect dials an EXTERNAL nats-server by URL (Tier C). It is a thin wrapper over
// nats.Connect so callers in this module have one place to add connection
// defaults later; for now it just forwards options.
func Connect(url string, opts ...nats.Option) (*nats.Conn, error) {
	nc, err := nats.Connect(url, opts...)
	if err != nil {
		return nil, &EmbeddedStartError{Reason: "connect to external server failed", Err: err}
	}
	return nc, nil
}
