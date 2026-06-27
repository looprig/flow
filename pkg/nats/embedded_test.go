package nats

import (
	"errors"
	"testing"
	"time"
)

// These tests cover the PURE, server-free surface of the embedded helper: option
// application and validation (a bad readiness timeout must be rejected fail-secure,
// CLAUDE.md). The actual boot path (Start + ReadyForConnections + InProcessConn)
// is exercised under the integration tag in embedded_integration_test.go, because
// it stands up a real in-process JetStream server.

func TestEmbeddedOptionsApply(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		opts        []EmbeddedOption
		wantTimeout time.Duration
		wantDirSet  bool // a non-empty StoreDir was provided
	}{
		{
			name:        "defaults applied when no options",
			opts:        nil,
			wantTimeout: defaultReadyTimeout,
			wantDirSet:  false,
		},
		{
			name:        "WithReadyTimeout overrides default",
			opts:        []EmbeddedOption{WithReadyTimeout(2 * time.Second)},
			wantTimeout: 2 * time.Second,
			wantDirSet:  false,
		},
		{
			name:        "WithStoreDir sets the store directory",
			opts:        []EmbeddedOption{WithStoreDir("/some/dir")},
			wantTimeout: defaultReadyTimeout,
			wantDirSet:  true,
		},
		{
			name:        "later option wins for timeout",
			opts:        []EmbeddedOption{WithReadyTimeout(time.Second), WithReadyTimeout(5 * time.Second)},
			wantTimeout: 5 * time.Second,
			wantDirSet:  false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := defaultEmbeddedConfig()
			for _, opt := range tt.opts {
				opt(&cfg)
			}
			if cfg.readyTimeout != tt.wantTimeout {
				t.Errorf("readyTimeout = %v, want %v", cfg.readyTimeout, tt.wantTimeout)
			}
			if (cfg.storeDir != "") != tt.wantDirSet {
				t.Errorf("storeDir set = %v, want %v (storeDir=%q)", cfg.storeDir != "", tt.wantDirSet, cfg.storeDir)
			}
		})
	}
}

func TestEmbeddedRejectsBadReadyTimeout(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		timeout time.Duration
		wantErr bool
	}{
		{name: "positive timeout is accepted", timeout: time.Second, wantErr: false},
		{name: "zero timeout rejected", timeout: 0, wantErr: true},
		{name: "negative timeout rejected", timeout: -time.Second, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := defaultEmbeddedConfig()
			WithReadyTimeout(tt.timeout)(&cfg)
			err := cfg.validate()
			if (err != nil) != tt.wantErr {
				t.Fatalf("validate() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				var ce *EmbeddedConfigError
				if !errors.As(err, &ce) {
					t.Fatalf("validate() error = %v, want *EmbeddedConfigError", err)
				}
			}
		})
	}
}
