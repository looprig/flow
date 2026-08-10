package flow_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/looprig/flow/pkg/controlplane"
	"github.com/looprig/flow/pkg/flow"
)

func TestCoreHasNoConcreteBackendDependencies(t *testing.T) {
	root, err := os.Getwd()
	if err != nil {
		t.Fatalf("working directory: %v", err)
	}
	forbiddenDirs := []string{"nats", "fsstore", "natsstore", "rclonestore"}
	for _, name := range forbiddenDirs {
		path := filepath.Join(root, "pkg", name)
		if _, statErr := os.Stat(path); statErr == nil {
			t.Fatalf("core contains concrete backend directory %q; keep adapters outside the core module", filepath.Join("pkg", name))
		} else if !os.IsNotExist(statErr) {
			t.Fatalf("stat concrete backend directory %q: %v", filepath.Join("pkg", name), statErr)
		}
	}

	cmd := exec.Command("go", "list", "-deps", "./pkg/flow/...", "./pkg/controlplane/...", "./pkg/registry/...", "./pkg/ingress/...")
	cmd.Dir = root
	cmd.Env = append(os.Environ(), "GOWORK=off")
	deps, err := cmd.Output()
	if err != nil {
		t.Fatalf("enumerate core dependencies: %v", err)
	}
	forbiddenImports := []string{
		"github.com/nats-io/nats.go",
		"github.com/looprig/storage",
		"github.com/looprig/flow/store",
		"github.com/looprig/natsstore",
		"github.com/looprig/fsstore",
		"github.com/looprig/rclonestore",
	}
	for _, importPath := range forbiddenImports {
		if strings.Contains(string(deps), importPath) {
			t.Fatalf("core dependency list contains concrete adapter %q", importPath)
		}
	}

	var _ flow.CheckpointStore = (*flow.MemStore)(nil)
	var _ flow.ControlPlane = (*controlplane.MemControlPlane)(nil)
}
