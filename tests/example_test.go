package examples_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

const offlineCommand = "GOWORK=off go test -race ./..."

type manifest struct {
	SchemaVersion int           `json:"schemaVersion"`
	Repository    string        `json:"repository"`
	ProofSources  []proofSource `json:"proofSources"`
	Examples      []example     `json:"examples"`
}

type proofSource struct {
	ID     string `json:"id"`
	Type   string `json:"type"`
	Path   string `json:"path"`
	Symbol string `json:"symbol"`
}

type example struct {
	ID             string            `json:"id"`
	Ecosystem      string            `json:"ecosystem"`
	Owner          string            `json:"owner"`
	SourcePath     string            `json:"sourcePath"`
	Availability   string            `json:"availability"`
	Versions       map[string]string `json:"versions"`
	OfflineCommand string            `json:"offlineCommand"`
	Assertion      string            `json:"assertion"`
	WorkflowPath   string            `json:"workflowPath"`
	JobID          string            `json:"jobId"`
	Cleanup        string            `json:"cleanup"`
	LiveGate       any               `json:"liveGate"`
	ProofIDs       []string          `json:"proofIds"`
}

type artifact struct {
	path   string
	symbol string
}

var expectedArtifacts = map[string]artifact{
	"example-flow-graph-deterministic-execution": {
		path:   "examples/graph/example_test.go",
		symbol: "Example_deterministicExecution",
	},
	"example-flow-branch-conditional-routing": {
		path:   "examples/branch/example_test.go",
		symbol: "Example_conditionalRouting",
	},
	"example-flow-interrupt-typed-awaiting": {
		path:   "examples/interrupt/example_test.go",
		symbol: "Example_typedInterruption",
	},
	"example-flow-resume-checkpoint-continuation": {
		path:   "examples/resume/example_test.go",
		symbol: "Example_checkpointResume",
	},
}

func TestRunnableExamplesExist(t *testing.T) {
	t.Parallel()
	for id, want := range expectedArtifacts {
		id, want := id, want
		t.Run(id, func(t *testing.T) {
			t.Parallel()
			if _, err := os.Stat(filepath.Join("..", want.path)); err != nil {
				t.Fatalf("runnable example %q: %v", want.path, err)
			}
		})
	}
}

func TestDocsExamplesArtifacts(t *testing.T) {
	t.Parallel()
	root := ".."
	data, err := os.ReadFile(filepath.Join(root, "testdata/docs/examples.json"))
	if err != nil {
		t.Fatal(err)
	}
	var got manifest
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	if got.SchemaVersion != 1 || got.Repository != "flow" {
		t.Fatalf("manifest identity = (%d, %q)", got.SchemaVersion, got.Repository)
	}
	if len(got.Examples) != len(expectedArtifacts) {
		t.Fatalf("manifest has %d examples, want %d", len(got.Examples), len(expectedArtifacts))
	}

	proofs := make(map[string]proofSource, len(got.ProofSources))
	for _, proof := range got.ProofSources {
		if !strings.HasPrefix(proof.ID, "example-flow-") || proof.Path == "" || proof.Symbol == "" {
			t.Errorf("invalid proof source: %#v", proof)
		}
		if proof.Type != "executable-fixture" && proof.Type != "test" {
			t.Errorf("proof source %q type = %q", proof.ID, proof.Type)
		}
		if strings.Contains(proof.Path, "#") {
			t.Errorf("proof source %q path contains a symbol fragment: %q", proof.ID, proof.Path)
		}
		if _, duplicate := proofs[proof.ID]; duplicate {
			t.Errorf("duplicate proof source %q", proof.ID)
		}
		proofs[proof.ID] = proof
		if _, err := os.Stat(filepath.Join(root, proof.Path)); err != nil {
			t.Errorf("proof source %q: %v", proof.ID, err)
		}
	}
	if len(proofs) != len(expectedArtifacts)+1 {
		t.Errorf("manifest has %d proof sources, want %d", len(proofs), len(expectedArtifacts)+1)
	}
	contract := proofs["example-flow-artifacts-contract-test"]
	if contract.Type != "test" || contract.Path != "tests/example_test.go" || contract.Symbol != "TestDocsExamplesArtifacts" {
		t.Errorf("contract proof = %#v", contract)
	}

	workflowData, err := os.ReadFile(filepath.Join(root, ".github/workflows/docs-examples.yml"))
	if err != nil {
		t.Fatal(err)
	}
	workflow := string(workflowData)
	for _, required := range []string{
		"docs-examples:",
		"run: " + offlineCommand,
		"run: GOWORK=off make check",
		"GOCACHE:",
	} {
		if !strings.Contains(workflow, required) {
			t.Errorf("workflow does not contain %q", required)
		}
	}

	seen := make(map[string]struct{}, len(got.Examples))
	for _, item := range got.Examples {
		want, ok := expectedArtifacts[item.ID]
		if !ok {
			t.Errorf("unexpected example ID %q", item.ID)
			continue
		}
		if _, duplicate := seen[item.ID]; duplicate {
			t.Errorf("duplicate example ID %q", item.ID)
		}
		seen[item.ID] = struct{}{}
		if item.Ecosystem != "go" || item.Owner != "flow" || item.Availability != "source-workspace" {
			t.Errorf("example %q identity fields are invalid", item.ID)
		}
		if !reflect.DeepEqual(item.Versions, map[string]string{"github.com/looprig/flow": "source-workspace"}) {
			t.Errorf("example %q versions = %#v", item.ID, item.Versions)
		}
		if item.OfflineCommand != offlineCommand || item.Assertion == "" || item.Cleanup == "" {
			t.Errorf("example %q execution metadata is incomplete", item.ID)
		}
		if item.SourcePath != want.path || item.WorkflowPath != ".github/workflows/docs-examples.yml" || item.JobID != "docs-examples" || item.LiveGate != nil {
			t.Errorf("example %q source/automation metadata is invalid", item.ID)
		}
		if _, err := os.Stat(filepath.Join(root, item.SourcePath)); err != nil {
			t.Errorf("example %q source: %v", item.ID, err)
		}
		sourceProofID := item.ID + "-source"
		if !reflect.DeepEqual(item.ProofIDs, []string{sourceProofID, "example-flow-artifacts-contract-test"}) {
			t.Errorf("example %q proof IDs = %#v", item.ID, item.ProofIDs)
		}
		proof := proofs[sourceProofID]
		if proof.Type != "executable-fixture" || proof.Path != want.path || proof.Symbol != want.symbol {
			t.Errorf("example %q source proof = %#v", item.ID, proof)
		}
	}
	if len(seen) != len(expectedArtifacts) {
		t.Errorf("manifest covers %d examples, want %d", len(seen), len(expectedArtifacts))
	}
}
