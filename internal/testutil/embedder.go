// Package testutil provides small test-only helpers shared across this
// module's test suites. It's a regular (non-_test.go) package specifically
// so other packages' test files can import it — Go doesn't allow importing
// symbols from another package's _test.go files.
package testutil

import (
	"os/exec"
	"path/filepath"
	"testing"
)

// BuildMockEmbedder compiles tools/mock_embedder into a temp binary and
// returns its path. Several packages (embedder, indexer, retrieval) need a
// real embedder subprocess for their tests — IngestDocument and Search both
// take a concrete *embedder.Embedder, not an interface, so a Go-level fake
// isn't an option for those. mock_embedder is deterministic (FNV hash, no
// model, no Python) and already exists for exactly this purpose.
//
// Uses the module import path rather than a relative filesystem path so it
// works regardless of which package's test is invoking it — `go test`
// runs with the working directory set to the package under test, not the
// module root, but `go build` resolves import paths independent of that.
func BuildMockEmbedder(t *testing.T) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "mock_embedder")
	cmd := exec.Command("go", "build", "-o", bin, "pipeline/tools/mock_embedder")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build mock_embedder: %v\n%s", err, out)
	}
	return bin
}
