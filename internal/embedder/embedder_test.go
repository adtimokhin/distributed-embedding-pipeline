package embedder

import (
	"fmt"
	"sync"
	"testing"

	"pipeline/internal/testutil"
)

func TestEmbedReturnsAVector(t *testing.T) {
	bin := testutil.BuildMockEmbedder(t)
	emb, err := Start(bin)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer emb.Close() //nolint:errcheck

	resp, err := emb.Embed(Request{ChunkID: "c1", Text: "hello world"})
	if err != nil {
		t.Fatalf("Embed: %v", err)
	}
	if resp.ChunkID != "c1" {
		t.Errorf("ChunkID = %q, want %q", resp.ChunkID, "c1")
	}
	if len(resp.Vector) == 0 {
		t.Error("Vector is empty, want a non-empty embedding")
	}
}

func TestEmbedIsDeterministic(t *testing.T) {
	bin := testutil.BuildMockEmbedder(t)
	emb, err := Start(bin)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer emb.Close() //nolint:errcheck

	r1, err := emb.Embed(Request{ChunkID: "a", Text: "same text"})
	if err != nil {
		t.Fatalf("Embed 1: %v", err)
	}
	r2, err := emb.Embed(Request{ChunkID: "b", Text: "same text"})
	if err != nil {
		t.Fatalf("Embed 2: %v", err)
	}
	if len(r1.Vector) != len(r2.Vector) {
		t.Fatalf("vector lengths differ: %d vs %d", len(r1.Vector), len(r2.Vector))
	}
	for i := range r1.Vector {
		if r1.Vector[i] != r2.Vector[i] {
			t.Fatalf("vectors for identical text differ at index %d: %v vs %v", i, r1.Vector[i], r2.Vector[i])
		}
	}
}

// TestEmbedSequentialCallsReuseSubprocess is the actual point of this
// package: one subprocess serves many Embed calls, not one spawned per call.
func TestEmbedSequentialCallsReuseSubprocess(t *testing.T) {
	bin := testutil.BuildMockEmbedder(t)
	emb, err := Start(bin)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer emb.Close() //nolint:errcheck

	pid := emb.cmd.Process.Pid
	for i := 0; i < 5; i++ {
		if _, err := emb.Embed(Request{ChunkID: fmt.Sprintf("c%d", i), Text: "x"}); err != nil {
			t.Fatalf("Embed %d: %v", i, err)
		}
		if emb.cmd.Process.Pid != pid {
			t.Fatalf("subprocess PID changed between calls (was %d, now %d) — a new process was spawned", pid, emb.cmd.Process.Pid)
		}
	}
}

// TestEmbedConcurrentCallsDoNotInterleave is a regression test for the bug
// this package's mutex fixes: before it existed, concurrent callers writing
// to and reading from the same subprocess's stdin/stdout could receive each
// other's responses. Each goroutine embeds text containing its own goroutine
// index and asserts the response's ChunkID matches what it sent — if
// requests/responses ever interleaved, some goroutine would see a mismatch.
func TestEmbedConcurrentCallsDoNotInterleave(t *testing.T) {
	bin := testutil.BuildMockEmbedder(t)
	emb, err := Start(bin)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer emb.Close() //nolint:errcheck

	const goroutines = 20
	const callsEach = 20

	var wg sync.WaitGroup
	errCh := make(chan error, goroutines*callsEach)

	for g := 0; g < goroutines; g++ {
		g := g
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < callsEach; i++ {
				chunkID := fmt.Sprintf("g%d-%d", g, i)
				resp, err := emb.Embed(Request{ChunkID: chunkID, Text: "some text"})
				if err != nil {
					errCh <- fmt.Errorf("goroutine %d call %d: %w", g, i, err)
					return
				}
				if resp.ChunkID != chunkID {
					errCh <- fmt.Errorf("goroutine %d call %d: got response for chunk_id %q, want %q (interleaved response)", g, i, resp.ChunkID, chunkID)
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errCh)

	for err := range errCh {
		t.Error(err)
	}
}
