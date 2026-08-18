// Unit tests for the broker's replicated-state logic that don't need a
// running Raft cluster: command encode/decode and applyLoop's effect on
// state given a synthetic commitCh. applyLoop never touches s.rf, so these
// run against a bare brokerServer with no Raft instance at all.
package main

import (
	"testing"
	"time"

	"pipeline/raft"
)

func TestEncodeDecodeSubmitRoundTrip(t *testing.T) {
	cmd := encodeSubmit("task-1", `{"chunk_id":"doc1-0","text":"hello"}`)
	op, id, data := decodeCommand(cmd)
	if op != "submit" {
		t.Errorf("op = %q, want %q", op, "submit")
	}
	if id != "task-1" {
		t.Errorf("id = %q, want %q", id, "task-1")
	}
	if data != `{"chunk_id":"doc1-0","text":"hello"}` {
		t.Errorf("data = %q, want the original payload back", data)
	}
}

func TestEncodeDecodeCompleteRoundTrip(t *testing.T) {
	cmd := encodeComplete("task-1", "embed: subprocess closed stdout unexpectedly")
	op, id, data := decodeCommand(cmd)
	if op != "complete" {
		t.Errorf("op = %q, want %q", op, "complete")
	}
	if id != "task-1" {
		t.Errorf("id = %q, want %q", id, "task-1")
	}
	if data != "embed: subprocess closed stdout unexpectedly" {
		t.Errorf("data = %q, want the original error message back", data)
	}
}

// TestDecodeCommandIgnoresRaftNoop verifies that Raft's own "noop"
// leader-election marker (raft.go's becomeLeader appends one every time a
// node wins an election) decodes to an empty op rather than erroring or
// panicking, since it flows through the exact same commitCh/applyLoop path
// as real commands.
func TestDecodeCommandIgnoresRaftNoop(t *testing.T) {
	op, id, data := decodeCommand("noop")
	if op != "" || id != "" || data != "" {
		t.Errorf("decodeCommand(%q) = (%q, %q, %q), want all empty", "noop", op, id, data)
	}
}

func TestDecodeCommandRejectsMalformed(t *testing.T) {
	cases := []string{"", "submit", "submit:onlyid", "submit:id:not-valid-base64!!!"}
	for _, c := range cases {
		op, id, data := decodeCommand(c)
		if op != "" || id != "" || data != "" {
			t.Errorf("decodeCommand(%q) = (%q, %q, %q), want all empty", c, op, id, data)
		}
	}
}

// newBareBrokerServer builds a brokerServer with no Raft instance — enough
// to exercise applyLoop in isolation, since applyLoop never touches s.rf.
func newBareBrokerServer() *brokerServer {
	return &brokerServer{
		submitted:  make(map[string]string),
		done:       make(map[string]bool),
		errors:     make(map[string]string),
		inflight:   make(map[string]*inFlightEntry),
		pendingOps: make(map[int64]*pendingOp),
		leaderID:   -1,
	}
}

func TestApplyLoopSubmit(t *testing.T) {
	s := newBareBrokerServer()
	commitCh := make(chan raft.ApplyMsg, 10)
	go s.applyLoop(commitCh)

	commitCh <- raft.ApplyMsg{Index: 1, Term: 1, Command: encodeSubmit("t1", "payload-1")}
	commitCh <- raft.ApplyMsg{Index: 2, Term: 1, Command: encodeSubmit("t2", "payload-2")}
	close(commitCh)

	if !pollUntil(func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return len(s.submitted) == 2
	}, time.Second, time.Millisecond) {
		t.Fatal("applyLoop did not apply both submits in time")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.submitted["t1"] != "payload-1" || s.submitted["t2"] != "payload-2" {
		t.Errorf("submitted = %v, want t1/t2 with their payloads", s.submitted)
	}
	// submittedOrder must reflect commit order, since Poll's derived pending
	// view depends on it for FIFO behavior.
	if len(s.submittedOrder) != 2 || s.submittedOrder[0] != "t1" || s.submittedOrder[1] != "t2" {
		t.Errorf("submittedOrder = %v, want [t1 t2]", s.submittedOrder)
	}
}

func TestApplyLoopSubmitIsIdempotent(t *testing.T) {
	s := newBareBrokerServer()
	commitCh := make(chan raft.ApplyMsg, 10)
	go s.applyLoop(commitCh)

	// Same task_id committed twice (e.g. a retried proposal) must not
	// duplicate the submittedOrder entry.
	commitCh <- raft.ApplyMsg{Index: 1, Term: 1, Command: encodeSubmit("t1", "first")}
	commitCh <- raft.ApplyMsg{Index: 2, Term: 1, Command: encodeSubmit("t1", "second")}
	close(commitCh)

	if !pollUntil(func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return len(s.submittedOrder) > 0
	}, time.Second, time.Millisecond) {
		t.Fatal("applyLoop did not apply the submit in time")
	}
	time.Sleep(20 * time.Millisecond) // let the (already-applied) second entry through too

	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.submittedOrder) != 1 {
		t.Fatalf("submittedOrder = %v, want exactly one entry for a re-submitted id", s.submittedOrder)
	}
	if s.submitted["t1"] != "first" {
		t.Errorf(`submitted["t1"] = %q, want "first" (first write wins, not overwritten)`, s.submitted["t1"])
	}
}

func TestApplyLoopCompleteClearsInflight(t *testing.T) {
	s := newBareBrokerServer()
	s.inflight["t1"] = &inFlightEntry{workerID: "w1", lastHeartbeat: time.Now()}

	commitCh := make(chan raft.ApplyMsg, 10)
	go s.applyLoop(commitCh)
	commitCh <- raft.ApplyMsg{Index: 1, Term: 1, Command: encodeComplete("t1", "")}
	close(commitCh)

	if !pollUntil(func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.done["t1"]
	}, time.Second, time.Millisecond) {
		t.Fatal("applyLoop did not apply the complete in time")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if _, stillInflight := s.inflight["t1"]; stillInflight {
		t.Error("inflight entry for a completed task was not cleared by applyLoop")
	}
	if s.errors["t1"] != "" {
		t.Errorf(`errors["t1"] = %q, want empty for a successful completion`, s.errors["t1"])
	}
}

func TestApplyLoopWakesPendingOp(t *testing.T) {
	s := newBareBrokerServer()
	op := &pendingOp{term: 1, ch: make(chan struct{})}
	s.pendingOps[1] = op

	commitCh := make(chan raft.ApplyMsg, 10)
	go s.applyLoop(commitCh)
	commitCh <- raft.ApplyMsg{Index: 1, Term: 1, Command: encodeSubmit("t1", "payload")}
	close(commitCh)

	select {
	case <-op.ch:
		// woken, as expected
	case <-time.After(time.Second):
		t.Fatal("applyLoop did not close the pendingOp channel for the matching index")
	}

	if op.term != 1 {
		t.Errorf("op.term = %d, want 1 (the committed entry's term)", op.term)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if _, stillPending := s.pendingOps[1]; stillPending {
		t.Error("pendingOps[1] was not removed after being woken")
	}
}
