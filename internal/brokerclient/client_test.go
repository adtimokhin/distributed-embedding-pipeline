// Tests for Client's redirect/retry logic. These run against real (loopback
// TCP) fake pb.BrokerServer implementations rather than a real broker
// cluster — Client.clientFor always dials a plain address with no injectable
// transport, so bufconn isn't an option without changing production code;
// loopback TCP on an OS-assigned port exercises the exact same code path
// Client uses in production, just against a scripted server instead of a
// real one.
package brokerclient

import (
	"context"
	"net"
	"sync"
	"testing"

	pb "pipeline/proto"

	"google.golang.org/grpc"
)

// fakeBroker is a scriptable pb.BrokerServer: either "I'm the leader, here's
// your answer" or "not leader, redirect to X".
type fakeBroker struct {
	pb.UnimplementedBrokerServer

	mu           sync.Mutex
	ok           bool
	redirectAddr string
	taskID       string
	hasTask      bool
	calls        int
}

func (f *fakeBroker) snapshot() (ok bool, redirect, taskID string, hasTask bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	return f.ok, f.redirectAddr, f.taskID, f.hasTask
}

func (f *fakeBroker) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func (f *fakeBroker) Submit(_ context.Context, _ *pb.SubmitRequest) (*pb.SubmitResponse, error) {
	ok, redirect, taskID, _ := f.snapshot()
	if !ok {
		return &pb.SubmitResponse{Ok: false, RedirectAddr: redirect}, nil
	}
	return &pb.SubmitResponse{Ok: true, TaskId: taskID}, nil
}

func (f *fakeBroker) Poll(_ context.Context, _ *pb.PollRequest) (*pb.PollResponse, error) {
	ok, redirect, taskID, hasTask := f.snapshot()
	if !ok {
		return &pb.PollResponse{HasTask: false, RedirectAddr: redirect}, nil
	}
	if !hasTask {
		return &pb.PollResponse{HasTask: false}, nil
	}
	return &pb.PollResponse{HasTask: true, Task: &pb.Task{Id: taskID}}, nil
}

func (f *fakeBroker) Complete(_ context.Context, _ *pb.CompleteRequest) (*pb.CompleteResponse, error) {
	ok, redirect, _, _ := f.snapshot()
	return &pb.CompleteResponse{Ok: ok, RedirectAddr: redirect}, nil
}

func (f *fakeBroker) Heartbeat(_ context.Context, _ *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	ok, redirect, _, _ := f.snapshot()
	return &pb.HeartbeatResponse{Ok: ok, RedirectAddr: redirect}, nil
}

func (f *fakeBroker) GetResult(_ context.Context, _ *pb.GetResultRequest) (*pb.GetResultResponse, error) {
	f.snapshot()
	return &pb.GetResultResponse{Done: true}, nil
}

// startFakeBroker serves fb on a real loopback TCP port and returns its
// address. The server is stopped automatically when the test ends.
func startFakeBroker(t *testing.T, fb *fakeBroker) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	pb.RegisterBrokerServer(srv, fb)
	go srv.Serve(lis) //nolint:errcheck
	t.Cleanup(srv.Stop)
	return lis.Addr().String()
}

// deadAddr returns a loopback address nothing is listening on, for
// exercising Client's connection-error handling.
func deadAddr(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := lis.Addr().String()
	lis.Close()
	return addr
}

func TestClientSucceedsAgainstLeader(t *testing.T) {
	leader := startFakeBroker(t, &fakeBroker{ok: true, taskID: "t1"})
	c := New([]string{leader})

	taskID, err := c.Submit(context.Background(), "payload")
	if err != nil {
		t.Fatalf("Submit error: %v", err)
	}
	if taskID != "t1" {
		t.Errorf("taskID = %q, want %q", taskID, "t1")
	}
}

func TestClientFollowsRedirectToKnownAddress(t *testing.T) {
	follower := &fakeBroker{}
	leader := &fakeBroker{ok: true, taskID: "t1"}

	followerAddr := startFakeBroker(t, follower)
	leaderAddr := startFakeBroker(t, leader)
	follower.mu.Lock()
	follower.redirectAddr = leaderAddr
	follower.mu.Unlock()

	c := New([]string{followerAddr, leaderAddr})

	taskID, err := c.Submit(context.Background(), "payload")
	if err != nil {
		t.Fatalf("Submit error: %v", err)
	}
	if taskID != "t1" {
		t.Errorf("taskID = %q, want %q", taskID, "t1")
	}
	if follower.callCount() != 1 {
		t.Errorf("follower.calls = %d, want exactly 1 (should not be retried)", follower.callCount())
	}

	// The client should now jump straight to the known leader address on
	// its next call — no wasted hop back through the follower.
	if _, err := c.Submit(context.Background(), "payload-2"); err != nil {
		t.Fatalf("second Submit error: %v", err)
	}
	if follower.callCount() != 1 {
		t.Errorf("follower.calls after second Submit = %d, want still 1 (client should have stuck with the known leader)", follower.callCount())
	}
}

// TestClientRoundRobinsOnUnknownRedirect covers the Docker-vs-host hostname
// mismatch this package's doc comment describes: a redirect target the
// client doesn't recognize (not in its own configured address list) must
// not be dialed directly — Client falls back to round-robining its own list.
func TestClientRoundRobinsOnUnknownRedirect(t *testing.T) {
	follower := &fakeBroker{ok: false, redirectAddr: "broker-internal-hostname:9000"}
	leader := &fakeBroker{ok: true, taskID: "t1"}

	followerAddr := startFakeBroker(t, follower)
	leaderAddr := startFakeBroker(t, leader)

	c := New([]string{followerAddr, leaderAddr})

	taskID, err := c.Submit(context.Background(), "payload")
	if err != nil {
		t.Fatalf("Submit error: %v", err)
	}
	if taskID != "t1" {
		t.Errorf("taskID = %q, want %q", taskID, "t1")
	}
}

func TestClientAdvancesPastDeadAddress(t *testing.T) {
	dead := deadAddr(t)
	leaderAddr := startFakeBroker(t, &fakeBroker{ok: true, taskID: "t1"})

	c := New([]string{dead, leaderAddr})

	taskID, err := c.Submit(context.Background(), "payload")
	if err != nil {
		t.Fatalf("Submit error: %v", err)
	}
	if taskID != "t1" {
		t.Errorf("taskID = %q, want %q", taskID, "t1")
	}
}

func TestClientReturnsErrorWhenClusterUnreachable(t *testing.T) {
	dead1 := deadAddr(t)
	dead2 := deadAddr(t)

	c := New([]string{dead1, dead2})

	if _, err := c.Submit(context.Background(), "payload"); err == nil {
		t.Fatal("Submit against an entirely dead cluster returned nil error, want an error")
	}
}

func TestClientPollNoTaskIsNotAnError(t *testing.T) {
	leaderAddr := startFakeBroker(t, &fakeBroker{ok: true, hasTask: false})
	c := New([]string{leaderAddr})

	task, ok, err := c.Poll(context.Background(), "worker-1")
	if err != nil {
		t.Fatalf("Poll error: %v", err)
	}
	if ok {
		t.Errorf("Poll: ok = true (task=%v), want false for an empty queue", task)
	}
}

func TestClientGetResultNeverRedirects(t *testing.T) {
	// A GetResult call against a "follower" (ok=false) should still work,
	// since GetResult is served locally by any node — the redirect fields
	// on GetResultResponse don't even exist in the proto.
	addr := startFakeBroker(t, &fakeBroker{ok: false})
	c := New([]string{addr})

	done, _, err := c.GetResult(context.Background(), "t1")
	if err != nil {
		t.Fatalf("GetResult error: %v", err)
	}
	if !done {
		t.Error("GetResult: done = false, want true (fake always reports done)")
	}
}
