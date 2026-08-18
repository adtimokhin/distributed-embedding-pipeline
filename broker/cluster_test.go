// Integration tests for the Raft-replicated broker cluster.
//
// Before this file existed, the claims in README.md's "Design decisions
// actually implemented" section — leader-only Poll/Heartbeat, replicated
// Submit/Complete, automatic failover without losing submitted-but-not-done
// work — were verified exactly once, by hand, during development (start 3
// processes, kill -9 the leader, read logs). These tests automate that.
package main

import (
	"testing"
	"time"

	pb "pipeline/proto"
)

// requireSubmitOK submits a task and fails the test if the RPC errors or the
// broker reports it wasn't accepted (not leader).
func requireSubmitOK(t *testing.T, client pb.BrokerClient, payload string) *pb.SubmitResponse {
	t.Helper()
	resp, err := client.Submit(testCtx(t), &pb.SubmitRequest{Payload: payload})
	if err != nil {
		t.Fatalf("Submit RPC error: %v", err)
	}
	if !resp.Ok {
		t.Fatalf("Submit: got ok=false (redirect_addr=%q), want ok=true", resp.RedirectAddr)
	}
	return resp
}

func TestSubmitPollCompleteGetResultRoundTrip(t *testing.T) {
	tc := newTestCluster(t)
	leader := tc.waitForLeader(t)
	ctx := testCtx(t)

	submitResp := requireSubmitOK(t, leader.BrokerClient, `{"chunk_id":"doc1-0","text":"hello"}`)
	taskID := submitResp.TaskId
	if taskID == "" {
		t.Fatal("Submit returned empty task_id")
	}

	// GetResult before Complete: not done yet.
	getResp, err := leader.BrokerClient.GetResult(ctx, &pb.GetResultRequest{TaskId: taskID})
	if err != nil {
		t.Fatalf("GetResult RPC error: %v", err)
	}
	if getResp.Done {
		t.Fatal("GetResult: got done=true before Complete, want false")
	}

	// Poll should hand back exactly the task we submitted.
	pollResp, err := leader.BrokerClient.Poll(ctx, &pb.PollRequest{WorkerId: "w1"})
	if err != nil {
		t.Fatalf("Poll RPC error: %v", err)
	}
	if !pollResp.HasTask {
		t.Fatal("Poll: got has_task=false, want true")
	}
	if pollResp.Task.Id != taskID {
		t.Fatalf("Poll: got task id %q, want %q", pollResp.Task.Id, taskID)
	}

	// A second Poll must not hand out the same task again (it's now in-flight).
	pollResp2, err := leader.BrokerClient.Poll(ctx, &pb.PollRequest{WorkerId: "w2"})
	if err != nil {
		t.Fatalf("second Poll RPC error: %v", err)
	}
	if pollResp2.HasTask {
		t.Fatalf("second Poll: got a task (id=%q) while the only task is already in-flight", pollResp2.Task.Id)
	}

	// Heartbeat should succeed for the in-flight task.
	hbResp, err := leader.BrokerClient.Heartbeat(ctx, &pb.HeartbeatRequest{WorkerId: "w1", TaskId: taskID})
	if err != nil {
		t.Fatalf("Heartbeat RPC error: %v", err)
	}
	if !hbResp.Ok {
		t.Fatalf("Heartbeat: got ok=false (redirect_addr=%q), want ok=true", hbResp.RedirectAddr)
	}

	// Complete, then GetResult should report done.
	completeResp, err := leader.BrokerClient.Complete(ctx, &pb.CompleteRequest{TaskId: taskID, WorkerId: "w1", Error: ""})
	if err != nil {
		t.Fatalf("Complete RPC error: %v", err)
	}
	if !completeResp.Ok {
		t.Fatalf("Complete: got ok=false (redirect_addr=%q), want ok=true", completeResp.RedirectAddr)
	}

	getResp2, err := leader.BrokerClient.GetResult(ctx, &pb.GetResultRequest{TaskId: taskID})
	if err != nil {
		t.Fatalf("GetResult RPC error: %v", err)
	}
	if !getResp2.Done {
		t.Fatal("GetResult: got done=false after Complete, want true")
	}
	if getResp2.Error != "" {
		t.Fatalf("GetResult: got error=%q, want empty", getResp2.Error)
	}
}

// TestClusterRedirectsNonLeaderCalls verifies that Submit/Poll/Heartbeat/
// Complete against a follower report ok=false / has_task=false with
// redirect_addr pointing at the actual leader — the mechanism worker/producer
// rely on (via internal/brokerclient) to find the leader without being told
// directly which node it is.
func TestClusterRedirectsNonLeaderCalls(t *testing.T) {
	tc := newTestCluster(t)
	leader := tc.waitForLeader(t)
	ctx := testCtx(t)

	var follower *TestServer
	for _, ts := range tc.Servers {
		if ts != leader {
			follower = ts
			break
		}
	}
	if follower == nil {
		t.Fatal("could not find a follower")
	}
	wantRedirect := leader.srv.cfg.Nodes[leader.srv.id].ClientAddr

	submitResp, err := follower.BrokerClient.Submit(ctx, &pb.SubmitRequest{Payload: "x"})
	if err != nil {
		t.Fatalf("Submit RPC error: %v", err)
	}
	if submitResp.Ok {
		t.Fatal("Submit on follower: got ok=true, want false")
	}
	if submitResp.RedirectAddr != wantRedirect {
		t.Fatalf("Submit on follower: redirect_addr=%q, want %q", submitResp.RedirectAddr, wantRedirect)
	}

	pollResp, err := follower.BrokerClient.Poll(ctx, &pb.PollRequest{WorkerId: "w"})
	if err != nil {
		t.Fatalf("Poll RPC error: %v", err)
	}
	if pollResp.HasTask {
		t.Fatal("Poll on follower: got has_task=true, want false")
	}
	if pollResp.RedirectAddr != wantRedirect {
		t.Fatalf("Poll on follower: redirect_addr=%q, want %q", pollResp.RedirectAddr, wantRedirect)
	}

	hbResp, err := follower.BrokerClient.Heartbeat(ctx, &pb.HeartbeatRequest{WorkerId: "w", TaskId: "t"})
	if err != nil {
		t.Fatalf("Heartbeat RPC error: %v", err)
	}
	if hbResp.Ok {
		t.Fatal("Heartbeat on follower: got ok=true, want false")
	}
	if hbResp.RedirectAddr != wantRedirect {
		t.Fatalf("Heartbeat on follower: redirect_addr=%q, want %q", hbResp.RedirectAddr, wantRedirect)
	}

	completeResp, err := follower.BrokerClient.Complete(ctx, &pb.CompleteRequest{TaskId: "t", WorkerId: "w"})
	if err != nil {
		t.Fatalf("Complete RPC error: %v", err)
	}
	if completeResp.Ok {
		t.Fatal("Complete on follower: got ok=true, want false")
	}
	if completeResp.RedirectAddr != wantRedirect {
		t.Fatalf("Complete on follower: redirect_addr=%q, want %q", completeResp.RedirectAddr, wantRedirect)
	}
}

// TestGetResultServedByAnyNode verifies GetResult never redirects — any
// node answers from its own replicated state, the same stale-read tradeoff
// HW3 accepts for Get.
func TestGetResultServedByAnyNode(t *testing.T) {
	tc := newTestCluster(t)
	leader := tc.waitForLeader(t)
	ctx := testCtx(t)

	submitResp := requireSubmitOK(t, leader.BrokerClient, "payload")
	taskID := submitResp.TaskId

	if _, err := leader.BrokerClient.Complete(ctx, &pb.CompleteRequest{TaskId: taskID, WorkerId: "w"}); err != nil {
		t.Fatalf("Complete RPC error: %v", err)
	}

	// Every node — leader and followers — must eventually agree the task is done.
	for i, ts := range tc.Servers {
		ok := pollUntil(func() bool {
			resp, err := ts.BrokerClient.GetResult(ctx, &pb.GetResultRequest{TaskId: taskID})
			return err == nil && resp.Done
		}, 2*time.Second, 10*time.Millisecond)
		if !ok {
			t.Errorf("node %d: GetResult never reported done=true for a completed task", i)
		}
	}
}

// TestLeaderCrashDoesNotLoseSubmittedWork is the flagship test for this
// extension: a task that was submitted (and durably committed) but never
// completed must survive the leader crashing, and become pollable again on
// the new leader — the same guarantee manually verified during development
// by killing the leader process mid-run and watching the producer/worker
// recover. This also exercises refreshLeaderState's inflight-reset-on-
// becoming-leader path, since the new leader was a follower a moment ago.
func TestLeaderCrashDoesNotLoseSubmittedWork(t *testing.T) {
	tc := newTestCluster(t)
	leader := tc.waitForLeader(t)
	ctx := testCtx(t)

	// Submit two tasks; poll (but don't complete) one of them, so it's
	// in-flight on the doomed leader when it crashes.
	doneTask := requireSubmitOK(t, leader.BrokerClient, "will-be-completed")
	pendingTask := requireSubmitOK(t, leader.BrokerClient, "will-be-in-flight-then-crash")

	if _, err := leader.BrokerClient.Complete(ctx, &pb.CompleteRequest{TaskId: doneTask.TaskId, WorkerId: "w"}); err != nil {
		t.Fatalf("Complete RPC error: %v", err)
	}
	pollResp, err := leader.BrokerClient.Poll(ctx, &pb.PollRequest{WorkerId: "stalled-worker"})
	if err != nil {
		t.Fatalf("Poll RPC error: %v", err)
	}
	if !pollResp.HasTask || pollResp.Task.Id != pendingTask.TaskId {
		t.Fatalf("expected to poll pendingTask %q, got has_task=%v id=%q", pendingTask.TaskId, pollResp.HasTask, pollResp.Task.GetId())
	}

	oldLeaderID := tc.leaderID()
	tc.KillServer(oldLeaderID)

	newLeader := tc.waitForLeader(t)
	if newLeader == leader {
		t.Fatal("waitForLeader returned the killed server")
	}

	// The completed task's result must have survived (replicated before the crash).
	getResp, err := newLeader.BrokerClient.GetResult(ctx, &pb.GetResultRequest{TaskId: doneTask.TaskId})
	if err != nil {
		t.Fatalf("GetResult RPC error: %v", err)
	}
	if !getResp.Done {
		t.Fatal("completed task lost its done=true status after leader crash")
	}

	// The in-flight-but-never-completed task must become pollable again on
	// the new leader — its assignment to the stalled worker was leader-local
	// state that the crash correctly forgot, per the design in README.md.
	ok := pollUntil(func() bool {
		resp, err := newLeader.BrokerClient.Poll(ctx, &pb.PollRequest{WorkerId: "recovery-worker"})
		return err == nil && resp.HasTask && resp.Task.Id == pendingTask.TaskId
	}, 2*time.Second, 10*time.Millisecond)
	if !ok {
		t.Fatal("pending task was not re-offered by the new leader after the old leader crashed")
	}
}

// TestStaleHeartbeatTriggersReEnqueue exercises Stage 2's original
// at-least-once mechanism — a task whose worker stops heartbeating becomes
// available again — which the Raft-cluster tests above never touched (they
// only exercise the leader-crash path, a different way a task can need
// re-offering). checkStalledTasks is called directly instead of waiting
// CheckInterval/TaskTimeout of real wall-clock time.
func TestStaleHeartbeatTriggersReEnqueue(t *testing.T) {
	tc := newTestCluster(t)
	leader := tc.waitForLeader(t)
	ctx := testCtx(t)

	submitResp := requireSubmitOK(t, leader.BrokerClient, "payload")
	pollResp, err := leader.BrokerClient.Poll(ctx, &pb.PollRequest{WorkerId: "worker-a"})
	if err != nil {
		t.Fatalf("Poll RPC error: %v", err)
	}
	if !pollResp.HasTask || pollResp.Task.Id != submitResp.TaskId {
		t.Fatalf("expected to poll the submitted task, got has_task=%v", pollResp.HasTask)
	}

	// Simulate the polling worker going silent: back-date its last
	// heartbeat past TaskTimeout.
	leader.srv.mu.Lock()
	entry, ok := leader.srv.inflight[submitResp.TaskId]
	if !ok {
		leader.srv.mu.Unlock()
		t.Fatal("task is not tracked as inflight after Poll")
	}
	entry.lastHeartbeat = time.Now().Add(-(TaskTimeout + time.Second))
	leader.srv.mu.Unlock()

	leader.srv.checkStalledTasks()

	pollResp2, err := leader.BrokerClient.Poll(ctx, &pb.PollRequest{WorkerId: "worker-b"})
	if err != nil {
		t.Fatalf("second Poll RPC error: %v", err)
	}
	if !pollResp2.HasTask || pollResp2.Task.Id != submitResp.TaskId {
		t.Fatalf("stalled task was not re-offered: has_task=%v", pollResp2.HasTask)
	}
}

// TestSubmitTimesOutWithoutQuorum exercises proposeAndWait's timeout path,
// previously uncovered: a leader that can't reach a majority (isolated from
// both followers, so it locally still believes it's leader — nothing tells
// it otherwise — but can never commit anything new) must eventually give up
// and report ok=false rather than blocking forever. Takes ~5s (commitTimeout).
func TestSubmitTimesOutWithoutQuorum(t *testing.T) {
	tc := newTestCluster(t)
	leader := tc.waitForLeader(t)
	leaderID := tc.leaderID()

	tc.isolate(leaderID)

	start := time.Now()
	resp, err := leader.BrokerClient.Submit(testCtx(t), &pb.SubmitRequest{Payload: "payload"})
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("Submit RPC error: %v", err)
	}
	if resp.Ok {
		t.Fatal("Submit on an isolated (quorum-less) leader returned ok=true, want false after timing out")
	}
	if elapsed < 4*time.Second {
		t.Errorf("Submit returned after only %s, want it to have waited out the ~5s commit timeout", elapsed)
	}
}

func TestGetResultUnknownTaskIDIsNotDone(t *testing.T) {
	tc := newTestCluster(t)
	leader := tc.waitForLeader(t)

	resp, err := leader.BrokerClient.GetResult(testCtx(t), &pb.GetResultRequest{TaskId: "never-submitted"})
	if err != nil {
		t.Fatalf("GetResult RPC error: %v", err)
	}
	if resp.Done {
		t.Error("GetResult for an unknown task ID reported done=true, want false")
	}
}

// TestDoubleCompleteIsIdempotent covers the case the at-least-once design
// relies on: two workers both finish the same re-enqueued task and both
// call Complete. The second call must not error or corrupt state.
func TestDoubleCompleteIsIdempotent(t *testing.T) {
	tc := newTestCluster(t)
	leader := tc.waitForLeader(t)
	ctx := testCtx(t)

	submitResp := requireSubmitOK(t, leader.BrokerClient, "payload")

	resp1, err := leader.BrokerClient.Complete(ctx, &pb.CompleteRequest{TaskId: submitResp.TaskId, WorkerId: "w1"})
	if err != nil {
		t.Fatalf("first Complete RPC error: %v", err)
	}
	if !resp1.Ok {
		t.Fatal("first Complete: got ok=false, want true")
	}

	resp2, err := leader.BrokerClient.Complete(ctx, &pb.CompleteRequest{TaskId: submitResp.TaskId, WorkerId: "w2"})
	if err != nil {
		t.Fatalf("second Complete RPC error: %v", err)
	}
	if !resp2.Ok {
		t.Fatal("second Complete (duplicate) : got ok=false, want true (must be idempotent, not an error)")
	}

	getResp, err := leader.BrokerClient.GetResult(ctx, &pb.GetResultRequest{TaskId: submitResp.TaskId})
	if err != nil {
		t.Fatalf("GetResult RPC error: %v", err)
	}
	if !getResp.Done {
		t.Error("task not reported done after two Complete calls")
	}
}
