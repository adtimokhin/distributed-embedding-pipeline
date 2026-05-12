// Broker — HW4 embedding pipeline task broker.
//
// Stage 1: implement Submit, Poll, Complete, GetResult.
// Stage 2: implement Heartbeat and the background re-enqueue goroutine.

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	pb "pipeline/proto"

	"github.com/google/uuid"
	"google.golang.org/grpc"
)

// ──────────────────────────────────────────────────────────────────────────────
// Tunable constants
// ──────────────────────────────────────────────────────────────────────────────

const (
	TaskTimeout   = 10 * time.Second // Stage 2: re-enqueue after this much silence
	CheckInterval = 5 * time.Second  // Stage 2: how often to scan inflight tasks
)

// ──────────────────────────────────────────────────────────────────────────────
// Types
// ──────────────────────────────────────────────────────────────────────────────

type inFlightEntry struct {
	task          pb.Task
	workerID      string
	lastHeartbeat time.Time // Stage 2: updated by Heartbeat RPCs
}

type brokerServer struct {
	pb.UnimplementedBrokerServer

	mu       sync.Mutex
	pending  []pb.Task                 // tasks waiting to be assigned
	inflight map[string]*inFlightEntry // task_id → entry
	errors   map[string]string         // task_id → error string (empty = success)
	done     map[string]bool           // task_id → completed?
}

func newBrokerServer() *brokerServer {
	return &brokerServer{
		inflight: make(map[string]*inFlightEntry),
		errors:   make(map[string]string),
		done:     make(map[string]bool),
	}
}

// ──────────────────────────────────────────────────────────────────────────────
// RPC handlers — Stage 1
// ──────────────────────────────────────────────────────────────────────────────

// Submit enqueues a new task and returns its assigned ID.
func (s *brokerServer) Submit(_ context.Context, req *pb.SubmitRequest) (*pb.SubmitResponse, error) {
	// TODO (Stage 1): assign a UUID, append a Task to s.pending, return the ID.
	_ = uuid.New() // hint: use uuid.New().String()
	return nil, fmt.Errorf("Submit: not implemented")
}

// Poll returns one pending task to the calling worker, or has_task=false if
// the queue is empty.
func (s *brokerServer) Poll(_ context.Context, req *pb.PollRequest) (*pb.PollResponse, error) {
	// TODO (Stage 1): dequeue head of s.pending, move to s.inflight, return task.
	// If queue is empty, return &pb.PollResponse{HasTask: false}.
	return nil, fmt.Errorf("Poll: not implemented")
}

// Complete records that a worker finished a task (successfully or with error).
func (s *brokerServer) Complete(_ context.Context, req *pb.CompleteRequest) (*pb.CompleteResponse, error) {
	// TODO (Stage 1): move task from s.inflight to s.done / s.errors.
	// If task_id is unknown or already done, treat as a no-op (return ok=true).
	return nil, fmt.Errorf("Complete: not implemented")
}

// GetResult reports whether a task is done and, if so, any error.
func (s *brokerServer) GetResult(_ context.Context, req *pb.GetResultRequest) (*pb.GetResultResponse, error) {
	// TODO (Stage 1): check s.done[task_id]; return done=true if present.
	return nil, fmt.Errorf("GetResult: not implemented")
}

// ──────────────────────────────────────────────────────────────────────────────
// RPC handlers — Stage 2
// ──────────────────────────────────────────────────────────────────────────────

// Heartbeat updates the last-seen timestamp for an in-flight task so the broker
// knows the worker is still alive.
func (s *brokerServer) Heartbeat(_ context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	// TODO (Stage 2): find inflight[req.TaskId] and update lastHeartbeat.
	// Stage 1 stub: just return ok.
	return &pb.HeartbeatResponse{Ok: true}, nil
}

// reEnqueueStalled is the background goroutine that scans inflight tasks and
// moves any that have not sent a heartbeat within TaskTimeout back to pending.
func (s *brokerServer) reEnqueueStalled() {
	// TODO (Stage 2): ticker loop; on each tick, lock and scan s.inflight.
	// For entries where time.Since(e.lastHeartbeat) > TaskTimeout, delete from
	// inflight and prepend to pending (so they are picked up promptly).
}

// ──────────────────────────────────────────────────────────────────────────────
// gRPC server wiring
// ──────────────────────────────────────────────────────────────────────────────

func main() {
	port := flag.Int("port", 9000, "port to listen on")
	flag.Parse()

	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("listen: %v", err)
	}

	srv := grpc.NewServer()
	bs := newBrokerServer()

	// Stage 2: uncomment to start the re-enqueue background goroutine.
	// go bs.reEnqueueStalled()

	pb.RegisterBrokerServer(srv, bs)
	log.Printf("broker listening on :%d", *port)
	if err := srv.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
