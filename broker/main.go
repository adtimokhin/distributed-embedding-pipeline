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
	task          *pb.Task
	workerID      string
	lastHeartbeat time.Time // Stage 2: updated by Heartbeat RPCs
}

type brokerServer struct {
	pb.UnimplementedBrokerServer

	mu       sync.Mutex
	pending  []*pb.Task                // tasks waiting to be assigned
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
	id := uuid.New().String()
	task := &pb.Task{Id: id, Payload: req.Payload}

	s.mu.Lock()
	s.pending = append(s.pending, task)
	s.mu.Unlock()

	return &pb.SubmitResponse{TaskId: id}, nil
}

// Poll returns one pending task to the calling worker, or has_task=false if
// the queue is empty.
func (s *brokerServer) Poll(_ context.Context, req *pb.PollRequest) (*pb.PollResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.pending) == 0 {
		return &pb.PollResponse{HasTask: false}, nil
	}

	task := s.pending[0]
	s.pending = s.pending[1:]
	s.inflight[task.Id] = &inFlightEntry{
		task:          task,
		workerID:      req.WorkerId,
		lastHeartbeat: time.Now(),
	}

	return &pb.PollResponse{Task: task, HasTask: true}, nil
}

// Complete records that a worker finished a task (successfully or with error).
func (s *brokerServer) Complete(_ context.Context, req *pb.CompleteRequest) (*pb.CompleteResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, inflight := s.inflight[req.TaskId]; !inflight {
		// Unknown or already done — no-op.
		return &pb.CompleteResponse{Ok: true}, nil
	}

	delete(s.inflight, req.TaskId)
	s.done[req.TaskId] = true
	s.errors[req.TaskId] = req.Error

	return &pb.CompleteResponse{Ok: true}, nil
}

// GetResult reports whether a task is done and, if so, any error.
func (s *brokerServer) GetResult(_ context.Context, req *pb.GetResultRequest) (*pb.GetResultResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.done[req.TaskId] {
		return &pb.GetResultResponse{Done: false}, nil
	}

	return &pb.GetResultResponse{Done: true, Error: s.errors[req.TaskId]}, nil
}

// ──────────────────────────────────────────────────────────────────────────────
// RPC handlers — Stage 2
// ──────────────────────────────────────────────────────────────────────────────

// Heartbeat updates the last-seen timestamp for an in-flight task so the broker
// knows the worker is still alive.
func (s *brokerServer) Heartbeat(_ context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if e, ok := s.inflight[req.TaskId]; ok {
		e.lastHeartbeat = time.Now()
	}
	return &pb.HeartbeatResponse{Ok: true}, nil
}

// reEnqueueStalled is the background goroutine that scans inflight tasks and
// moves any that have not sent a heartbeat within TaskTimeout back to pending.
func (s *brokerServer) reEnqueueStalled() {
	ticker := time.NewTicker(CheckInterval)
	defer ticker.Stop()
	for range ticker.C {
		s.mu.Lock()
		for id, e := range s.inflight {
			if time.Since(e.lastHeartbeat) > TaskTimeout {
				log.Printf("re-enqueuing stalled task %s (worker %s, silent for %s)",
					id, e.workerID, time.Since(e.lastHeartbeat).Round(time.Second))
				s.pending = append([]*pb.Task{e.task}, s.pending...)
				delete(s.inflight, id)
			}
		}
		s.mu.Unlock()
	}
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

	go bs.reEnqueueStalled()

	pb.RegisterBrokerServer(srv, bs)
	log.Printf("broker listening on :%d", *port)
	if err := srv.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
