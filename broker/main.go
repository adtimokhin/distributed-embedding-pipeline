// Broker — HW4 embedding pipeline task broker.
//
// The broker is a Raft-replicated 3-node cluster (reusing HW3's raft
// package) instead of a single in-memory process. Two kinds of state are
// tracked, deliberately split the same way HW3 splits Put (replicated) from
// Get (local, stale-OK):
//
//   - Replicated via Raft (submitted tasks, done/error results): these are
//     the facts that must survive a crash — losing "this task exists" drops
//     work forever, losing "this task is done" makes the producer poll
//     GetResult on an orphaned ID forever.
//   - Leader-local only (task→worker assignment, heartbeat liveness): purely
//     ephemeral scheduling state. If the leader crashes, in-flight
//     assignments are simply forgotten and the new leader treats every
//     submitted-but-not-done task as available again — safe because
//     re-embedding is idempotent, the same property that already makes
//     Stage 2's at-least-once re-enqueue safe. This avoids paying a
//     consensus round trip on every 100ms worker poll.
//
// "Pending" is therefore a derived view, not stored state: scan
// submittedOrder, skip anything in done or inflight.
package main

import (
	"context"
	"encoding/base64"
	"flag"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	"pipeline/config"
	pb "pipeline/proto"
	"pipeline/raft"

	"github.com/google/uuid"
	"google.golang.org/grpc"
)

// ──────────────────────────────────────────────────────────────────────────────
// Tunable constants
// ──────────────────────────────────────────────────────────────────────────────

const (
	TaskTimeout   = 10 * time.Second // re-enqueue after this much heartbeat silence
	CheckInterval = 5 * time.Second  // how often to scan inflight tasks
	commitTimeout = 5 * time.Second  // how long Submit/Complete wait for their Raft entry to commit
)

// ──────────────────────────────────────────────────────────────────────────────
// Types
// ──────────────────────────────────────────────────────────────────────────────

type inFlightEntry struct {
	task          *pb.Task
	workerID      string
	lastHeartbeat time.Time
}

// pendingOp tracks an in-flight Submit/Complete RPC waiting for its Raft log
// entry to commit — same shape as HW3's pendingPut in kvraft's server.
type pendingOp struct {
	term int64
	ch   chan struct{} // closed when the entry commits (or leadership is lost)
}

type brokerServer struct {
	pb.UnimplementedBrokerServer
	pb.UnimplementedRaftRPCServer

	id  int32
	cfg *config.ClusterConfig
	rf  *raft.Raft

	mu sync.Mutex

	// ── Replicated state — mutated only by applyLoop ──────────────────────
	submitted      map[string]string // task_id -> payload
	submittedOrder []string          // task_ids in commit order (identical on every replica: applyLoop processes commitCh in order)
	done           map[string]bool
	errors         map[string]string

	// ── Leader-local scheduling state — never replicated ──────────────────
	inflight  map[string]*inFlightEntry
	wasLeader bool // tracks the last-observed GetState() result, to detect false->true transitions

	// ── RPC bookkeeping ─────────────────────────────────────────────────────
	pendingOps map[int64]*pendingOp // log index -> caller blocked on commit
	leaderID   int32                // last known leader, -1 if unknown; updated from AppendEntries
}

func newBrokerServer(id int32, cfg *config.ClusterConfig) *brokerServer {
	s := &brokerServer{
		id:         id,
		cfg:        cfg,
		submitted:  make(map[string]string),
		done:       make(map[string]bool),
		errors:     make(map[string]string),
		inflight:   make(map[string]*inFlightEntry),
		pendingOps: make(map[int64]*pendingOp),
		leaderID:   -1,
	}
	commitCh := make(chan raft.ApplyMsg, 100)
	s.rf = raft.New(id, cfg, commitCh)
	go s.applyLoop(commitCh)
	return s
}

// ──────────────────────────────────────────────────────────────────────────────
// Command encoding — commands replicated through the Raft log
// ──────────────────────────────────────────────────────────────────────────────

// encodeSubmit/encodeComplete produce "op:id:base64(data)". Base64 avoids
// delimiter collisions with JSON payloads or free-text error messages that
// may themselves contain colons.
func encodeSubmit(id, payload string) string {
	return "submit:" + id + ":" + base64.StdEncoding.EncodeToString([]byte(payload))
}

func encodeComplete(id, errMsg string) string {
	return "complete:" + id + ":" + base64.StdEncoding.EncodeToString([]byte(errMsg))
}

// decodeCommand parses "op:id:base64(data)". Unrecognized shapes (including
// Raft's own "noop" leader-election marker) decode to op="", which the
// applyLoop switch below silently ignores.
func decodeCommand(cmd string) (op, id, data string) {
	parts := strings.SplitN(cmd, ":", 3)
	if len(parts) != 3 {
		return "", "", ""
	}
	raw, err := base64.StdEncoding.DecodeString(parts[2])
	if err != nil {
		return "", "", ""
	}
	return parts[0], parts[1], string(raw)
}

// ──────────────────────────────────────────────────────────────────────────────
// Apply loop — the only goroutine that mutates replicated state
// ──────────────────────────────────────────────────────────────────────────────

func (s *brokerServer) applyLoop(commitCh <-chan raft.ApplyMsg) {
	for msg := range commitCh {
		op, id, data := decodeCommand(msg.Command)

		s.mu.Lock()
		switch op {
		case "submit":
			if _, exists := s.submitted[id]; !exists {
				s.submitted[id] = data
				s.submittedOrder = append(s.submittedOrder, id)
			}
		case "complete":
			s.done[id] = true
			s.errors[id] = data
			delete(s.inflight, id) // no-op unless this replica is currently leader
		}

		if p, ok := s.pendingOps[msg.Index]; ok {
			p.term = msg.Term // lets the waiting RPC handler detect a leadership change
			delete(s.pendingOps, msg.Index)
			close(p.ch)
		}
		s.mu.Unlock()
	}
}

// proposeAndWait submits cmd to Raft and blocks until it commits. Returns
// true only if the entry that committed at the returned index is the one
// this call submitted (i.e. leadership did not change out from under it).
func (s *brokerServer) proposeAndWait(cmd string) bool {
	index, term, isLeader := s.rf.Start(cmd)
	if !isLeader {
		return false
	}

	op := &pendingOp{term: term, ch: make(chan struct{})}
	s.mu.Lock()
	s.pendingOps[index] = op
	s.mu.Unlock()

	select {
	case <-op.ch:
		return op.term == term
	case <-time.After(commitTimeout):
		s.mu.Lock()
		delete(s.pendingOps, index)
		s.mu.Unlock()
		return false
	}
}

// ──────────────────────────────────────────────────────────────────────────────
// RPC handlers — Broker service (client-facing)
// ──────────────────────────────────────────────────────────────────────────────

// Submit proposes a new task through Raft and blocks until it is durably
// committed before returning its ID.
func (s *brokerServer) Submit(_ context.Context, req *pb.SubmitRequest) (*pb.SubmitResponse, error) {
	id := uuid.New().String()
	if !s.proposeAndWait(encodeSubmit(id, req.Payload)) {
		return &pb.SubmitResponse{Ok: false, RedirectAddr: s.currentLeaderAddr()}, nil
	}
	return &pb.SubmitResponse{Ok: true, TaskId: id}, nil
}

// Poll hands out one pending task, or redirects if this node is not the
// leader. Leader-only, in-memory — see the package doc for why this is safe
// to leave unreplicated.
func (s *brokerServer) Poll(_ context.Context, req *pb.PollRequest) (*pb.PollResponse, error) {
	if !s.refreshLeaderState() {
		return &pb.PollResponse{HasTask: false, RedirectAddr: s.currentLeaderAddr()}, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	for _, id := range s.submittedOrder {
		if s.done[id] {
			continue
		}
		if _, busy := s.inflight[id]; busy {
			continue
		}
		task := &pb.Task{Id: id, Payload: s.submitted[id]}
		s.inflight[id] = &inFlightEntry{task: task, workerID: req.WorkerId, lastHeartbeat: time.Now()}
		return &pb.PollResponse{Task: task, HasTask: true}, nil
	}
	return &pb.PollResponse{HasTask: false}, nil
}

// Complete proposes a task's completion through Raft and blocks until it is
// durably committed.
func (s *brokerServer) Complete(_ context.Context, req *pb.CompleteRequest) (*pb.CompleteResponse, error) {
	if !s.proposeAndWait(encodeComplete(req.TaskId, req.Error)) {
		return &pb.CompleteResponse{Ok: false, RedirectAddr: s.currentLeaderAddr()}, nil
	}
	return &pb.CompleteResponse{Ok: true}, nil
}

// GetResult reads local replicated state and never redirects — any node can
// answer, possibly with a result that lags the leader by one replication
// round trip. Same stale-read tradeoff HW3 accepts for Get.
func (s *brokerServer) GetResult(_ context.Context, req *pb.GetResultRequest) (*pb.GetResultResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.done[req.TaskId] {
		return &pb.GetResultResponse{Done: false}, nil
	}
	return &pb.GetResultResponse{Done: true, Error: s.errors[req.TaskId]}, nil
}

// Heartbeat updates the last-seen timestamp for an in-flight task. Leader-only,
// in-memory, same reasoning as Poll.
func (s *brokerServer) Heartbeat(_ context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	if !s.refreshLeaderState() {
		return &pb.HeartbeatResponse{Ok: false, RedirectAddr: s.currentLeaderAddr()}, nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if e, ok := s.inflight[req.TaskId]; ok {
		e.lastHeartbeat = time.Now()
	}
	return &pb.HeartbeatResponse{Ok: true}, nil
}

// reEnqueueStalled is the background goroutine that periodically scans
// inflight tasks for ones that have gone stale. Split out from
// checkStalledTasks so tests can trigger a single scan synchronously
// instead of waiting CheckInterval/TaskTimeout of real wall-clock time.
func (s *brokerServer) reEnqueueStalled() {
	ticker := time.NewTicker(CheckInterval)
	defer ticker.Stop()
	for range ticker.C {
		s.checkStalledTasks()
	}
}

// checkStalledTasks drops any inflight task that has not sent a heartbeat
// within TaskTimeout. Dropping the entry IS the re-enqueue: Poll's scan
// treats anything not in inflight (and not done) as available again. No-op
// unless this node is currently the leader — inflight is only meaningful there.
func (s *brokerServer) checkStalledTasks() {
	if !s.refreshLeaderState() {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, e := range s.inflight {
		if time.Since(e.lastHeartbeat) > TaskTimeout {
			log.Printf("node %d: re-enqueuing stalled task %s (worker %s, silent for %s)",
				s.id, id, e.workerID, time.Since(e.lastHeartbeat).Round(time.Second))
			delete(s.inflight, id)
		}
	}
}

// refreshLeaderState checks current Raft leadership and, on a false->true
// transition, wipes leader-local scheduling state. This matters for the case
// where a node was leader with some tasks inflight, lost leadership before
// those tasks completed, and later becomes leader again — without the wipe,
// those stale inflight entries would permanently block their tasks from ever
// being re-offered, since no one is left to time them out from a map no
// scan/heartbeat path is touching anymore. Must NOT be called with mu held.
func (s *brokerServer) refreshLeaderState() bool {
	_, isLeader := s.rf.GetState()
	s.mu.Lock()
	if isLeader && !s.wasLeader {
		s.inflight = make(map[string]*inFlightEntry)
		log.Printf("node %d: became leader, reset in-flight scheduling state", s.id)
	}
	s.wasLeader = isLeader
	s.mu.Unlock()
	return isLeader
}

// currentLeaderAddr returns the client-facing address of the current leader,
// or "" if unknown.
func (s *brokerServer) currentLeaderAddr() string {
	if _, isLeader := s.rf.GetState(); isLeader {
		return s.cfg.Nodes[s.id].ClientAddr
	}
	s.mu.Lock()
	id := s.leaderID
	s.mu.Unlock()
	if id < 0 || int(id) >= len(s.cfg.Nodes) {
		return ""
	}
	return s.cfg.Nodes[id].ClientAddr
}

// ──────────────────────────────────────────────────────────────────────────────
// RPC handlers — RaftRPC service (peer-to-peer), forwarded to raft.Raft
// ──────────────────────────────────────────────────────────────────────────────

func (s *brokerServer) RequestVote(ctx context.Context, req *pb.RequestVoteArgs) (*pb.RequestVoteReply, error) {
	return s.rf.RequestVote(ctx, req)
}

func (s *brokerServer) AppendEntries(ctx context.Context, req *pb.AppendEntriesArgs) (*pb.AppendEntriesReply, error) {
	reply, err := s.rf.AppendEntries(ctx, req)
	// Track the sender as leader only when it was actually accepted (not a
	// stale leader whose term the reply just rejected).
	if err == nil && reply != nil && reply.Term == req.Term {
		s.mu.Lock()
		s.leaderID = req.LeaderId
		s.mu.Unlock()
	}
	return reply, err
}

func (s *brokerServer) InstallSnapshot(ctx context.Context, req *pb.InstallSnapshotArgs) (*pb.InstallSnapshotReply, error) {
	return s.rf.InstallSnapshot(ctx, req)
}

// ──────────────────────────────────────────────────────────────────────────────
// gRPC server wiring
// ──────────────────────────────────────────────────────────────────────────────

func main() {
	idFlag := flag.Int("id", -1, "node ID (0, 1, or 2)")
	cfgPath := flag.String("config", "broker_nodeconfig.json", "path to broker_nodeconfig.json")
	flag.Parse()

	if *idFlag < 0 {
		log.Fatal("--id is required")
	}

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	self, err := cfg.Self(int32(*idFlag))
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	srv := newBrokerServer(self.ID, cfg)
	go srv.reEnqueueStalled()

	// Broker gRPC server (client-facing: producer/worker).
	// Bind on all interfaces so Docker port mapping works; the advertised
	// address (self.ClientAddr) is what redirects report to callers.
	_, clientPort, err := net.SplitHostPort(self.ClientAddr)
	if err != nil {
		log.Fatalf("parse client addr %s: %v", self.ClientAddr, err)
	}
	clientLis, err := net.Listen("tcp", ":"+clientPort)
	if err != nil {
		log.Fatalf("listen client %s: %v", self.ClientAddr, err)
	}
	clientGRPC := grpc.NewServer()
	pb.RegisterBrokerServer(clientGRPC, srv)

	// RaftRPC gRPC server (peer-to-peer).
	_, peerPort, err := net.SplitHostPort(self.PeerAddr)
	if err != nil {
		log.Fatalf("parse peer addr %s: %v", self.PeerAddr, err)
	}
	peerLis, err := net.Listen("tcp", ":"+peerPort)
	if err != nil {
		log.Fatalf("listen peer %s: %v", self.PeerAddr, err)
	}
	peerGRPC := grpc.NewServer()
	pb.RegisterRaftRPCServer(peerGRPC, srv)

	log.Printf("[broker %d] listening | client=%s peer=%s", self.ID, self.ClientAddr, self.PeerAddr)

	errCh := make(chan error, 2)
	go func() { errCh <- clientGRPC.Serve(clientLis) }()
	go func() { errCh <- peerGRPC.Serve(peerLis) }()

	log.Fatal(<-errCh)
}
