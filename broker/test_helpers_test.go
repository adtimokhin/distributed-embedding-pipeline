// Test infrastructure for the Raft-replicated broker cluster.
//
// Ported from hw3/server/test_helpers_test.go — same bufconn-based
// in-memory-transport pattern, adapted from the KVStore service to the
// Broker service. Uses bufconn for both the Broker client-facing service
// and the RaftRPC peer-to-peer service, so tests run without real TCP ports.
//
// TestServer wraps a brokerServer with two bufconn listeners (client and
// peer) and a pre-wired Broker gRPC client. TestCluster wires three
// TestServers together via proxyClient so that Raft peer RPCs flow over
// bufconn, and crashes/partitions can be injected.
package main

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"pipeline/config"
	pb "pipeline/proto"
	"pipeline/raft"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

const bufSize = 1 << 20 // 1 MB

// ── proxyClient ───────────────────────────────────────────────────────────────
//
// Implements pb.RaftRPCClient; drops all RPCs when connected=false.

type proxyClient struct {
	mu        sync.Mutex
	inner     pb.RaftRPCClient
	connected bool
}

func (p *proxyClient) drop() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return !p.connected
}

func (p *proxyClient) RequestVote(ctx context.Context, in *pb.RequestVoteArgs, opts ...grpc.CallOption) (*pb.RequestVoteReply, error) {
	if p.drop() {
		return nil, status.Error(codes.Unavailable, "partitioned")
	}
	return p.inner.RequestVote(ctx, in, opts...)
}

func (p *proxyClient) AppendEntries(ctx context.Context, in *pb.AppendEntriesArgs, opts ...grpc.CallOption) (*pb.AppendEntriesReply, error) {
	if p.drop() {
		return nil, status.Error(codes.Unavailable, "partitioned")
	}
	return p.inner.AppendEntries(ctx, in, opts...)
}

func (p *proxyClient) InstallSnapshot(ctx context.Context, in *pb.InstallSnapshotArgs, opts ...grpc.CallOption) (*pb.InstallSnapshotReply, error) {
	if p.drop() {
		return nil, status.Error(codes.Unavailable, "partitioned")
	}
	return p.inner.InstallSnapshot(ctx, in, opts...)
}

// ── TestServer ────────────────────────────────────────────────────────────────

type TestServer struct {
	srv          *brokerServer
	clientLis    *bufconn.Listener
	peerLis      *bufconn.Listener
	clientGRPC   *grpc.Server
	peerGRPC     *grpc.Server
	BrokerClient pb.BrokerClient
	clientConn   *grpc.ClientConn
	proxies      [3]*proxyClient // proxies[j] controls this server's Raft link to node j
}

// ── TestCluster ───────────────────────────────────────────────────────────────

type TestCluster struct {
	Servers [3]*TestServer
}

func testCfg() *config.ClusterConfig {
	return &config.ClusterConfig{
		Nodes: []config.NodeConfig{
			{ID: 0, ClientAddr: "test:9000", PeerAddr: "test:9100"},
			{ID: 1, ClientAddr: "test:9001", PeerAddr: "test:9101"},
			{ID: 2, ClientAddr: "test:9002", PeerAddr: "test:9102"},
		},
	}
}

// newTestCluster creates a 3-node cluster wired over bufconn.
//
// Raft instances are created via raft.NewPaused so that peerConns can be
// fully wired before any election timers fire. All three nodes are resumed
// together.
func newTestCluster(t *testing.T) *TestCluster {
	t.Helper()
	cfg := testCfg()
	tc := &TestCluster{}

	// Phase 1 — create all servers (Raft paused) and their gRPC listeners.
	for i := range 3 {
		tc.Servers[i] = newTestServer(t, int32(i), cfg)
	}

	// Phase 2 — wire Raft peer connections via proxyClient.
	for i := range 3 {
		for j := range 3 {
			if i == j {
				continue
			}
			j := j
			conn, err := grpc.NewClient(
				"passthrough://bufnet",
				grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
					ts := tc.Servers[j]
					if ts == nil {
						return nil, fmt.Errorf("node %d has been killed", j)
					}
					return ts.peerLis.DialContext(ctx)
				}),
				grpc.WithTransportCredentials(insecure.NewCredentials()),
			)
			if err != nil {
				t.Fatalf("dial raft bufconn %d→%d: %v", i, j, err)
			}
			proxy := &proxyClient{inner: pb.NewRaftRPCClient(conn), connected: true}
			tc.Servers[i].proxies[j] = proxy
			tc.Servers[i].srv.rf.SetPeerClient(int32(j), proxy)
		}
	}

	// Phase 3 — resume all nodes simultaneously.
	for _, ts := range tc.Servers {
		ts.srv.rf.Resume()
		go ts.srv.reEnqueueStalled()
	}

	t.Cleanup(func() {
		for _, ts := range tc.Servers {
			if ts != nil {
				ts.srv.rf.Kill()
				ts.clientGRPC.Stop()
				ts.peerGRPC.Stop()
				ts.clientConn.Close()
				ts.clientLis.Close()
				ts.peerLis.Close()
			}
		}
	})

	return tc
}

// newTestServer creates a single broker node with paused Raft and two
// bufconn gRPC servers. The caller is responsible for wiring peerConns and
// calling Resume.
func newTestServer(t *testing.T, id int32, cfg *config.ClusterConfig) *TestServer {
	t.Helper()

	commitCh := make(chan raft.ApplyMsg, 100)
	rf := raft.NewPaused(id, cfg, commitCh)

	// Construct brokerServer directly (package-internal access) so we control Raft.
	s := &brokerServer{
		id:         id,
		cfg:        cfg,
		rf:         rf,
		submitted:  make(map[string]string),
		done:       make(map[string]bool),
		errors:     make(map[string]string),
		inflight:   make(map[string]*inFlightEntry),
		pendingOps: make(map[int64]*pendingOp),
		leaderID:   -1,
	}
	go s.applyLoop(commitCh)

	clientLis := bufconn.Listen(bufSize)
	peerLis := bufconn.Listen(bufSize)

	clientGRPC := grpc.NewServer()
	pb.RegisterBrokerServer(clientGRPC, s)

	peerGRPC := grpc.NewServer()
	pb.RegisterRaftRPCServer(peerGRPC, s)

	go clientGRPC.Serve(clientLis) //nolint:errcheck
	go peerGRPC.Serve(peerLis)     //nolint:errcheck

	clientConn := dialBufconn(t, clientLis)

	return &TestServer{
		srv:          s,
		clientLis:    clientLis,
		peerLis:      peerLis,
		clientGRPC:   clientGRPC,
		peerGRPC:     peerGRPC,
		BrokerClient: pb.NewBrokerClient(clientConn),
		clientConn:   clientConn,
	}
}

func dialBufconn(t *testing.T, lis *bufconn.Listener) *grpc.ClientConn {
	t.Helper()
	conn, err := grpc.NewClient(
		"passthrough://bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dialBufconn: %v", err)
	}
	return conn
}

// ── Cluster helpers ───────────────────────────────────────────────────────────

// KillServer simulates a hard crash: stops the Raft instance and both gRPC servers.
func (tc *TestCluster) KillServer(id int) {
	ts := tc.Servers[id]
	ts.srv.rf.Kill()
	ts.clientGRPC.Stop()
	ts.peerGRPC.Stop()
	ts.clientLis.Close()
	ts.peerLis.Close()
	tc.Servers[id] = nil
}

// partition makes server from unable to send Raft RPCs to server to (one-way).
func (tc *TestCluster) partition(from, to int) {
	tc.Servers[from].proxies[to].mu.Lock()
	tc.Servers[from].proxies[to].connected = false
	tc.Servers[from].proxies[to].mu.Unlock()
}

// heal restores the one-way Raft RPC link from → to.
func (tc *TestCluster) heal(from, to int) {
	tc.Servers[from].proxies[to].mu.Lock()
	tc.Servers[from].proxies[to].connected = true
	tc.Servers[from].proxies[to].mu.Unlock()
}

// isolate disconnects server id from all others (bidirectionally).
func (tc *TestCluster) isolate(id int) {
	for j := range 3 {
		if j == id {
			continue
		}
		tc.partition(id, j)
		tc.partition(j, id)
	}
}

// reconnect restores all connections to and from server id.
func (tc *TestCluster) reconnect(id int) {
	for j := range 3 {
		if j == id || tc.Servers[j] == nil {
			continue
		}
		tc.heal(id, j)
		tc.heal(j, id)
	}
}

// leaderServer returns the TestServer that is currently the Raft leader, or nil.
func (tc *TestCluster) leaderServer() *TestServer {
	for _, ts := range tc.Servers {
		if ts == nil {
			continue
		}
		if _, isLeader := ts.srv.rf.GetState(); isLeader {
			return ts
		}
	}
	return nil
}

// leaderID returns the index of the current leader, or -1.
func (tc *TestCluster) leaderID() int {
	for i, ts := range tc.Servers {
		if ts == nil {
			continue
		}
		if _, isLeader := ts.srv.rf.GetState(); isLeader {
			return i
		}
	}
	return -1
}

// waitForLeader polls until a leader is found, failing the test on timeout.
func (tc *TestCluster) waitForLeader(t *testing.T) *TestServer {
	t.Helper()
	var leader *TestServer
	ok := pollUntil(func() bool {
		leader = tc.leaderServer()
		return leader != nil
	}, 2*time.Second, 10*time.Millisecond)
	if !ok {
		t.Fatal("no leader elected within 2s")
	}
	return leader
}

// ── Timing helpers ────────────────────────────────────────────────────────────

// testCtx returns a context that times out after 10 seconds.
func testCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

// pollUntil calls cond every interval until it returns true or timeout elapses.
func pollUntil(cond func() bool, timeout, interval time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(interval)
	}
	return false
}
