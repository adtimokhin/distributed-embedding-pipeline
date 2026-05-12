// Worker — HW4 embedding pipeline worker.
//
// Stage 1: poll broker, drive embedding subprocess, upsert to Qdrant, complete task.
// Stage 2: send periodic heartbeats to the broker while a task is in progress.

package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log"
	"math/big"
	"os"
	"os/exec"
	"strings"
	"time"

	pb "pipeline/proto"

	qdrant "github.com/qdrant/go-client/qdrant"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// ──────────────────────────────────────────────────────────────────────────────
// Tunable constants
// ──────────────────────────────────────────────────────────────────────────────

const (
	PollInterval      = 100 * time.Millisecond
	HeartbeatInterval = 3 * time.Second // Stage 2
	VectorDim         = 384
	QdrantCollection  = "documents"
)

// ──────────────────────────────────────────────────────────────────────────────
// Subprocess protocol types
// ──────────────────────────────────────────────────────────────────────────────

type embedRequest struct {
	ChunkID string `json:"chunk_id"`
	Text    string `json:"text"`
}

type embedResponse struct {
	ChunkID string    `json:"chunk_id"`
	Vector  []float32 `json:"vector"`
}

// taskPayload mirrors what the producer encodes in Task.Payload.
type taskPayload struct {
	DocID   string `json:"doc_id"`
	ChunkID string `json:"chunk_id"`
	Title   string `json:"title"`
	Text    string `json:"text"`
}

// ──────────────────────────────────────────────────────────────────────────────
// Subprocess management
// ──────────────────────────────────────────────────────────────────────────────

type embedder struct {
	cmd    *exec.Cmd
	stdin  *bufio.Writer
	stdout *bufio.Scanner
}

// startEmbedder spawns the embedding subprocess described by cmdStr.
// cmdStr may be a space-separated command + arguments (e.g. "python3 embedder.py").
func startEmbedder(cmdStr string) (*embedder, error) {
	// TODO (Stage 1): split cmdStr on spaces, exec.Command, wire up stdin/stdout pipes,
	// start the process, and return an *embedder.

	_ = strings.Fields(cmdStr) // remove this line when you implement
	return nil, fmt.Errorf("startEmbedder: not implemented")
}

// embed sends one request line and reads one response line.
func (e *embedder) embed(req embedRequest) (embedResponse, error) {
	// TODO (Stage 1): marshal req to JSON, write to e.stdin (flush!), read one
	// line from e.stdout, unmarshal into embedResponse and return.
	return embedResponse{}, fmt.Errorf("embed: not implemented")
}

// ──────────────────────────────────────────────────────────────────────────────
// Qdrant helpers
// ──────────────────────────────────────────────────────────────────────────────

// chunkIDToUUID deterministically converts a string chunk ID to a Qdrant point
// ID by treating the FNV-64 hash as the low 64 bits of a UUID.
func chunkIDToUUID(chunkID string) *qdrant.PointId {
	// Simple deterministic mapping: hash the chunk ID.
	h := fnv64(chunkID)
	return &qdrant.PointId{
		PointIdOptions: &qdrant.PointId_Num{Num: h},
	}
}

func fnv64(s string) uint64 {
	const prime = 1099511628211
	h := big.NewInt(0)
	offset := new(big.Int).SetUint64(14695981039346656037)
	p := new(big.Int).SetUint64(prime)
	mod := new(big.Int).Lsh(big.NewInt(1), 64)
	h.Set(offset)
	for i := 0; i < len(s); i++ {
		h.Xor(h, big.NewInt(int64(s[i])))
		h.Mul(h, p)
		h.Mod(h, mod)
	}
	return h.Uint64()
}

// upsertVector writes one embedding point to the Qdrant collection.
func upsertVector(ctx context.Context, client qdrant.PointsClient, p taskPayload, vector []float32) error {
	// TODO (Stage 1): build a qdrant.UpsertPoints request and call client.Upsert.
	//
	// Point ID: chunkIDToUUID(p.ChunkID)
	// Vector:   qdrant.NewVectors(vector...)   — single unnamed vector
	// Payload:  qdrant.NewValueMap(map[string]any{"doc_id":..., "title":..., "text":...})
	//
	// Hint: use qdrant.UpsertPoints and qdrant.PointStruct.
	_ = ctx
	_ = client
	_ = p
	_ = vector
	return fmt.Errorf("upsertVector: not implemented")
}

// ──────────────────────────────────────────────────────────────────────────────
// Main worker loop
// ──────────────────────────────────────────────────────────────────────────────

func run(brokerAddr, qdrantAddr, embedderCmd string) error {
	// ── Connect to broker ────────────────────────────────────────────────────
	brokerConn, err := grpc.NewClient(brokerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial broker: %w", err)
	}
	defer brokerConn.Close()
	_ = pb.NewBrokerClient(brokerConn) // TODO: assign to brokerClient and use below

	// ── Connect to Qdrant ────────────────────────────────────────────────────
	// TODO (Stage 1): open a gRPC connection to qdrantAddr and create a
	// qdrant.PointsClient. Qdrant's gRPC port is 6334 (HTTP/REST is 6333).
	// The worker flag --qdrant accepts host:grpc_port (e.g. localhost:6334).
	var qdrantClient qdrant.PointsClient
	_ = qdrantClient // remove when implemented

	// ── Spawn embedding subprocess ───────────────────────────────────────────
	emb, err := startEmbedder(embedderCmd)
	if err != nil {
		return fmt.Errorf("start embedder: %w", err)
	}
	_ = emb

	// ── Worker ID ────────────────────────────────────────────────────────────
	workerID := fmt.Sprintf("worker-%d", os.Getpid())
	log.Printf("worker %s starting (broker=%s qdrant=%s)", workerID, brokerAddr, qdrantAddr)

	// ── Poll loop ────────────────────────────────────────────────────────────
	for {
		ctx := context.Background()

		// TODO (Stage 1): call brokerClient.Poll; if !has_task, sleep and continue.
		_ = ctx

		// TODO (Stage 1): decode task.Payload into taskPayload.

		// TODO (Stage 2): start a background heartbeat goroutine that calls
		// brokerClient.Heartbeat(workerID, task.Id) every HeartbeatInterval.
		// Cancel it (via context or channel) after Complete/error.

		// TODO (Stage 1): call emb.embed with the chunk text.

		// TODO (Stage 1): call upsertVector.

		// TODO (Stage 1): call brokerClient.Complete(task.Id, workerID, "").
		// On any error above, call brokerClient.Complete with a non-empty error
		// string, then return (let the process exit so the broker re-enqueues).

		time.Sleep(PollInterval)
	}
}

func main() {
	brokerAddr := flag.String("broker", "localhost:9000", "broker gRPC address")
	qdrantAddr := flag.String("qdrant", "localhost:6334", "qdrant gRPC address (port 6334)")
	embedderCmd := flag.String("embedder", "", "embedding subprocess command (e.g. 'python3 tools/embedder/embedder.py')")
	flag.Parse()

	if *embedderCmd == "" {
		log.Fatal("--embedder is required")
	}

	if err := run(*brokerAddr, *qdrantAddr, *embedderCmd); err != nil {
		log.Fatal(err)
	}
}
