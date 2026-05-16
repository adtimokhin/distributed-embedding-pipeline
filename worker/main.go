// Worker — HW4 embedding pipeline worker.
//
// Stage 1: poll broker, drive embedding subprocess, upsert to Qdrant, complete task.
// Stage 2: send periodic heartbeats to the broker while a task is in progress.

package main

import (
	"bufio"
	"context"
	"encoding/json"
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
	parts := strings.Fields(cmdStr)
	if len(parts) == 0 {
		return nil, fmt.Errorf("empty embedder command")
	}
	cmd := exec.Command(parts[0], parts[1:]...)

	stdinPipe, err := cmd.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("stdin pipe: %w", err)
	}
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("stdout pipe: %w", err)
	}
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start subprocess: %w", err)
	}
	return &embedder{
		cmd:    cmd,
		stdin:  bufio.NewWriter(stdinPipe),
		stdout: bufio.NewScanner(stdoutPipe),
	}, nil
}

// embed sends one request line and reads one response line.
func (e *embedder) embed(req embedRequest) (embedResponse, error) {
	line, err := json.Marshal(req)
	if err != nil {
		return embedResponse{}, fmt.Errorf("marshal request: %w", err)
	}
	if _, err := fmt.Fprintf(e.stdin, "%s\n", line); err != nil {
		return embedResponse{}, fmt.Errorf("write to subprocess: %w", err)
	}
	if err := e.stdin.Flush(); err != nil {
		return embedResponse{}, fmt.Errorf("flush subprocess stdin: %w", err)
	}
	if !e.stdout.Scan() {
		if err := e.stdout.Err(); err != nil {
			return embedResponse{}, fmt.Errorf("read from subprocess: %w", err)
		}
		return embedResponse{}, fmt.Errorf("subprocess closed stdout unexpectedly")
	}
	var resp embedResponse
	if err := json.Unmarshal(e.stdout.Bytes(), &resp); err != nil {
		return embedResponse{}, fmt.Errorf("decode subprocess response: %w", err)
	}
	return resp, nil
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
	_, err := client.Upsert(ctx, &qdrant.UpsertPoints{
		CollectionName: QdrantCollection,
		Points: []*qdrant.PointStruct{
			{
				Id:      chunkIDToUUID(p.ChunkID),
				Vectors: qdrant.NewVectors(vector...),
				Payload: qdrant.NewValueMap(map[string]any{
					"doc_id": p.DocID,
					"title":  p.Title,
					"text":   p.Text,
				}),
			},
		},
	})
	return err
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
	brokerClient := pb.NewBrokerClient(brokerConn)

	// ── Connect to Qdrant ────────────────────────────────────────────────────
	qdrantConn, err := grpc.NewClient(qdrantAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial qdrant: %w", err)
	}
	defer qdrantConn.Close()

	// Ensure the collection exists before processing any tasks.
	initCtx := context.Background()
	collClient := qdrant.NewCollectionsClient(qdrantConn)
	if _, err := collClient.Get(initCtx, &qdrant.GetCollectionInfoRequest{CollectionName: QdrantCollection}); err != nil {
		if _, err := collClient.Create(initCtx, &qdrant.CreateCollection{
			CollectionName: QdrantCollection,
			VectorsConfig: qdrant.NewVectorsConfig(&qdrant.VectorParams{
				Size:     VectorDim,
				Distance: qdrant.Distance_Cosine,
			}),
		}); err != nil {
			return fmt.Errorf("create qdrant collection: %w", err)
		}
	}
	qdrantClient := qdrant.NewPointsClient(qdrantConn)

	// ── Spawn embedding subprocess ───────────────────────────────────────────
	emb, err := startEmbedder(embedderCmd)
	if err != nil {
		return fmt.Errorf("start embedder: %w", err)
	}

	// ── Worker ID ────────────────────────────────────────────────────────────
	workerID := fmt.Sprintf("worker-%d", os.Getpid())
	log.Printf("worker %s starting (broker=%s qdrant=%s)", workerID, brokerAddr, qdrantAddr)

	// ── Poll loop ────────────────────────────────────────────────────────────
	for {
		ctx := context.Background()

		pollResp, err := brokerClient.Poll(ctx, &pb.PollRequest{WorkerId: workerID})
		if err != nil {
			log.Printf("poll error: %v", err)
			time.Sleep(PollInterval)
			continue
		}
		if !pollResp.HasTask {
			time.Sleep(PollInterval)
			continue
		}
		task := pollResp.Task

		// TODO (Stage 2): start a background heartbeat goroutine that calls
		// brokerClient.Heartbeat(workerID, task.Id) every HeartbeatInterval.
		// Cancel it (via context or channel) after Complete/error.

		var p taskPayload
		if err := json.Unmarshal([]byte(task.Payload), &p); err != nil {
			log.Printf("decode task %s: %v", task.Id, err)
			brokerClient.Complete(ctx, &pb.CompleteRequest{TaskId: task.Id, WorkerId: workerID, Error: err.Error()}) //nolint:errcheck
			return fmt.Errorf("decode task payload: %w", err)
		}

		embedResp, err := emb.embed(embedRequest{ChunkID: p.ChunkID, Text: p.Text})
		if err != nil {
			log.Printf("embed task %s: %v", task.Id, err)
			brokerClient.Complete(ctx, &pb.CompleteRequest{TaskId: task.Id, WorkerId: workerID, Error: err.Error()}) //nolint:errcheck
			return fmt.Errorf("embed task: %w", err)
		}

		if err := upsertVector(ctx, qdrantClient, p, embedResp.Vector); err != nil {
			log.Printf("upsert task %s: %v", task.Id, err)
			brokerClient.Complete(ctx, &pb.CompleteRequest{TaskId: task.Id, WorkerId: workerID, Error: err.Error()}) //nolint:errcheck
			return fmt.Errorf("upsert task: %w", err)
		}

		if _, err := brokerClient.Complete(ctx, &pb.CompleteRequest{TaskId: task.Id, WorkerId: workerID, Error: ""}); err != nil {
			log.Printf("complete task %s: %v", task.Id, err)
		}
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
