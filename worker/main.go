// Worker — HW4 embedding pipeline worker.
//
// Stage 1: poll broker, drive embedding subprocess, upsert to Qdrant, complete task.
// Stage 2: send periodic heartbeats to the broker while a task is in progress.

package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/big"
	"os"
	"strings"
	"time"

	"pipeline/internal/brokerclient"
	"pipeline/internal/embedder"

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

// taskPayload mirrors what the producer encodes in Task.Payload.
type taskPayload struct {
	DocID   string `json:"doc_id"`
	ChunkID string `json:"chunk_id"`
	Title   string `json:"title"`
	Text    string `json:"text"`
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

func run(brokerAddrs []string, qdrantAddr, embedderCmd string) error {
	// ── Broker client (Raft cluster: follows redirects, retries across nodes) ─
	brokerClient := brokerclient.New(brokerAddrs)

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
	emb, err := embedder.Start(embedderCmd)
	if err != nil {
		return fmt.Errorf("start embedder: %w", err)
	}
	defer emb.Close() //nolint:errcheck

	// ── Worker ID ────────────────────────────────────────────────────────────
	workerID := fmt.Sprintf("worker-%d", os.Getpid())
	log.Printf("worker %s starting (broker=%v qdrant=%s)", workerID, brokerAddrs, qdrantAddr)

	// ── Poll loop ────────────────────────────────────────────────────────────
	for {
		ctx := context.Background()

		task, hasTask, err := brokerClient.Poll(ctx, workerID)
		if err != nil {
			log.Printf("poll error: %v", err)
			time.Sleep(PollInterval)
			continue
		}
		if !hasTask {
			time.Sleep(PollInterval)
			continue
		}

		// Heartbeat goroutine: runs for the lifetime of this task.
		hbCtx, cancelHB := context.WithCancel(context.Background())
		go func() {
			ticker := time.NewTicker(HeartbeatInterval)
			defer ticker.Stop()
			for {
				select {
				case <-hbCtx.Done():
					return
				case <-ticker.C:
					if err := brokerClient.Heartbeat(hbCtx, workerID, task.Id); err != nil && hbCtx.Err() == nil {
						log.Printf("heartbeat task %s: %v", task.Id, err)
					}
				}
			}
		}()

		var p taskPayload
		if err := json.Unmarshal([]byte(task.Payload), &p); err != nil {
			log.Printf("decode task %s: %v", task.Id, err)
			cancelHB()
			brokerClient.Complete(ctx, task.Id, workerID, err.Error()) //nolint:errcheck
			return fmt.Errorf("decode task payload: %w", err)
		}

		embedResp, err := emb.Embed(embedder.Request{ChunkID: p.ChunkID, Text: p.Text})
		if err != nil {
			log.Printf("embed task %s: %v", task.Id, err)
			cancelHB()
			brokerClient.Complete(ctx, task.Id, workerID, err.Error()) //nolint:errcheck
			return fmt.Errorf("embed task: %w", err)
		}

		if err := upsertVector(ctx, qdrantClient, p, embedResp.Vector); err != nil {
			log.Printf("upsert task %s: %v", task.Id, err)
			cancelHB()
			brokerClient.Complete(ctx, task.Id, workerID, err.Error()) //nolint:errcheck
			return fmt.Errorf("upsert task: %w", err)
		}

		cancelHB()
		if err := brokerClient.Complete(ctx, task.Id, workerID, ""); err != nil {
			log.Printf("complete task %s: %v", task.Id, err)
		}
	}
}

func main() {
	brokerAddrs := flag.String("broker", "localhost:9000,localhost:9001,localhost:9002", "comma-separated broker gRPC addresses (client-facing)")
	qdrantAddr := flag.String("qdrant", "localhost:6334", "qdrant gRPC address (port 6334)")
	embedderCmd := flag.String("embedder", "", "embedding subprocess command (e.g. 'python3 tools/embedder/embedder.py')")
	flag.Parse()

	if *embedderCmd == "" {
		log.Fatal("--embedder is required")
	}

	if err := run(strings.Split(*brokerAddrs, ","), *qdrantAddr, *embedderCmd); err != nil {
		log.Fatal(err)
	}
}
