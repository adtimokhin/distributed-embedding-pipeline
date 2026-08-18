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
	"os"
	"strings"
	"time"

	"pipeline/internal/brokerclient"
	"pipeline/internal/embedder"
	"pipeline/internal/indexer"

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
)

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
	collClient := qdrant.NewCollectionsClient(qdrantConn)
	if err := indexer.EnsureCollection(context.Background(), collClient); err != nil {
		return fmt.Errorf("create qdrant collection: %w", err)
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

		var c indexer.Chunk
		if err := json.Unmarshal([]byte(task.Payload), &c); err != nil {
			log.Printf("decode task %s: %v", task.Id, err)
			cancelHB()
			brokerClient.Complete(ctx, task.Id, workerID, err.Error()) //nolint:errcheck
			return fmt.Errorf("decode task payload: %w", err)
		}

		embedResp, err := emb.Embed(embedder.Request{ChunkID: c.ChunkID, Text: c.Text})
		if err != nil {
			log.Printf("embed task %s: %v", task.Id, err)
			cancelHB()
			brokerClient.Complete(ctx, task.Id, workerID, err.Error()) //nolint:errcheck
			return fmt.Errorf("embed task: %w", err)
		}

		if err := indexer.UpsertChunk(ctx, qdrantClient, c, embedResp.Vector); err != nil {
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
