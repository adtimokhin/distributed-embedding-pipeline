// Query CLI — HW4 embedding pipeline semantic search.
//
// Embeds a natural-language query using the same long-lived embedder
// subprocess pattern as the worker, then runs a kNN search against Qdrant
// and prints the top-K passages.

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"strings"

	"pipeline/internal/embedder"

	qdrant "github.com/qdrant/go-client/qdrant"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const QdrantCollection = "documents"

// ──────────────────────────────────────────────────────────────────────────────
// Main
// ──────────────────────────────────────────────────────────────────────────────

// run embeds queryText using emb — a subprocess started once by main and
// reused across every query, rather than spawned and killed per call — then
// runs a kNN search against Qdrant.
func run(emb *embedder.Embedder, queryText, qdrantAddr string, topK int) error {
	// ── Embed the query ──────────────────────────────────────────────────────
	embedResp, err := emb.Embed(embedder.Request{ChunkID: "query", Text: queryText})
	if err != nil {
		return fmt.Errorf("embed query: %w", err)
	}
	vector := embedResp.Vector

	// ── Connect to Qdrant ────────────────────────────────────────────────────
	conn, err := grpc.NewClient(qdrantAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial qdrant: %w", err)
	}
	defer conn.Close()
	pointsClient := qdrant.NewPointsClient(conn)

	// ── kNN search ───────────────────────────────────────────────────────────
	ctx := context.Background()
	resp, err := pointsClient.Search(ctx, &qdrant.SearchPoints{
		CollectionName: QdrantCollection,
		Vector:         vector,
		Limit:          uint64(topK),
		WithPayload:    &qdrant.WithPayloadSelector{SelectorOptions: &qdrant.WithPayloadSelector_Enable{Enable: true}},
	})
	if err != nil {
		return fmt.Errorf("qdrant search: %w", err)
	}

	if len(resp.Result) == 0 {
		fmt.Println("no results")
		return nil
	}

	for i, point := range resp.Result {
		title := point.Payload["title"].GetStringValue()
		text := point.Payload["text"].GetStringValue()
		if len(text) > 200 {
			text = text[:200] + "..."
		}
		fmt.Printf("[%d] %s (score=%.4f) — %s\n", i+1, title, point.Score, text)
	}

	return nil
}

func main() {
	qdrantAddr := flag.String("qdrant", "localhost:6334", "qdrant gRPC address (port 6334)")
	embedderCmd := flag.String("embedder", "python3 tools/embedder/embedder.py", "embedding subprocess command")
	topK := flag.Int("top", 5, "number of results to return")
	flag.Parse()

	if flag.NArg() < 1 {
		log.Fatal("usage: query [flags] <query text>")
	}
	queryText := strings.Join(flag.Args(), " ")

	emb, err := embedder.Start(*embedderCmd)
	if err != nil {
		log.Fatalf("start embedder: %v", err)
	}
	defer emb.Close() //nolint:errcheck

	if err := run(emb, queryText, *qdrantAddr, *topK); err != nil {
		log.Fatal(err)
	}
}
