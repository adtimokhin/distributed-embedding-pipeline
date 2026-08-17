// Query CLI — HW4 embedding pipeline semantic search.
//
// Embeds a natural-language query using the same long-lived embedder
// subprocess pattern as the worker, then runs a kNN search against Qdrant
// and prints the top-K passages. Thin wrapper around internal/retrieval,
// the same search logic the HTTP retrieval API (queryserver/) uses.

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"strings"

	"pipeline/internal/embedder"
	"pipeline/internal/retrieval"

	qdrant "github.com/qdrant/go-client/qdrant"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// ──────────────────────────────────────────────────────────────────────────────
// Main
// ──────────────────────────────────────────────────────────────────────────────

// run embeds queryText using emb — a subprocess started once by main and
// reused across every query, rather than spawned and killed per call — then
// runs a kNN search against Qdrant and prints the results.
func run(emb *embedder.Embedder, queryText, qdrantAddr string, topK int) error {
	conn, err := grpc.NewClient(qdrantAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial qdrant: %w", err)
	}
	defer conn.Close()
	pointsClient := qdrant.NewPointsClient(conn)

	results, err := retrieval.Search(context.Background(), pointsClient, emb, queryText, topK)
	if err != nil {
		return err
	}

	if len(results) == 0 {
		fmt.Println("no results")
		return nil
	}

	for i, r := range results {
		text := r.Text
		if len(text) > 200 {
			text = text[:200] + "..."
		}
		fmt.Printf("[%d] %s (score=%.4f) — %s\n", i+1, r.Title, r.Score, text)
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
