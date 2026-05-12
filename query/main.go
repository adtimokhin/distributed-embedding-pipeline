// Query CLI — HW4 embedding pipeline semantic search.
//
// Embeds a natural-language query using the same subprocess protocol as the
// worker, then runs a kNN search against Qdrant and prints the top-K passages.

package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os/exec"
	"strings"

	qdrant "github.com/qdrant/go-client/qdrant"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const QdrantCollection = "documents"

// ──────────────────────────────────────────────────────────────────────────────
// Subprocess protocol (same as worker)
// ──────────────────────────────────────────────────────────────────────────────

type embedRequest struct {
	ChunkID string `json:"chunk_id"`
	Text    string `json:"text"`
}

type embedResponse struct {
	ChunkID string    `json:"chunk_id"`
	Vector  []float32 `json:"vector"`
}

// embedQuery embeds the query string using the subprocess at cmdStr.
// Spawns and tears down the subprocess for each query (queries are infrequent).
func embedQuery(cmdStr, queryText string) ([]float32, error) {
	parts := strings.Fields(cmdStr)
	if len(parts) == 0 {
		return nil, fmt.Errorf("empty embedder command")
	}

	cmd := exec.Command(parts[0], parts[1:]...)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start embedder: %w", err)
	}
	defer cmd.Process.Kill() //nolint:errcheck

	w := bufio.NewWriter(stdin)
	r := bufio.NewScanner(stdout)

	req := embedRequest{ChunkID: "query", Text: queryText}
	line, _ := json.Marshal(req)
	if _, err := fmt.Fprintf(w, "%s\n", line); err != nil {
		return nil, err
	}
	if err := w.Flush(); err != nil {
		return nil, err
	}

	if !r.Scan() {
		return nil, fmt.Errorf("no response from embedder")
	}
	var resp embedResponse
	if err := json.Unmarshal(r.Bytes(), &resp); err != nil {
		return nil, fmt.Errorf("decode embedder response: %w", err)
	}
	return resp.Vector, nil
}

// ──────────────────────────────────────────────────────────────────────────────
// Main
// ──────────────────────────────────────────────────────────────────────────────

func run(queryText, qdrantAddr, embedderCmd string, topK int) error {
	// ── Embed the query ──────────────────────────────────────────────────────
	vector, err := embedQuery(embedderCmd, queryText)
	if err != nil {
		return fmt.Errorf("embed query: %w", err)
	}

	// ── Connect to Qdrant ────────────────────────────────────────────────────
	conn, err := grpc.NewClient(qdrantAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial qdrant: %w", err)
	}
	defer conn.Close()
	pointsClient := qdrant.NewPointsClient(conn)

	// ── kNN search ───────────────────────────────────────────────────────────
	// TODO: call pointsClient.Search with the query vector and return the top-K results.
	// Print each result as: [rank] title — text[:200]...
	//
	// Hint: use qdrant.SearchPoints with:
	//   CollectionName: QdrantCollection
	//   Vector:         vector (as []float32)
	//   Limit:          uint64(topK)
	//   WithPayload:    &qdrant.WithPayloadSelector{SelectorOptions: &qdrant.WithPayloadSelector_Enable{Enable: true}}
	ctx := context.Background()
	_ = ctx
	_ = pointsClient
	_ = vector

	return fmt.Errorf("query search: not implemented")
}

func main() {
	qdrantAddr  := flag.String("qdrant",   "localhost:6334", "qdrant gRPC address (port 6334)")
	embedderCmd := flag.String("embedder", "python3 tools/embedder/embedder.py", "embedding subprocess command")
	topK        := flag.Int("top",         5,                "number of results to return")
	flag.Parse()

	if flag.NArg() < 1 {
		log.Fatal("usage: query [flags] <query text>")
	}
	queryText := strings.Join(flag.Args(), " ")

	if err := run(queryText, *qdrantAddr, *embedderCmd, *topK); err != nil {
		log.Fatal(err)
	}
}
