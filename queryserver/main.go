// Queryserver — HTTP retrieval API for the HW4 embedding pipeline.
//
// Exposes the same embed-query-then-kNN-search path the query CLI uses
// (internal/retrieval) as a long-running HTTP service, so an agent can call
// it synchronously instead of shelling out to a CLI. This is what makes the
// pipeline's index actually reachable from a RAG system — TO_IMPLEMENT.md
// item 3.
//
// POST /search  {"query": "...", "top_k": 5}
//
//	->  [{"doc_id": "...", "title": "...", "text": "...", "score": 0.83}, ...]
//
// The embedder subprocess and Qdrant connection are both established once at
// startup and held for the server's lifetime (TO_IMPLEMENT.md item 2) rather
// than per-request.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"time"

	"pipeline/internal/embedder"
	"pipeline/internal/retrieval"

	qdrant "github.com/qdrant/go-client/qdrant"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	defaultTopK = 5
	maxTopK     = 50
	rpcTimeout  = 10 * time.Second
)

type server struct {
	pointsClient qdrant.PointsClient
	emb          *embedder.Embedder
}

type searchRequest struct {
	Query string `json:"query"`
	TopK  int    `json:"top_k"`
}

func (s *server) handleSearch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "only POST is supported")
		return
	}

	var req searchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid request body: %v", err))
		return
	}
	if req.Query == "" {
		writeError(w, http.StatusBadRequest, "query must not be empty")
		return
	}

	topK := req.TopK
	if topK <= 0 {
		topK = defaultTopK
	}
	if topK > maxTopK {
		topK = maxTopK
	}

	ctx, cancel := context.WithTimeout(r.Context(), rpcTimeout)
	defer cancel()

	results, err := retrieval.Search(ctx, s.pointsClient, s.emb, req.Query, topK)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if results == nil {
		results = []retrieval.Result{} // "[]", not "null", for an empty result set
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(results) //nolint:errcheck
}

func handleHealthz(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok")) //nolint:errcheck
}

func writeError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]string{"error": msg}) //nolint:errcheck
}

func main() {
	addr := flag.String("addr", ":8080", "HTTP listen address")
	qdrantAddr := flag.String("qdrant", "localhost:6334", "qdrant gRPC address (port 6334)")
	embedderCmd := flag.String("embedder", "python3 tools/embedder/embedder.py", "embedding subprocess command")
	flag.Parse()

	conn, err := grpc.NewClient(*qdrantAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("dial qdrant: %v", err)
	}
	defer conn.Close()

	emb, err := embedder.Start(*embedderCmd)
	if err != nil {
		log.Fatalf("start embedder: %v", err)
	}
	defer emb.Close() //nolint:errcheck

	srv := &server{pointsClient: qdrant.NewPointsClient(conn), emb: emb}

	mux := http.NewServeMux()
	mux.HandleFunc("/search", srv.handleSearch)
	mux.HandleFunc("/healthz", handleHealthz)

	log.Printf("queryserver listening on %s (qdrant=%s)", *addr, *qdrantAddr)
	log.Fatal(http.ListenAndServe(*addr, mux))
}
