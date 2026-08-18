// Queryserver — HTTP retrieval API for the HW4 embedding pipeline.
//
// Exposes the same embed-query-then-kNN-search path the query CLI uses
// (internal/retrieval) as a long-running HTTP service, so an agent can call
// it synchronously instead of shelling out to a CLI. This is what makes the
// pipeline's index actually reachable from a RAG system — TO_IMPLEMENT.md
// item 3. It also exposes synchronous single-document ingestion
// (internal/indexer) — TO_IMPLEMENT.md item 4 — for adding one document an
// agent just produced without standing up the batch broker/producer/worker
// pipeline for it.
//
// POST /search   {"query": "...", "top_k": 5}
//
//	->  [{"doc_id": "...", "title": "...", "text": "...", "score": 0.83}, ...]
//
// POST /ingest   {"doc_id": "...", "title": "...", "text": "...", "chunk_size": 200}
//
//	->  {"doc_id": "...", "chunk_ids": ["doc42-0", "doc42-1", ...]}
//
// Re-ingesting the same doc_id overwrites its previous chunks (deterministic
// chunk-ID -> Qdrant point-ID mapping, same as the batch path) rather than
// duplicating them — but if the new text chunks into fewer passages than
// before, stale trailing chunks from the old version are not deleted; no
// delete/update path exists yet (see TO_IMPLEMENT.md's deferred items).
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
	"pipeline/internal/indexer"
	"pipeline/internal/retrieval"

	qdrant "github.com/qdrant/go-client/qdrant"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	defaultTopK      = 5
	maxTopK          = 50
	defaultChunkSize = 200 // words per chunk, matches producer's default
	rpcTimeout       = 10 * time.Second
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

type ingestRequest struct {
	DocID     string `json:"doc_id"`
	Title     string `json:"title"`
	Text      string `json:"text"`
	ChunkSize int    `json:"chunk_size"`
}

type ingestResponse struct {
	DocID    string   `json:"doc_id"`
	ChunkIDs []string `json:"chunk_ids"`
}

// handleIngest chunks, embeds, and upserts one document synchronously —
// TO_IMPLEMENT.md item 4. Unlike the batch pipeline, this never touches the
// broker: it's the "agent just produced a document, index it now" path, not
// bulk (re)indexing.
func (s *server) handleIngest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "only POST is supported")
		return
	}

	var req ingestRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid request body: %v", err))
		return
	}
	if req.DocID == "" {
		writeError(w, http.StatusBadRequest, "doc_id must not be empty")
		return
	}
	if req.Text == "" {
		writeError(w, http.StatusBadRequest, "text must not be empty")
		return
	}

	chunkSize := req.ChunkSize
	if chunkSize <= 0 {
		chunkSize = defaultChunkSize
	}

	ctx, cancel := context.WithTimeout(r.Context(), rpcTimeout)
	defer cancel()

	chunkIDs, err := indexer.IngestDocument(ctx, s.pointsClient, s.emb, req.DocID, req.Title, req.Text, chunkSize)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(ingestResponse{DocID: req.DocID, ChunkIDs: chunkIDs}) //nolint:errcheck
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

	// Ensure the collection exists — queryserver may be the first process to
	// touch Qdrant if /ingest is called before any batch worker has run.
	if err := indexer.EnsureCollection(context.Background(), qdrant.NewCollectionsClient(conn)); err != nil {
		log.Fatalf("create qdrant collection: %v", err)
	}

	emb, err := embedder.Start(*embedderCmd)
	if err != nil {
		log.Fatalf("start embedder: %v", err)
	}
	defer emb.Close() //nolint:errcheck

	srv := &server{pointsClient: qdrant.NewPointsClient(conn), emb: emb}

	mux := http.NewServeMux()
	mux.HandleFunc("/search", srv.handleSearch)
	mux.HandleFunc("/ingest", srv.handleIngest)
	mux.HandleFunc("/healthz", handleHealthz)

	log.Printf("queryserver listening on %s (qdrant=%s)", *addr, *qdrantAddr)
	log.Fatal(http.ListenAndServe(*addr, mux))
}
