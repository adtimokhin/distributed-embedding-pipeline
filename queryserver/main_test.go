// Tests for queryserver's HTTP handlers: request validation, defaults, and
// status codes. internal/retrieval.Search and internal/indexer.IngestDocument
// (the logic these handlers call into) have their own dedicated tests — these
// exercise the HTTP-specific layer on top: JSON decoding, method checks,
// top_k/chunk_size defaulting and clamping, error responses.
package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"pipeline/internal/embedder"
	"pipeline/internal/retrieval"
	"pipeline/internal/testutil"

	qdrant "github.com/qdrant/go-client/qdrant"
	"google.golang.org/grpc"
)

// fakePointsClient scripts Search/Upsert responses and records the last
// request of each, for asserting what the handlers actually sent to Qdrant.
type fakePointsClient struct {
	qdrant.PointsClient
	searchResponse *qdrant.SearchResponse
	lastSearch     *qdrant.SearchPoints
	lastUpsert     *qdrant.UpsertPoints
	upsertCount    int
}

func (f *fakePointsClient) Search(_ context.Context, in *qdrant.SearchPoints, _ ...grpc.CallOption) (*qdrant.SearchResponse, error) {
	f.lastSearch = in
	if f.searchResponse == nil {
		return &qdrant.SearchResponse{}, nil
	}
	return f.searchResponse, nil
}

func (f *fakePointsClient) Upsert(_ context.Context, in *qdrant.UpsertPoints, _ ...grpc.CallOption) (*qdrant.PointsOperationResponse, error) {
	f.lastUpsert = in
	f.upsertCount++
	return &qdrant.PointsOperationResponse{}, nil
}

func newTestServer(t *testing.T) (*server, *fakePointsClient) {
	t.Helper()
	bin := testutil.BuildMockEmbedder(t)
	emb, err := embedder.Start(bin)
	if err != nil {
		t.Fatalf("start embedder: %v", err)
	}
	t.Cleanup(func() { emb.Close() }) //nolint:errcheck

	fp := &fakePointsClient{}
	return &server{pointsClient: fp, emb: emb}, fp
}

func doRequest(t *testing.T, handler http.HandlerFunc, method, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, "/", strings.NewReader(body))
	rec := httptest.NewRecorder()
	handler(rec, req)
	return rec
}

// ── /search ──────────────────────────────────────────────────────────────────

func TestHandleSearchSuccess(t *testing.T) {
	srv, fp := newTestServer(t)
	fp.searchResponse = &qdrant.SearchResponse{
		Result: []*qdrant.ScoredPoint{
			{Score: 0.9, Payload: map[string]*qdrant.Value{
				"doc_id": {Kind: &qdrant.Value_StringValue{StringValue: "d1"}},
				"title":  {Kind: &qdrant.Value_StringValue{StringValue: "T"}},
				"text":   {Kind: &qdrant.Value_StringValue{StringValue: "X"}},
			}},
		},
	}

	rec := doRequest(t, srv.handleSearch, http.MethodPost, `{"query":"hello","top_k":3}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}

	var results []retrieval.Result
	if err := json.Unmarshal(rec.Body.Bytes(), &results); err != nil {
		t.Fatalf("decode response: %v; body=%s", err, rec.Body.String())
	}
	if len(results) != 1 || results[0].DocID != "d1" {
		t.Errorf("results = %+v, want one result with doc_id=d1", results)
	}
	if fp.lastSearch.Limit != 3 {
		t.Errorf("Limit sent to Qdrant = %d, want 3 (from top_k)", fp.lastSearch.Limit)
	}
}

func TestHandleSearchDefaultsTopK(t *testing.T) {
	srv, fp := newTestServer(t)
	rec := doRequest(t, srv.handleSearch, http.MethodPost, `{"query":"hello"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	if fp.lastSearch.Limit != defaultTopK {
		t.Errorf("Limit = %d, want default %d", fp.lastSearch.Limit, defaultTopK)
	}
}

func TestHandleSearchClampsTopKToMax(t *testing.T) {
	srv, fp := newTestServer(t)
	rec := doRequest(t, srv.handleSearch, http.MethodPost, `{"query":"hello","top_k":1000}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	if fp.lastSearch.Limit != maxTopK {
		t.Errorf("Limit = %d, want clamped to %d", fp.lastSearch.Limit, maxTopK)
	}
}

func TestHandleSearchEmptyQueryReturns400(t *testing.T) {
	srv, _ := newTestServer(t)
	rec := doRequest(t, srv.handleSearch, http.MethodPost, `{"query":""}`)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400; body=%s", rec.Code, rec.Body.String())
	}
}

func TestHandleSearchMalformedJSONReturns400(t *testing.T) {
	srv, _ := newTestServer(t)
	rec := doRequest(t, srv.handleSearch, http.MethodPost, `not json`)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400; body=%s", rec.Code, rec.Body.String())
	}
}

func TestHandleSearchGetMethodReturns405(t *testing.T) {
	srv, _ := newTestServer(t)
	rec := doRequest(t, srv.handleSearch, http.MethodGet, "")
	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("status = %d, want 405; body=%s", rec.Code, rec.Body.String())
	}
}

// ── /ingest ──────────────────────────────────────────────────────────────────

func TestHandleIngestSuccess(t *testing.T) {
	srv, fp := newTestServer(t)
	rec := doRequest(t, srv.handleIngest, http.MethodPost, `{"doc_id":"d1","title":"T","text":"one two three four"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}

	var resp ingestResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v; body=%s", err, rec.Body.String())
	}
	if resp.DocID != "d1" {
		t.Errorf("DocID = %q, want %q", resp.DocID, "d1")
	}
	if len(resp.ChunkIDs) != 1 || resp.ChunkIDs[0] != "d1-0" {
		t.Errorf("ChunkIDs = %v, want [d1-0] (default chunk_size fits this short text in one chunk)", resp.ChunkIDs)
	}
	if fp.upsertCount != 1 {
		t.Errorf("Upsert called %d times, want 1", fp.upsertCount)
	}
}

func TestHandleIngestRespectsChunkSize(t *testing.T) {
	srv, fp := newTestServer(t)
	rec := doRequest(t, srv.handleIngest, http.MethodPost, `{"doc_id":"d1","text":"one two three four","chunk_size":2}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	if fp.upsertCount != 2 {
		t.Errorf("Upsert called %d times, want 2 (4 words / chunk_size=2)", fp.upsertCount)
	}
}

func TestHandleIngestMissingDocIDReturns400(t *testing.T) {
	srv, _ := newTestServer(t)
	rec := doRequest(t, srv.handleIngest, http.MethodPost, `{"text":"hello"}`)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400; body=%s", rec.Code, rec.Body.String())
	}
}

func TestHandleIngestMissingTextReturns400(t *testing.T) {
	srv, _ := newTestServer(t)
	rec := doRequest(t, srv.handleIngest, http.MethodPost, `{"doc_id":"d1"}`)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400; body=%s", rec.Code, rec.Body.String())
	}
}

func TestHandleIngestGetMethodReturns405(t *testing.T) {
	srv, _ := newTestServer(t)
	rec := doRequest(t, srv.handleIngest, http.MethodGet, "")
	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("status = %d, want 405; body=%s", rec.Code, rec.Body.String())
	}
}

// ── /healthz ─────────────────────────────────────────────────────────────────

func TestHandleHealthz(t *testing.T) {
	rec := doRequest(t, handleHealthz, http.MethodGet, "")
	if rec.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", rec.Code)
	}
	if rec.Body.String() != "ok" {
		t.Errorf("body = %q, want %q", rec.Body.String(), "ok")
	}
}
