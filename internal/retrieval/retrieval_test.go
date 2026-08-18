package retrieval

import (
	"context"
	"fmt"
	"testing"

	"pipeline/internal/embedder"
	"pipeline/internal/testutil"

	qdrant "github.com/qdrant/go-client/qdrant"
	"google.golang.org/grpc"
)

// fakeSearchClient is a scriptable qdrant.PointsClient that records the
// SearchPoints request it received and returns a canned response.
type fakeSearchClient struct {
	qdrant.PointsClient
	lastRequest *qdrant.SearchPoints
	response    *qdrant.SearchResponse
}

func (f *fakeSearchClient) Search(_ context.Context, in *qdrant.SearchPoints, _ ...grpc.CallOption) (*qdrant.SearchResponse, error) {
	f.lastRequest = in
	return f.response, nil
}

func startTestEmbedder(t *testing.T) *embedder.Embedder {
	t.Helper()
	bin := testutil.BuildMockEmbedder(t)
	emb, err := embedder.Start(bin)
	if err != nil {
		t.Fatalf("start embedder: %v", err)
	}
	t.Cleanup(func() { emb.Close() }) //nolint:errcheck
	return emb
}

func TestSearchEmbedsQueryAndMapsResults(t *testing.T) {
	emb := startTestEmbedder(t)
	fc := &fakeSearchClient{
		response: &qdrant.SearchResponse{
			Result: []*qdrant.ScoredPoint{
				{
					Score: 0.83,
					Payload: map[string]*qdrant.Value{
						"doc_id": {Kind: &qdrant.Value_StringValue{StringValue: "doc1"}},
						"title":  {Kind: &qdrant.Value_StringValue{StringValue: "Title 1"}},
						"text":   {Kind: &qdrant.Value_StringValue{StringValue: "Text 1"}},
					},
				},
				{
					Score: 0.42,
					Payload: map[string]*qdrant.Value{
						"doc_id": {Kind: &qdrant.Value_StringValue{StringValue: "doc2"}},
						"title":  {Kind: &qdrant.Value_StringValue{StringValue: "Title 2"}},
						"text":   {Kind: &qdrant.Value_StringValue{StringValue: "Text 2"}},
					},
				},
			},
		},
	}

	results, err := Search(context.Background(), fc, emb, "some query", 5)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}

	if fc.lastRequest == nil {
		t.Fatal("Qdrant Search was never called")
	}
	if fc.lastRequest.CollectionName != QdrantCollection {
		t.Errorf("CollectionName = %q, want %q", fc.lastRequest.CollectionName, QdrantCollection)
	}
	if fc.lastRequest.Limit != 5 {
		t.Errorf("Limit = %d, want 5", fc.lastRequest.Limit)
	}
	if len(fc.lastRequest.Vector) == 0 {
		t.Error("Vector sent to Qdrant is empty — query was not embedded")
	}

	if len(results) != 2 {
		t.Fatalf("got %d results, want 2", len(results))
	}
	if results[0].DocID != "doc1" || results[0].Title != "Title 1" || results[0].Text != "Text 1" || results[0].Score != 0.83 {
		t.Errorf("results[0] = %+v, fields don't match the scored point", results[0])
	}
	if results[1].DocID != "doc2" || results[1].Score != 0.42 {
		t.Errorf("results[1] = %+v, fields don't match the scored point", results[1])
	}
}

func TestSearchEmptyResultsReturnsEmptySlice(t *testing.T) {
	emb := startTestEmbedder(t)
	fc := &fakeSearchClient{response: &qdrant.SearchResponse{Result: nil}}

	results, err := Search(context.Background(), fc, emb, "query", 5)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("got %d results, want 0", len(results))
	}
}

func TestSearchMissingPayloadFieldsDoNotPanic(t *testing.T) {
	emb := startTestEmbedder(t)
	fc := &fakeSearchClient{
		response: &qdrant.SearchResponse{
			Result: []*qdrant.ScoredPoint{
				{Score: 0.5, Payload: map[string]*qdrant.Value{}}, // no doc_id/title/text at all
			},
		},
	}

	results, err := Search(context.Background(), fc, emb, "query", 5)
	if err != nil {
		t.Fatalf("Search: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("got %d results, want 1", len(results))
	}
	if results[0].DocID != "" || results[0].Title != "" || results[0].Text != "" {
		t.Errorf("results[0] = %+v, want all-empty string fields for a point with no matching payload keys", results[0])
	}
}

// fakeErroringSearchClient always fails Search, for testing that Qdrant
// errors propagate out of Search rather than being swallowed.
type fakeErroringSearchClient struct {
	qdrant.PointsClient
}

func (f *fakeErroringSearchClient) Search(_ context.Context, _ *qdrant.SearchPoints, _ ...grpc.CallOption) (*qdrant.SearchResponse, error) {
	return nil, fmt.Errorf("qdrant unavailable")
}

func TestSearchPropagatesQdrantError(t *testing.T) {
	emb := startTestEmbedder(t)
	fc := &fakeErroringSearchClient{}

	if _, err := Search(context.Background(), fc, emb, "query", 5); err == nil {
		t.Fatal("Search returned nil error when Qdrant Search failed, want the error propagated")
	}
}

func TestSearchZeroTopK(t *testing.T) {
	emb := startTestEmbedder(t)
	fc := &fakeSearchClient{response: &qdrant.SearchResponse{}}

	if _, err := Search(context.Background(), fc, emb, "query", 0); err != nil {
		t.Fatalf("Search with topK=0: %v", err)
	}
	if fc.lastRequest.Limit != 0 {
		t.Errorf("Limit sent to Qdrant = %d, want 0 (Search itself doesn't default topK — that's the HTTP layer's job)", fc.lastRequest.Limit)
	}
}
