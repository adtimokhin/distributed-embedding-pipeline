package indexer

import (
	"context"
	"fmt"
	"testing"

	"pipeline/internal/embedder"
	"pipeline/internal/testutil"

	qdrant "github.com/qdrant/go-client/qdrant"
	"google.golang.org/grpc"
)

// ── ChunkText ────────────────────────────────────────────────────────────────

func TestChunkText(t *testing.T) {
	cases := []struct {
		name     string
		text     string
		maxWords int
		want     []string
	}{
		{
			name:     "splits at max words",
			text:     "one two three four five six seven",
			maxWords: 3,
			want:     []string{"one two three", "four five six", "seven"},
		},
		{
			name:     "shorter than max words is a single chunk",
			text:     "just a few words",
			maxWords: 200,
			want:     []string{"just a few words"},
		},
		{
			name:     "empty text produces no chunks",
			text:     "",
			maxWords: 200,
			want:     nil,
		},
		{
			name:     "whitespace-only text produces no chunks",
			text:     "   \t\n  ",
			maxWords: 200,
			want:     nil,
		},
		{
			name:     "unicode whitespace boundaries (tab, newline)",
			text:     "one\ttwo\nthree four",
			maxWords: 2,
			want:     []string{"one two", "three four"},
		},
		{
			name:     "single word",
			text:     "hello",
			maxWords: 200,
			want:     []string{"hello"},
		},
		{
			name:     "exact multiple of max words leaves no short trailing chunk",
			text:     "one two three four",
			maxWords: 2,
			want:     []string{"one two", "three four"},
		},
		{
			// Regression: maxWords <= 0 used to loop forever (i += 0 never
			// advances past len(words)). Now treated as "don't split".
			name:     "zero max words does not hang, returns one chunk",
			text:     "one two three",
			maxWords: 0,
			want:     []string{"one two three"},
		},
		{
			// Regression: negative maxWords used to panic on the first
			// negative slice index (i += a negative number).
			name:     "negative max words does not panic, returns one chunk",
			text:     "one two three",
			maxWords: -5,
			want:     []string{"one two three"},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := ChunkText(c.text, c.maxWords)
			if len(got) != len(c.want) {
				t.Fatalf("got %d chunks %v, want %d chunks %v", len(got), got, len(c.want), c.want)
			}
			for i := range c.want {
				if got[i] != c.want[i] {
					t.Errorf("chunk %d = %q, want %q", i, got[i], c.want[i])
				}
			}
		})
	}
}

// ── ChunkIDToUUID ────────────────────────────────────────────────────────────

func TestChunkIDToUUID(t *testing.T) {
	t.Run("deterministic", func(t *testing.T) {
		a := ChunkIDToUUID("doc42-3")
		b := ChunkIDToUUID("doc42-3")
		if a.GetNum() != b.GetNum() {
			t.Errorf("same chunk ID produced different point IDs: %d vs %d", a.GetNum(), b.GetNum())
		}
	})

	t.Run("differs across IDs", func(t *testing.T) {
		a := ChunkIDToUUID("doc42-0")
		b := ChunkIDToUUID("doc42-1")
		if a.GetNum() == b.GetNum() {
			t.Error("different chunk IDs hashed to the same point ID (collision or bug)")
		}
	})

	t.Run("empty chunk ID does not panic", func(t *testing.T) {
		id := ChunkIDToUUID("")
		_ = id.GetNum() // must not panic
	})
}

// ── EnsureCollection ─────────────────────────────────────────────────────────

type fakeCollectionsClient struct {
	qdrant.CollectionsClient
	exists       bool
	createErr    error
	createCalled bool
}

func (f *fakeCollectionsClient) Get(_ context.Context, _ *qdrant.GetCollectionInfoRequest, _ ...grpc.CallOption) (*qdrant.GetCollectionInfoResponse, error) {
	if f.exists {
		return &qdrant.GetCollectionInfoResponse{}, nil
	}
	return nil, fmt.Errorf("collection not found")
}

func (f *fakeCollectionsClient) Create(_ context.Context, _ *qdrant.CreateCollection, _ ...grpc.CallOption) (*qdrant.CollectionOperationResponse, error) {
	f.createCalled = true
	if f.createErr != nil {
		return nil, f.createErr
	}
	return &qdrant.CollectionOperationResponse{Result: true}, nil
}

func TestEnsureCollectionSkipsIfAlreadyExists(t *testing.T) {
	fc := &fakeCollectionsClient{exists: true}
	if err := EnsureCollection(context.Background(), fc); err != nil {
		t.Fatalf("EnsureCollection: %v", err)
	}
	if fc.createCalled {
		t.Error("Create was called even though the collection already exists")
	}
}

func TestEnsureCollectionCreatesIfMissing(t *testing.T) {
	fc := &fakeCollectionsClient{exists: false}
	if err := EnsureCollection(context.Background(), fc); err != nil {
		t.Fatalf("EnsureCollection: %v", err)
	}
	if !fc.createCalled {
		t.Error("Create was not called even though the collection doesn't exist")
	}
}

func TestEnsureCollectionPropagatesCreateError(t *testing.T) {
	fc := &fakeCollectionsClient{exists: false, createErr: fmt.Errorf("qdrant unavailable")}
	if err := EnsureCollection(context.Background(), fc); err == nil {
		t.Fatal("EnsureCollection returned nil error when Create failed, want the error propagated")
	}
}

// ── UpsertChunk ──────────────────────────────────────────────────────────────

type fakePointsClient struct {
	qdrant.PointsClient
	lastUpsert *qdrant.UpsertPoints
	upsertErr  error
}

func (f *fakePointsClient) Upsert(_ context.Context, in *qdrant.UpsertPoints, _ ...grpc.CallOption) (*qdrant.PointsOperationResponse, error) {
	f.lastUpsert = in
	if f.upsertErr != nil {
		return nil, f.upsertErr
	}
	return &qdrant.PointsOperationResponse{}, nil
}

func TestUpsertChunkWritesExpectedPayload(t *testing.T) {
	fp := &fakePointsClient{}
	c := Chunk{DocID: "doc1", ChunkID: "doc1-0", Title: "Title", Text: "some text"}
	vector := []float32{0.1, 0.2, 0.3}

	if err := UpsertChunk(context.Background(), fp, c, vector); err != nil {
		t.Fatalf("UpsertChunk: %v", err)
	}
	if fp.lastUpsert == nil {
		t.Fatal("Upsert was never called")
	}
	if fp.lastUpsert.CollectionName != QdrantCollection {
		t.Errorf("CollectionName = %q, want %q", fp.lastUpsert.CollectionName, QdrantCollection)
	}
	if len(fp.lastUpsert.Points) != 1 {
		t.Fatalf("got %d points, want 1", len(fp.lastUpsert.Points))
	}
	point := fp.lastUpsert.Points[0]
	if point.Id.GetNum() != ChunkIDToUUID("doc1-0").GetNum() {
		t.Error("point ID does not match ChunkIDToUUID(chunk_id)")
	}
	payload := point.Payload
	if payload["doc_id"].GetStringValue() != "doc1" {
		t.Errorf(`payload["doc_id"] = %q, want "doc1"`, payload["doc_id"].GetStringValue())
	}
	if payload["title"].GetStringValue() != "Title" {
		t.Errorf(`payload["title"] = %q, want "Title"`, payload["title"].GetStringValue())
	}
	if payload["text"].GetStringValue() != "some text" {
		t.Errorf(`payload["text"] = %q, want "some text"`, payload["text"].GetStringValue())
	}
}

func TestUpsertChunkPropagatesQdrantError(t *testing.T) {
	fp := &fakePointsClient{upsertErr: fmt.Errorf("connection reset")}
	c := Chunk{DocID: "doc1", ChunkID: "doc1-0"}
	if err := UpsertChunk(context.Background(), fp, c, []float32{0.1}); err == nil {
		t.Fatal("UpsertChunk returned nil error when Qdrant Upsert failed, want the error propagated")
	}
}

// ── IngestDocument ───────────────────────────────────────────────────────────
//
// IngestDocument takes a concrete *embedder.Embedder, not an interface, so
// these need a real (if fake-vector-producing) subprocess.

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

// countingPointsClient records how many times Upsert was called and can be
// scripted to fail starting at a given call (1-indexed) to simulate a
// partial-document ingestion failure.
type countingPointsClient struct {
	qdrant.PointsClient
	upsertCount int
	failAtCall  int // 0 = never fail
}

func (f *countingPointsClient) Upsert(_ context.Context, _ *qdrant.UpsertPoints, _ ...grpc.CallOption) (*qdrant.PointsOperationResponse, error) {
	f.upsertCount++
	if f.failAtCall != 0 && f.upsertCount == f.failAtCall {
		return nil, fmt.Errorf("simulated upsert failure on call %d", f.upsertCount)
	}
	return &qdrant.PointsOperationResponse{}, nil
}

func TestIngestDocumentChunksEmbedsAndUpserts(t *testing.T) {
	emb := startTestEmbedder(t)
	fp := &countingPointsClient{}

	chunkIDs, err := IngestDocument(context.Background(), fp, emb, "doc1", "Title", "one two three four five six", 2)
	if err != nil {
		t.Fatalf("IngestDocument: %v", err)
	}

	wantIDs := []string{"doc1-0", "doc1-1", "doc1-2"} // 6 words / 2 per chunk = 3 chunks
	if len(chunkIDs) != len(wantIDs) {
		t.Fatalf("got chunk IDs %v, want %v", chunkIDs, wantIDs)
	}
	for i := range wantIDs {
		if chunkIDs[i] != wantIDs[i] {
			t.Errorf("chunkIDs[%d] = %q, want %q", i, chunkIDs[i], wantIDs[i])
		}
	}
	if fp.upsertCount != 3 {
		t.Errorf("Upsert was called %d times, want 3 (one per chunk)", fp.upsertCount)
	}
}

func TestIngestDocumentEmptyTextErrors(t *testing.T) {
	emb := startTestEmbedder(t)
	fp := &countingPointsClient{}

	if _, err := IngestDocument(context.Background(), fp, emb, "doc1", "Title", "", 200); err == nil {
		t.Fatal("IngestDocument with empty text returned nil error, want an error")
	}
}

// TestIngestDocumentZeroChunkSizeDoesNotHang is a regression test for the
// ChunkText(maxWords<=0) fix: before it, this call would have hung forever
// instead of returning.
func TestIngestDocumentZeroChunkSizeDoesNotHang(t *testing.T) {
	emb := startTestEmbedder(t)
	fp := &countingPointsClient{}

	chunkIDs, err := IngestDocument(context.Background(), fp, emb, "doc1", "Title", "one two three", 0)
	if err != nil {
		t.Fatalf("IngestDocument: %v", err)
	}
	if len(chunkIDs) != 1 {
		t.Errorf("got chunk IDs %v, want exactly one chunk (chunk_size<=0 means don't split)", chunkIDs)
	}
}

// TestIngestDocumentPartialFailureReturnsChunksWrittenSoFar documents the
// behavior IngestDocument's doc comment promises: if a later chunk fails,
// the caller still learns which earlier chunks succeeded, rather than
// getting an all-or-nothing nil.
func TestIngestDocumentPartialFailureReturnsChunksWrittenSoFar(t *testing.T) {
	emb := startTestEmbedder(t)
	fp := &countingPointsClient{failAtCall: 2} // second chunk's upsert fails

	chunkIDs, err := IngestDocument(context.Background(), fp, emb, "doc1", "Title", "one two three four five six", 2)
	if err == nil {
		t.Fatal("IngestDocument returned nil error despite a simulated upsert failure")
	}
	if len(chunkIDs) != 1 || chunkIDs[0] != "doc1-0" {
		t.Errorf("chunkIDs = %v, want [doc1-0] (only the chunk that succeeded before the failure)", chunkIDs)
	}
}
