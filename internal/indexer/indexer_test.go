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

func TestChunkTextSplitsAtMaxWords(t *testing.T) {
	chunks := ChunkText("one two three four five six seven", 3)
	want := []string{"one two three", "four five six", "seven"}
	if len(chunks) != len(want) {
		t.Fatalf("got %d chunks, want %d: %v", len(chunks), len(want), chunks)
	}
	for i := range want {
		if chunks[i] != want[i] {
			t.Errorf("chunk %d = %q, want %q", i, chunks[i], want[i])
		}
	}
}

func TestChunkTextShorterThanMaxWords(t *testing.T) {
	chunks := ChunkText("just a few words", 200)
	if len(chunks) != 1 {
		t.Fatalf("got %d chunks, want 1: %v", len(chunks), chunks)
	}
	if chunks[0] != "just a few words" {
		t.Errorf("chunk = %q, want the whole input back unchanged", chunks[0])
	}
}

func TestChunkTextEmpty(t *testing.T) {
	chunks := ChunkText("", 200)
	if len(chunks) != 0 {
		t.Errorf("got %d chunks for empty input, want 0: %v", len(chunks), chunks)
	}
}

func TestChunkTextUnicodeWhitespace(t *testing.T) {
	chunks := ChunkText("one\ttwo\nthree four", 2)
	want := []string{"one two", "three four"}
	if len(chunks) != len(want) {
		t.Fatalf("got %d chunks, want %d: %v", len(chunks), len(want), chunks)
	}
}

// ── ChunkIDToUUID ────────────────────────────────────────────────────────────

func TestChunkIDToUUIDIsDeterministic(t *testing.T) {
	a := ChunkIDToUUID("doc42-3")
	b := ChunkIDToUUID("doc42-3")
	if a.GetNum() != b.GetNum() {
		t.Errorf("same chunk ID produced different point IDs: %d vs %d", a.GetNum(), b.GetNum())
	}
}

func TestChunkIDToUUIDDiffersAcrossIDs(t *testing.T) {
	a := ChunkIDToUUID("doc42-0")
	b := ChunkIDToUUID("doc42-1")
	if a.GetNum() == b.GetNum() {
		t.Error("different chunk IDs hashed to the same point ID (collision or bug)")
	}
}

// ── EnsureCollection ─────────────────────────────────────────────────────────

type fakeCollectionsClient struct {
	qdrant.CollectionsClient
	exists       bool
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

// ── UpsertChunk ──────────────────────────────────────────────────────────────

type fakePointsClient struct {
	qdrant.PointsClient
	lastUpsert *qdrant.UpsertPoints
}

func (f *fakePointsClient) Upsert(_ context.Context, in *qdrant.UpsertPoints, _ ...grpc.CallOption) (*qdrant.PointsOperationResponse, error) {
	f.lastUpsert = in
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

type countingPointsClient struct {
	qdrant.PointsClient
	upsertCount int
}

func (f *countingPointsClient) Upsert(_ context.Context, _ *qdrant.UpsertPoints, _ ...grpc.CallOption) (*qdrant.PointsOperationResponse, error) {
	f.upsertCount++
	return &qdrant.PointsOperationResponse{}, nil
}
