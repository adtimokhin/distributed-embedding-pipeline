// Package indexer implements chunking, embedding, and upserting a document
// into Qdrant — the write-side counterpart to internal/retrieval. Shared by
// the batch pipeline (worker/producer, via the broker) and the synchronous
// single-document ingestion path (queryserver's /ingest), so both write
// chunks the same way and stay compatible with the same deterministic
// point-ID scheme — a document re-ingested under the same doc_id overwrites
// its previous chunks rather than duplicating them.
package indexer

import (
	"context"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"unicode"

	"pipeline/internal/embedder"

	qdrant "github.com/qdrant/go-client/qdrant"
)

const (
	VectorDim        = 384
	QdrantCollection = "documents"
)

// Chunk is one embeddable passage of a document. DocID/ChunkID/Title/Text
// match the wire format the broker moves task payloads around as (JSON tags
// unchanged from the original per-binary taskPayload types) so existing
// in-flight tasks and this package agree on shape.
type Chunk struct {
	DocID   string `json:"doc_id"`
	ChunkID string `json:"chunk_id"`
	Title   string `json:"title"`
	Text    string `json:"text"`
}

// ChunkText splits text into passages of at most maxWords words. Word
// boundaries are Unicode whitespace. maxWords <= 0 is treated as "don't
// split" (the whole text as one chunk) rather than looping forever — the
// loop below advances by maxWords each iteration, so a non-positive step
// would never reach len(words).
func ChunkText(text string, maxWords int) []string {
	words := strings.FieldsFunc(text, unicode.IsSpace)
	if len(words) == 0 {
		return nil
	}
	if maxWords <= 0 {
		return []string{strings.Join(words, " ")}
	}
	var chunks []string
	for i := 0; i < len(words); i += maxWords {
		end := i + maxWords
		if end > len(words) {
			end = len(words)
		}
		chunks = append(chunks, strings.Join(words[i:end], " "))
	}
	return chunks
}

// ChunkIDToUUID deterministically converts a string chunk ID to a Qdrant
// point ID by treating the FNV-64 hash as the low 64 bits of a UUID. The
// same chunk ID always maps to the same point, which is what makes both
// at-least-once re-execution (batch path) and re-ingestion (sync path)
// idempotent instead of creating duplicate points.
func ChunkIDToUUID(chunkID string) *qdrant.PointId {
	return &qdrant.PointId{PointIdOptions: &qdrant.PointId_Num{Num: fnv64(chunkID)}}
}

func fnv64(s string) uint64 {
	const prime = 1099511628211
	h := big.NewInt(0)
	offset := new(big.Int).SetUint64(14695981039346656037)
	p := new(big.Int).SetUint64(prime)
	mod := new(big.Int).Lsh(big.NewInt(1), 64)
	h.Set(offset)
	for i := 0; i < len(s); i++ {
		h.Xor(h, big.NewInt(int64(s[i])))
		h.Mul(h, p)
		h.Mod(h, mod)
	}
	return h.Uint64()
}

// EnsureCollection creates the Qdrant collection if it doesn't already
// exist. Safe to call from every process that might be the first to touch
// Qdrant (worker during batch ingestion, queryserver serving a standalone
// /ingest call before any worker has run).
func EnsureCollection(ctx context.Context, collClient qdrant.CollectionsClient) error {
	if _, err := collClient.Get(ctx, &qdrant.GetCollectionInfoRequest{CollectionName: QdrantCollection}); err == nil {
		return nil
	}
	_, err := collClient.Create(ctx, &qdrant.CreateCollection{
		CollectionName: QdrantCollection,
		VectorsConfig: qdrant.NewVectorsConfig(&qdrant.VectorParams{
			Size:     VectorDim,
			Distance: qdrant.Distance_Cosine,
		}),
	})
	return err
}

// UpsertChunk writes one embedding point to the Qdrant collection.
func UpsertChunk(ctx context.Context, client qdrant.PointsClient, c Chunk, vector []float32) error {
	_, err := client.Upsert(ctx, &qdrant.UpsertPoints{
		CollectionName: QdrantCollection,
		Points: []*qdrant.PointStruct{
			{
				Id:      ChunkIDToUUID(c.ChunkID),
				Vectors: qdrant.NewVectors(vector...),
				Payload: qdrant.NewValueMap(map[string]any{
					"doc_id": c.DocID,
					"title":  c.Title,
					"text":   c.Text,
				}),
			},
		},
	})
	return err
}

// IngestDocument chunks, embeds, and upserts one document synchronously —
// the write-side counterpart to retrieval.Search: chunk -> embed -> upsert
// inline for a single document, instead of routing through the broker's
// Submit/Poll/Complete for a bulk batch. Returns the chunk IDs written, in
// order, even if a later chunk fails partway through (so the caller knows
// what already landed).
func IngestDocument(ctx context.Context, client qdrant.PointsClient, emb *embedder.Embedder, docID, title, text string, chunkSize int) ([]string, error) {
	passages := ChunkText(text, chunkSize)
	if len(passages) == 0 {
		return nil, fmt.Errorf("document has no content to chunk")
	}

	chunkIDs := make([]string, 0, len(passages))
	for i, passage := range passages {
		chunkID := docID + "-" + strconv.Itoa(i)

		embedResp, err := emb.Embed(embedder.Request{ChunkID: chunkID, Text: passage})
		if err != nil {
			return chunkIDs, fmt.Errorf("embed chunk %s: %w", chunkID, err)
		}

		c := Chunk{DocID: docID, ChunkID: chunkID, Title: title, Text: passage}
		if err := UpsertChunk(ctx, client, c, embedResp.Vector); err != nil {
			return chunkIDs, fmt.Errorf("upsert chunk %s: %w", chunkID, err)
		}

		chunkIDs = append(chunkIDs, chunkID)
	}
	return chunkIDs, nil
}
