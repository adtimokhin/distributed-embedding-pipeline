// Package retrieval implements the embed-query-then-kNN-search path shared
// by the query CLI and the HTTP retrieval API: turn a natural-language query
// into a vector via the embedder subprocess, then search Qdrant for the
// nearest indexed chunks.
package retrieval

import (
	"context"
	"fmt"

	"pipeline/internal/embedder"

	qdrant "github.com/qdrant/go-client/qdrant"
)

// QdrantCollection is the collection workers upsert chunks into
// (worker/main.go's QdrantCollection) — kept in sync manually since query
// serving and ingestion are separate binaries that don't share a config type.
const QdrantCollection = "documents"

// Result is one ranked passage, using the same field names the worker
// upserts into each point's payload (worker/main.go:160-164) so the API
// contract doesn't invent new metadata beyond what's already stored.
type Result struct {
	DocID string  `json:"doc_id"`
	Title string  `json:"title"`
	Text  string  `json:"text"`
	Score float32 `json:"score"`
}

// Search embeds queryText using emb, then runs a kNN search against Qdrant
// for the topK nearest chunks.
func Search(ctx context.Context, pointsClient qdrant.PointsClient, emb *embedder.Embedder, queryText string, topK int) ([]Result, error) {
	embedResp, err := emb.Embed(embedder.Request{ChunkID: "query", Text: queryText})
	if err != nil {
		return nil, fmt.Errorf("embed query: %w", err)
	}

	resp, err := pointsClient.Search(ctx, &qdrant.SearchPoints{
		CollectionName: QdrantCollection,
		Vector:         embedResp.Vector,
		Limit:          uint64(topK),
		WithPayload:    &qdrant.WithPayloadSelector{SelectorOptions: &qdrant.WithPayloadSelector_Enable{Enable: true}},
	})
	if err != nil {
		return nil, fmt.Errorf("qdrant search: %w", err)
	}

	results := make([]Result, len(resp.Result))
	for i, point := range resp.Result {
		results[i] = Result{
			DocID: point.Payload["doc_id"].GetStringValue(),
			Title: point.Payload["title"].GetStringValue(),
			Text:  point.Payload["text"].GetStringValue(),
			Score: point.Score,
		}
	}
	return results, nil
}
