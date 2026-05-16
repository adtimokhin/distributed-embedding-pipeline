// Producer — HW4 embedding pipeline corpus producer.
//
// Reads wiki.jsonl.gz, chunks each article into ≤chunkSize-word passages,
// submits one task per chunk to the broker, then polls until all tasks complete.

package main

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
	"unicode"

	pb "pipeline/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// ──────────────────────────────────────────────────────────────────────────────
// Corpus types
// ──────────────────────────────────────────────────────────────────────────────

type article struct {
	ID    string `json:"id"`
	Title string `json:"title"`
	Text  string `json:"text"`
}

// taskPayload is what we encode in the broker task payload.
type taskPayload struct {
	DocID   string `json:"doc_id"`
	ChunkID string `json:"chunk_id"`
	Title   string `json:"title"`
	Text    string `json:"text"`
}

// ──────────────────────────────────────────────────────────────────────────────
// Chunking
// ──────────────────────────────────────────────────────────────────────────────

// chunkText splits text into passages of at most maxWords words.
// Word boundaries are Unicode whitespace.
func chunkText(text string, maxWords int) []string {
	words := strings.FieldsFunc(text, unicode.IsSpace)
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

// ──────────────────────────────────────────────────────────────────────────────
// Main
// ──────────────────────────────────────────────────────────────────────────────

func run(corpusPath, brokerAddr string, chunkSize int) error {
	// ── Open corpus ──────────────────────────────────────────────────────────
	f, err := os.Open(corpusPath)
	if err != nil {
		return fmt.Errorf("open corpus: %w", err)
	}
	defer f.Close()

	gr, err := gzip.NewReader(f)
	if err != nil {
		return fmt.Errorf("gzip reader: %w", err)
	}
	defer gr.Close()

	// ── Connect to broker ────────────────────────────────────────────────────
	conn, err := grpc.NewClient(brokerAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial broker: %w", err)
	}
	defer conn.Close()
	client := pb.NewBrokerClient(conn)
	ctx := context.Background()
	_ = client // used in TODO block below
	_ = ctx    // used in TODO block below

	// ── Submit tasks ─────────────────────────────────────────────────────────
	var taskIDs []string
	var articleCount, chunkCount int

	dec := json.NewDecoder(gr)
	for {
		var art article
		if err := dec.Decode(&art); err != nil {
			break // EOF or malformed line — stop
		}
		articleCount++

		for i, passage := range chunkText(art.Text, chunkSize) {
			p := taskPayload{
				DocID:   art.ID,
				ChunkID: art.ID + "-" + strconv.Itoa(i),
				Title:   art.Title,
				Text:    passage,
			}
			payloadBytes, err := json.Marshal(p)
			if err != nil {
				return fmt.Errorf("marshal payload: %w", err)
			}
			resp, err := client.Submit(ctx, &pb.SubmitRequest{Payload: string(payloadBytes)})
			if err != nil {
				return fmt.Errorf("submit task: %w", err)
			}
			taskIDs = append(taskIDs, resp.TaskId)
			chunkCount++
		}
	}

	log.Printf("submitted %d chunks from %d articles — waiting for completion...", chunkCount, articleCount)

	// ── Wait for all tasks ───────────────────────────────────────────────────
	// TODO (Stage 1): poll client.GetResult for each id in taskIDs until all
	// return done=true. A simple approach: loop until the pending set is empty,
	// sleeping 200ms between sweeps.
	pending := make(map[string]bool)
	for _, id := range taskIDs {
		pending[id] = true
	}

	for len(pending) > 0 {
		for id := range pending {
			resp, err := client.GetResult(ctx, &pb.GetResultRequest{TaskId: id})
			if err != nil {
				log.Printf("GetResult %s: %v", id, err)
				continue
			}
			if resp.Done {
				if resp.Error != "" {
					log.Printf("task %s failed: %s", id, resp.Error)
				}
				delete(pending, id)
			}
		}
		if len(pending) > 0 {
			time.Sleep(100 * time.Millisecond)
		}
	}

	fmt.Printf("indexed %d chunks from %d articles\n", chunkCount, articleCount)
	return nil
}

func main() {
	corpusPath := flag.String("corpus", "corpus/wiki.jsonl.gz", "path to gzipped JSONL corpus")
	brokerAddr := flag.String("broker", "localhost:9000", "broker gRPC address")
	chunkSize := flag.Int("chunk-size", 200, "max words per chunk")
	flag.Parse()

	if err := run(*corpusPath, *brokerAddr, *chunkSize); err != nil {
		log.Fatal(err)
	}
}
