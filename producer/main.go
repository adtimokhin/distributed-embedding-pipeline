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

	"pipeline/internal/brokerclient"
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

func run(corpusPath string, brokerAddrs []string, chunkSize int) error {
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

	// ── Broker client (Raft cluster: follows redirects, retries across nodes) ─
	client := brokerclient.New(brokerAddrs)
	ctx := context.Background()

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
			taskID, err := client.Submit(ctx, string(payloadBytes))
			if err != nil {
				return fmt.Errorf("submit task: %w", err)
			}
			taskIDs = append(taskIDs, taskID)
			chunkCount++
		}
	}

	log.Printf("submitted %d chunks from %d articles — waiting for completion...", chunkCount, articleCount)

	// ── Wait for all tasks ───────────────────────────────────────────────────
	pending := make(map[string]bool)
	for _, id := range taskIDs {
		pending[id] = true
	}

	for len(pending) > 0 {
		for id := range pending {
			done, errMsg, err := client.GetResult(ctx, id)
			if err != nil {
				log.Printf("GetResult %s: %v", id, err)
				continue
			}
			if done {
				if errMsg != "" {
					log.Printf("task %s failed: %s", id, errMsg)
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
	brokerAddrs := flag.String("broker", "localhost:9000,localhost:9001,localhost:9002", "comma-separated broker gRPC addresses (client-facing)")
	chunkSize := flag.Int("chunk-size", 200, "max words per chunk")
	flag.Parse()

	if err := run(*corpusPath, strings.Split(*brokerAddrs, ","), *chunkSize); err != nil {
		log.Fatal(err)
	}
}
