// mock_embedder — deterministic embedding subprocess for autograding.
//
// Speaks the same line-delimited JSON protocol as embedder.py:
//   stdin:  {"chunk_id": "...", "text": "..."}
//   stdout: {"chunk_id": "...", "vector": [...]}
//
// Vectors are produced by FNV-32a hash of the text, projected to 384 dims.
// Output is deterministic for any given input text (no randomness, no model).

package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"hash/fnv"
	"math"
	"os"
	"time"
)

const vectorDim = 384

type request struct {
	ChunkID string `json:"chunk_id"`
	Text    string `json:"text"`
}

type response struct {
	ChunkID string    `json:"chunk_id"`
	Vector  []float32 `json:"vector"`
}

// hashProject maps text to a unit vector in R^vectorDim via FNV hashing.
// Each dimension is determined by hashing text + dimension index.
func hashProject(text string) []float32 {
	vec := make([]float32, vectorDim)
	var norm float64

	for i := 0; i < vectorDim; i++ {
		h := fnv.New32a()
		fmt.Fprintf(h, "%s:%d", text, i) //nolint:errcheck
		raw := float64(int32(h.Sum32()))  // signed to get negative values
		vec[i] = float32(raw)
		norm += raw * raw
	}

	// L2 normalise
	if norm > 0 {
		scale := float32(1.0 / math.Sqrt(norm))
		for i := range vec {
			vec[i] *= scale
		}
	}
	return vec
}

func main() {
	delayMs := flag.Int("delay", 0, "artificial delay per request in milliseconds (for testing)")
	flag.Parse()

	delay := time.Duration(*delayMs) * time.Millisecond

	scanner := bufio.NewScanner(os.Stdin)
	writer := bufio.NewWriter(os.Stdout)

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		var req request
		if err := json.Unmarshal(line, &req); err != nil {
			fmt.Fprintf(os.Stderr, "[mock_embedder] bad JSON: %v\n", err)
			continue
		}

		if delay > 0 {
			time.Sleep(delay)
		}

		resp := response{
			ChunkID: req.ChunkID,
			Vector:  hashProject(req.Text),
		}

		out, _ := json.Marshal(resp)
		writer.Write(out) //nolint:errcheck
		writer.WriteByte('\n')
		writer.Flush() //nolint:errcheck
	}
}
