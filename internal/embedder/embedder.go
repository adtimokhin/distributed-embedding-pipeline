// Package embedder manages a long-lived embedding subprocess, shared by any
// process that needs to embed text (worker, query-serving code) so each
// reuses one process across many Embed calls instead of paying model-load
// cost per call.
package embedder

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"
	"sync"
)

// Request/Response mirror the embedder subprocess's line-delimited JSON
// protocol (see tools/embedder/embedder.py and tools/mock_embedder).
type Request struct {
	ChunkID string `json:"chunk_id"`
	Text    string `json:"text"`
}

type Response struct {
	ChunkID string    `json:"chunk_id"`
	Vector  []float32 `json:"vector"`
}

// Embedder drives one embedding subprocess for its entire lifetime — spawned
// once via Start, reused for every subsequent Embed call, torn down by Close.
// Safe for concurrent use: Embed serializes access to the subprocess's
// stdin/stdout so concurrent callers (e.g. an HTTP server handling several
// requests at once) can't interleave requests and responses.
type Embedder struct {
	mu     sync.Mutex
	cmd    *exec.Cmd
	stdin  *bufio.Writer
	stdout *bufio.Scanner
}

// Start spawns the embedding subprocess described by cmdStr. cmdStr may be a
// space-separated command + arguments (e.g. "python3 embedder.py").
func Start(cmdStr string) (*Embedder, error) {
	parts := strings.Fields(cmdStr)
	if len(parts) == 0 {
		return nil, fmt.Errorf("empty embedder command")
	}
	cmd := exec.Command(parts[0], parts[1:]...)

	stdinPipe, err := cmd.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("stdin pipe: %w", err)
	}
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("stdout pipe: %w", err)
	}
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("start subprocess: %w", err)
	}
	return &Embedder{
		cmd:    cmd,
		stdin:  bufio.NewWriter(stdinPipe),
		stdout: bufio.NewScanner(stdoutPipe),
	}, nil
}

// Embed sends one request line and reads one response line.
func (e *Embedder) Embed(req Request) (Response, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	line, err := json.Marshal(req)
	if err != nil {
		return Response{}, fmt.Errorf("marshal request: %w", err)
	}
	if _, err := fmt.Fprintf(e.stdin, "%s\n", line); err != nil {
		return Response{}, fmt.Errorf("write to subprocess: %w", err)
	}
	if err := e.stdin.Flush(); err != nil {
		return Response{}, fmt.Errorf("flush subprocess stdin: %w", err)
	}
	if !e.stdout.Scan() {
		if err := e.stdout.Err(); err != nil {
			return Response{}, fmt.Errorf("read from subprocess: %w", err)
		}
		return Response{}, fmt.Errorf("subprocess closed stdout unexpectedly")
	}
	var resp Response
	if err := json.Unmarshal(e.stdout.Bytes(), &resp); err != nil {
		return Response{}, fmt.Errorf("decode subprocess response: %w", err)
	}
	return resp, nil
}

// Close terminates the embedder subprocess.
func (e *Embedder) Close() error {
	return e.cmd.Process.Kill()
}
