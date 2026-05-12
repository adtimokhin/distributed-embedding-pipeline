# HW4 — Document Embedding Pipeline

## Overview

You will build a distributed document embedding pipeline with four components:

| Component | What it does |
|-----------|-------------|
| **Broker** | gRPC server — accepts tasks, queues them, tracks completion |
| **Worker** | Polls broker, drives embedding subprocess, writes to Qdrant |
| **Producer** | Reads corpus, chunks articles, submits tasks, waits for completion |
| **Query CLI** | Embeds a query, runs kNN search in Qdrant, prints top results |

---

## Prerequisites

### Go 1.24+
```bash
go version   # should print go1.24 or later
```

### Python 3.11+ with sentence-transformers (for real embeddings)
```bash
pip install -r tools/embedder/requirements.txt
```

### Qdrant (for local development)
```bash
# via Docker (recommended)
docker run -d -p 6333:6333 -p 6334:6334 qdrant/qdrant:latest
```

---

## Quick Start

### Stage 1 — Local Development

Open four terminals from the `HW4/` directory.

**Terminal 1 — Broker:**
```bash
go run ./broker --port=9000
```

**Terminal 2 — Worker (mock embedder, no Python needed):**
```bash
# Build the mock embedder first
go build -o /tmp/mock_embedder ./tools/mock_embedder

go run ./worker \
    --broker=localhost:9000 \
    --qdrant=localhost:6334 \
    --embedder=/tmp/mock_embedder
```

**Terminal 3 — Producer:**
```bash
go run ./producer \
    --corpus=corpus/wiki.jsonl.gz \
    --broker=localhost:9000
```

**Terminal 4 — Query:**
```bash
go run ./query "Byzantine fault tolerance" \
    --qdrant=localhost:6334 \
    --embedder="python3 tools/embedder/embedder.py" \
    --top=5
```

### Stage 3 — Docker Compose

```bash
# Build and start full stack (1 worker)
docker compose up --build --scale worker=1 -d

# Index corpus from host (broker port exposed on 9000)
go run ./producer --corpus=corpus/wiki.jsonl.gz --broker=localhost:9000

# Query
go run ./query "distributed consensus" \
    --qdrant=localhost:6334 \
    --embedder="python3 tools/embedder/embedder.py" \
    --top=5

# Scale to 4 workers
docker compose up --scale worker=4 -d

# Tear down
docker compose down
```

---

## Corpus

`corpus/wiki.jsonl.gz` contains ~1,000 Wikipedia articles in gzipped JSONL format:

```json
{"id": "42", "title": "Byzantine fault", "text": "A Byzantine fault is a condition..."}
```

A 1,000-article corpus produces roughly 5,000–8,000 chunks of ≤200 words each.

---

## Embedding Subprocess Protocol

Both `tools/embedder/embedder.py` (real, 384-dim) and `tools/mock_embedder/main.go`
(deterministic, for testing) speak the same line-delimited JSON protocol:

**stdin → subprocess (one line per request):**
```json
{"chunk_id": "doc42-3", "text": "Byzantine fault tolerance requires..."}
```

**subprocess → stdout (one line per response):**
```json
{"chunk_id": "doc42-3", "vector": [0.021, -0.143, 0.087, ...]}
```

One subprocess is started at worker startup and reused for the worker's lifetime.

---

## Qdrant Ports

| Port | Protocol | Use |
|------|----------|-----|
| 6333 | HTTP/REST | Dashboard, REST API |
| 6334 | gRPC     | Go client (`--qdrant` flag) |

The worker's `--qdrant` flag takes the **gRPC** address: `localhost:6334`.

The Qdrant collection is created automatically on first use; the Go client handles
collection creation if it does not exist. Collection name: `documents`, vector
dimension: 384, distance: Cosine.

---

## File Structure

```
HW4/
├── broker/main.go          ← YOU IMPLEMENT
├── worker/main.go          ← YOU IMPLEMENT
├── producer/main.go        ← YOU IMPLEMENT
├── query/main.go           ← YOU IMPLEMENT
├── Dockerfile.worker       ← YOU COMPLETE (Stage 3)
├── docker-compose.yml      ← YOU COMPLETE (Stage 3)
├── proto/                  ← DO NOT MODIFY
├── corpus/                 ← DO NOT MODIFY
├── tools/                  ← DO NOT MODIFY
├── go.mod / go.sum         ← DO NOT MODIFY
└── REFLECTIONS.md          ← YOU FILL IN
```

---

## Submission

Submit your entire `HW4/` directory on Gradescope. Required files:

- `broker/main.go`
- `worker/main.go`
- `producer/main.go`
- `query/main.go`
- `Dockerfile.worker`
- `docker-compose.yml`
- `REFLECTIONS.md`

Do **not** submit `corpus/wiki.jsonl.gz` (it is large and provided by the autograder).
