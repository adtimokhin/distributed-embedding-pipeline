# HW4 — Distributed Document Embedding Pipeline

A distributed pipeline that chunks Wikipedia articles, fans the chunks out to a pool of workers for vector embedding, and stores the results in Qdrant for kNN search. Stages 1–3 (core pipeline, at-least-once delivery, containerized deployment) are complete; the persistent-broker-WAL extension is not.

## Components

| Component | Role |
|-----------|------|
| **Broker** (`broker/`) | gRPC task queue: `Submit`, `Poll`, `Complete`, `GetResult`, `Heartbeat`. Tracks `pending` / `inflight` / `done` / `errors`. |
| **Worker** (`worker/`) | Polls the broker, drives a long-lived embedder subprocess, writes vectors directly to Qdrant. |
| **Producer** (`producer/`) | Reads the corpus, chunks articles to ≤200 words, submits tasks, polls for completion. |
| **Query CLI** (`query/`) | Embeds a query string, runs kNN search against Qdrant, prints top-k results. |

## Quick start

```bash
go build ./...
go build -o /tmp/mock_embedder ./tools/mock_embedder   # deterministic embedder, no Python needed
docker run -d -p 6333:6333 -p 6334:6334 qdrant/qdrant:latest
```

4 terminals, from `hw4/`:
```bash
go run ./broker --port=9000
go run ./worker --broker=localhost:9000 --qdrant=localhost:6334 --embedder=/tmp/mock_embedder
go run ./producer --corpus=corpus/wiki.jsonl.gz --broker=localhost:9000
go run ./query "Byzantine fault tolerance" --qdrant=localhost:6334 --embedder="python3 tools/embedder/embedder.py" --top=5
```

### Docker Compose (Stage 3)

```bash
docker compose up --build --scale worker=1 -d
go run ./producer --corpus=corpus/wiki.jsonl.gz --broker=localhost:9000
go run ./query "distributed consensus" --qdrant=localhost:6334 --embedder="python3 tools/embedder/embedder.py" --top=5
docker compose up --scale worker=4 -d   # scale workers
docker compose down
```

## Embedder subprocess protocol

One worker-lifetime subprocess speaks line-delimited JSON on stdin/stdout:
```json
{"chunk_id": "doc42-3", "text": "..."}              // request
{"chunk_id": "doc42-3", "vector": [0.02, ...]}       // response (384-dim)
```
`tools/embedder/embedder.py` (real, sentence-transformers) and `tools/mock_embedder` (deterministic, for testing) both implement it. Qdrant collection `documents` (384-dim, Cosine) is created automatically on first use; gRPC port is 6334, the one the `--qdrant` flag expects (6333 is the HTTP/dashboard port).

## Design decisions actually implemented

- **At-least-once delivery via re-execution, not exactly-once.** The broker's `reEnqueueStalled` goroutine moves a task back to `pending` if its worker misses heartbeats for `TaskTimeout = 10s`. This can embed the same chunk twice (e.g. a worker stalls in a GC pause, gets re-enqueued, then wakes up and finishes anyway). It's safe specifically because embedding is a pure function and `chunkIDToUUID` is a deterministic hash — duplicate upserts to Qdrant write the identical point ID and vector, so the collection ends up in the same state whether a chunk was embedded once or twice. This would *not* be safe for a KV store `Put`, which is why HW3 needs a replicated log instead of re-execution.
- **One embedder subprocess per worker, reused for its lifetime**, not spawned per task. Loading `all-MiniLM-L6-v2` costs several seconds; paying that once amortizes across the whole corpus instead of per chunk. The cost is reduced fault isolation — a leaked or corrupted subprocess takes the whole worker down, surfaced as `embed` returning `"subprocess closed stdout unexpectedly"`, after which the worker reports the error via `Complete` and exits (or, if it dies too hard to even call `Complete`, the broker's stall detector recovers the task instead).
- **Workers write to Qdrant directly; the broker never sees the vectors.** This means broker state (`done`) and Qdrant state can diverge after a crash between the upsert and the `Complete` call — not a correctness problem here only because the upsert is idempotent, but it does mean the broker's bookkeeping isn't a source of truth for what's actually indexed.
- **`*pb.Task` everywhere, not `pb.Task`.** Protobuf-generated structs embed `protoimpl.MessageState`, which contains a `sync.Mutex` — storing them by value triggers `go vet`'s "assignment copies lock value." Both the broker's internal maps and `inFlightEntry` had to switch to pointers.
- **`cancelHB()` is called before `Complete`, not after**, in the worker. Calling it after leaves a window where a heartbeat can fire for a task the broker has already marked done — harmless here (the lookup is a no-op) but the ordering removes the race entirely rather than relying on it being benign.
- **The broker is purely in-memory and not crash-tolerant.** A broker restart loses every queued/inflight task with no recovery path; the producer would poll orphaned task IDs forever. Fixing this is the unimplemented extension — a WAL that durably logs `Submit`/`Poll`/`Complete` before acking, replayed on startup.

## Scaling result (Stage 3, 1 vs. 4 workers)

| Workers | Time to index corpus | Notes |
|---------|----------------------|-------|
| 1 | 8.8s | real sentence-transformers embedder, ~490 MiB RSS |
| 4 | 10.1s | ~1.96 GB combined RSS — *slower* than 1 worker |

Scaling was negative, not just sub-linear: 4 containers on one host compete for CPU during model inference and for memory, which dominates any gain from parallelism. The broker and Qdrant were ruled out as bottlenecks — the broker's critical sections are map/slice operations with no measurable contention at this scale. Real linear scaling would need separate physical machines, not co-located containers.

## Corpus

`corpus/wiki.jsonl.gz` — ~1,000 gzipped JSONL Wikipedia articles, producing ~5,000–8,000 chunks of ≤200 words. **Do not submit this file to Gradescope** — the autograder provides its own copy.

## File structure

```
hw4/
├── broker/main.go          ← implementation
├── worker/main.go          ← implementation
├── producer/main.go        ← implementation
├── query/main.go           ← implementation
├── Dockerfile.worker       ← implementation (Stage 3)
├── docker-compose.yml      ← implementation (Stage 3)
├── proto/, corpus/, tools/ ← provided, unmodified
└── REFLECTIONS.md
```

See [REFLECTIONS.md](REFLECTIONS.md) for the full delivery-semantics trace, the comparison with HW3's Raft-based approach to fault tolerance, and the throughput-scaling analysis.
