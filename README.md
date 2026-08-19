# Distributed Document Embedding Pipeline

A distributed pipeline that chunks Wikipedia articles, fans the chunks out to a pool of workers for vector embedding, and stores the results in Qdrant for kNN search. Stages 1–3 (core pipeline, at-least-once delivery, containerized deployment) are complete. The broker is a Raft-replicated 3-node cluster (extension, reusing HW3's `raft` package) rather than a single in-memory process — see "Raft-replicated broker" below.

## Components

| Component | Role |
|-----------|------|
| **Broker** (`broker/`) | 3-node Raft-replicated task queue: `Submit`, `Poll`, `Complete`, `GetResult`, `Heartbeat` on the client-facing service; `RequestVote`/`AppendEntries`/`InstallSnapshot` on the peer-facing Raft service. |
| **Worker** (`worker/`) | Polls the broker (via `internal/brokerclient`, which follows leader redirects), drives a long-lived embedder subprocess, writes vectors directly to Qdrant. |
| **Producer** (`producer/`) | Reads the corpus, chunks articles to ≤200 words, submits tasks, polls for completion. |
| **Query CLI** (`query/`) | Embeds a query string, runs kNN search against Qdrant, prints top-k results. |
| **Query server** (`queryserver/`) | HTTP API — `POST /search {"query", "top_k"}` for retrieval, `POST /ingest {"doc_id", "title", "text"}` for synchronous single-document indexing — for calling into the index from an agent/RAG system instead of a CLI or the batch pipeline. Same embed+search logic as the CLI (`internal/retrieval`) and same chunk/embed/upsert logic as the worker (`internal/indexer`), served from a long-lived process instead of a one-shot invocation. |

## Testing

```bash
go test ./... -race
```

64 test functions across 6 packages (several table-driven with multiple
subtests) — no Docker/Qdrant/broker processes required, no network sockets
(the broker cluster tests run over `bufconn`, in-memory gRPC transport).
Covers: the broker's replicated-state logic in isolation, a full 3-node
cluster including leader-crash and quorum-loss recovery, `internal/brokerclient`'s
redirect/retry logic, the embedder subprocess including a concurrency
regression test, chunking/upsert/ingestion logic and edge cases, retrieval,
and the HTTP handlers.

Known remaining gaps: partition-and-heal / repeated-failure cluster tests,
`worker`/`producer`'s own orchestration code (the logic they call is
tested, the loop wiring isn't), real-Qdrant-wire-protocol tests (everything
here uses a fake `qdrant.PointsClient`), and no CI workflow.

## Quick start

```bash
go build ./...
go build -o /tmp/mock_embedder ./tools/mock_embedder   # deterministic embedder, no Python needed
docker run -d -p 6333:6333 -p 6334:6334 qdrant/qdrant:latest
```

6 terminals, from `hw4/` (3 broker replicas + worker + producer + query):
```bash
go run ./broker --id=0 --config=broker_nodeconfig.json
go run ./broker --id=1 --config=broker_nodeconfig.json
go run ./broker --id=2 --config=broker_nodeconfig.json
go run ./worker --broker=localhost:9000,localhost:9001,localhost:9002 --qdrant=localhost:6334 --embedder=/tmp/mock_embedder
go run ./producer --corpus=corpus/wiki.jsonl.gz --broker=localhost:9000,localhost:9001,localhost:9002
go run ./query "Byzantine fault tolerance" --qdrant=localhost:6334 --embedder="python3 tools/embedder/embedder.py" --top=5
```

`--broker` on the worker/producer takes any subset of the cluster's client addresses, in any order — it doesn't need to be the current leader. `internal/brokerclient` finds the leader by trying each address and following `redirect_addr` responses.

### Retrieval API

```bash
go run ./queryserver --addr=:8080 --qdrant=localhost:6334 --embedder=/tmp/mock_embedder

curl -X POST localhost:8080/search -d '{"query": "Byzantine fault tolerance", "top_k": 5}'
# [{"doc_id": "...", "title": "...", "text": "...", "score": 0.83}, ...]

curl localhost:8080/healthz
```

The embedder subprocess and Qdrant connection are established once at startup and held for the server's lifetime — unlike the CLI, which is a one-shot process, this is what actually makes "don't reload the model per query" observable. `Embed` is mutex-guarded so concurrent requests can't interleave on the subprocess's stdin/stdout.

`/ingest` adds one document without touching the broker at all — chunk → embed → upsert inline, synchronously:

```bash
curl -X POST localhost:8080/ingest -d '{
  "doc_id": "agent-doc-1",
  "title": "Notes on Paxos",
  "text": "Paxos is a family of protocols for solving consensus...",
  "chunk_size": 200
}'
# {"doc_id": "agent-doc-1", "chunk_ids": ["agent-doc-1-0", "agent-doc-1-1", ...]}
```

`chunk_size` is optional (defaults to 200 words, matching the producer). Re-ingesting the same `doc_id` overwrites its chunks rather than duplicating them (same deterministic point-ID scheme the batch path relies on for idempotent retries) — but if the new text produces fewer chunks than the old version, stale trailing chunks aren't cleaned up; there's no delete path yet. `/ingest` and the batch broker/worker/producer pipeline write into the same Qdrant collection and coexist without interference — verified by ingesting through both paths and confirming both show up in `/search`.

### Docker Compose (Stage 3)

```bash
docker compose up --build --scale worker=1 -d
go run ./producer --corpus=corpus/wiki.jsonl.gz --broker=localhost:9000,localhost:9001,localhost:9002
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
- **The broker is a 3-node Raft-replicated cluster (extension), not a single in-memory process.** `Submit` and `Complete` are proposed as Raft log entries and block until committed to a majority — these are the two facts that must survive a crash ("this task exists," "this task is done"), since losing either drops work forever or leaves the producer polling `GetResult` on an orphaned ID forever. `Poll`'s task→worker assignment and `Heartbeat`'s liveness tracking are deliberately *not* replicated: they're leader-local, ephemeral scheduling state. If the leader crashes, in-flight assignments are simply forgotten and the new leader treats every submitted-but-not-done task as available again — safe for exactly the same reason re-execution is already safe within a single broker (see the at-least-once bullet above). This keeps Poll/Heartbeat cheap (no consensus round trip on a 100ms poll loop) and means "pending" is a derived view rather than stored state: scan submitted tasks in commit order, skip anything done or currently in-flight. `worker`/`producer` no longer dial a fixed broker address — `internal/brokerclient` takes a list of cluster addresses and follows `redirect_addr` responses (or, if a redirect names an address the caller can't itself resolve — e.g. a Docker-internal hostname reported to a host-invoked producer — falls back to round-robining its own known address list) to find the current leader.
- **The embedder subprocess is a shared, reusable component (`internal/embedder`), not duplicated per binary.** `worker/main.go` originally had its own private spawn-once-reuse-for-lifetime subprocess type; `query/main.go` originally spawned and killed a fresh subprocess *per query*, which is fine for an infrequent CLI call but pays model-load latency on every call — unusable for something an agent calls repeatedly. Both now share one `Embedder` type. Making it shared also surfaced a real bug before it shipped: the original type had no concurrency guard, which was harmless when only one goroutine ever called `embed` (the worker's sequential poll loop) but would silently interleave requests/responses once `queryserver` started calling it from multiple HTTP handler goroutines at once. `Embed` now holds a mutex for the full request/response round trip.
- **Retrieval is exposed as `queryserver`'s HTTP API, not just the `query` CLI.** Both share `internal/retrieval`'s embed-then-search logic; the CLI is a thin one-shot wrapper around it, `queryserver` holds the embedder and Qdrant connection open across requests. This is the actual point at which the index becomes something a RAG agent can call synchronously, rather than something you inspect by hand.
- **Single-document ingestion (`queryserver`'s `/ingest`) bypasses the broker entirely.** `chunkIDToUUID`/`upsertVector`/`chunkText` originally lived as private, near-duplicated code inside `worker/main.go` and `producer/main.go`; both now share one `internal/indexer` package, which also gained `IngestDocument` (chunk → embed → upsert, inline, for one document) and `EnsureCollection` (idempotent create-if-missing, needed because `queryserver` might now be the *first* process to ever touch Qdrant, not just the worker). The broker's `Submit`/`Poll`/`Complete` machinery — durable, fault-tolerant, built for bulk batches of thousands of chunks — is the wrong tool for "an agent just produced one document, index it now": standing up that whole path for a single write adds latency and a dependency on broker availability that a synchronous local chunk/embed/upsert doesn't need. The two paths write into the same collection using the same deterministic chunk-ID scheme, so they coexist safely and a document ingested via `/ingest` is immediately visible to `/search` and to anything the batch pipeline indexes later.

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
├── broker/main.go              ← implementation (Raft-replicated state machine + gRPC wiring)
├── worker/main.go              ← implementation
├── producer/main.go            ← implementation
├── query/main.go               ← implementation — one-shot CLI, thin wrapper around internal/retrieval
├── queryserver/main.go         ← implementation — HTTP retrieval API, long-lived, same internal/retrieval logic
├── internal/brokerclient/      ← implementation — shared leader-discovery/redirect client (worker + producer)
├── internal/embedder/          ← implementation — shared long-lived embedder subprocess (worker, query, queryserver)
├── internal/retrieval/         ← implementation — shared embed+kNN-search logic (query, queryserver)
├── internal/indexer/           ← implementation — shared chunk/embed/upsert logic (worker, producer, queryserver's /ingest)
├── internal/testutil/          ← test-only — builds tools/mock_embedder for packages that need a real subprocess
├── */..._test.go               ← tests — broker/test_helpers_test.go ports a bufconn-based 3-node cluster test harness
├── raft/, config/, internal/log/ ← Raft consensus implementation, cluster config, and its append-only log
├── broker_nodeconfig.json      ← implementation — local 3-node cluster addresses
├── broker_nodeconfig-docker.json ← implementation — Docker Compose cluster addresses
├── Dockerfile.broker           ← implementation (Stage 3)
├── Dockerfile.worker           ← implementation (Stage 3)
├── docker-compose.yml          ← implementation (Stage 3) — broker0/broker1/broker2 + qdrant + worker
└── proto/, corpus/, tools/     ← provided, unmodified (proto/raft.proto copied from HW3 alongside the provided broker.proto)
```
