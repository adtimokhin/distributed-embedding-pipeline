# TO_IMPLEMENT — minimum path to a usable RAG backend

This is a scoping outline, not a design doc for homework submission. It lists
the minimum additions needed to make this pipeline callable by an agentic RAG
system, beyond what's already built (chunk → embed → upsert pipeline, 3-node
Raft-replicated broker, idempotent writes). Nothing here is implemented yet.

The Raft-replicated broker stays as-is — it remains the path for bulk/batch
(re)indexing. These items are additive, not a rework of it.

---

## 1. Finish `query/main.go`'s kNN search

**Status:** stub. `run()` in `query/main.go:87-117` embeds the query
correctly but the actual `Search` call is a `TODO` (`query/main.go:102-116`)
and always returns `"query search: not implemented"`.

**What's needed:** call `pointsClient.Search` with the embedded query vector,
`CollectionName: QdrantCollection`, `Limit: uint64(topK)`, and
`WithPayload` enabled, then return the ranked results (payload already
carries `doc_id`/`title`/`text` per point, written by `upsertVector` in
`worker/main.go:152-169`).

**Why it's minimum:** nothing downstream can retrieve anything until this
exists — it's the only missing piece of the actual search path.

**Scope:** small. The function signature, Qdrant client, and query embedding
are already wired up in `query/main.go`; this is one `Search` call plus
mapping the response.

---

## 2. Persistent query-time embedding (stop spawning a subprocess per query)

**Status:** `embedQuery` in `query/main.go:39-81` starts a fresh embedder
subprocess, sends one request, reads one response, and kills the subprocess
— every call. The doc comment even says why: "queries are infrequent." That
assumption breaks the moment an agent is calling this mid-conversation;
model load time (seconds) becomes per-query latency.

**What's needed:** a long-lived embedder subprocess that outlives a single
query, following the exact pattern `worker/main.go` already uses —
`startEmbedder`/`embedder.embed` (`worker/main.go:65-121`) spawn once and
reuse the process for the worker's lifetime. Query serving needs the same
shape: start the subprocess once when the serving process starts, keep it
alive, and send each incoming query down the same stdin/stdout pipe.

**Why it's minimum:** without this, #3 below is technically callable but
unusably slow for an agent loop.

**Scope:** small — it's copying an existing pattern (`worker/main.go`'s
embedder type) into whatever process ends up serving queries, not designing
something new.

---

## 3. Expose retrieval as a callable API

**Status:** retrieval only exists as `go run ./query "..."`, printing to
stdout. No agent can call a CLI subprocess mid-reasoning in any sane way.

**What's needed:** wrap embed (#2) → `Search` (#1) → format-results behind
an HTTP handler or MCP tool. Minimum shape: one endpoint/tool taking
`{query, top_k}` and returning structured JSON — `[{doc_id, title, text,
score}, ...]` — reusing the exact payload fields already stored per point
(`worker/main.go:160-164`) so no new metadata needs to be invented yet.

**Why it's minimum:** this is the actual definition of "usable in a RAG
system" — a stable, structured, synchronously-callable contract. Everything
else in this doc exists to make this endpoint correct (#1) and fast (#2).

**Scope:** small-to-medium. Mostly plumbing: stand up one HTTP server or MCP
tool process, call into the logic from #1/#2.

---

## 4. Synchronous single-document ingestion path (bypasses the broker)

**Status:** the only way to add a document today is the full batch pipeline
— `producer` reads a corpus file, submits N tasks to the broker cluster,
`worker` polls/embeds/upserts each one. Standing up that whole flow (or even
just a `Submit` call through the Raft-replicated broker) to add one document
an agent just produced is the wrong tool for the job — it's built for
bulk load, not single-item writes on the hot path.

**What's needed:** a small function, callable from the same process as #3,
that does chunk → embed → upsert **inline**, synchronously, for one document
— reusing `upsertVector`/`chunkIDToUUID` from `worker/main.go:127-169`
directly rather than routing through `Submit`/`Poll`/`Complete`. The broker
keeps doing what it does well (durable, fault-tolerant bulk (re)indexing);
this path handles "agent just produced a document, index it now."

**Why it's minimum:** without this, growing the index means re-running the
batch pipeline, which is not something an agent can reasonably do as part of
normal operation.

**Scope:** small. It's mostly `worker.go`'s embed+upsert logic called
directly instead of triggered by a polled task.

---

## Explicitly out of scope here (deferred, not blocking)

Auth/API keys, multi-tenancy, hybrid (BM25 + vector) search, reranking,
observability (latency/cost metrics), and delete/update support are all real
gaps but don't block a single agent using this as a personal retrieval
backend. Revisit once something beyond one agent/user is hitting this
concurrently, or once bad/stale chunks in the index become a practical
problem.
