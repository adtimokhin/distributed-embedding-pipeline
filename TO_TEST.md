# TO_TEST — what's needed to actually test this software

Originally a scoping outline written when `hw4/` had **zero** `*_test.go`
files and every claim about the Raft-replicated broker was verified by hand.
Sections 1–3 and most of 5 are now implemented — 64 top-level test functions
(several table-driven with multiple subtests) across 6 packages, all passing
under `-race`. What's left is noted inline and in the final section.

```
go test ./... -race
```

**Two real bugs surfaced by writing edge-case tests, both fixed:**
- `internal/indexer.ChunkText` looped forever on `maxWords <= 0` (`i +=
  maxWords` never advances past `len(words)` when the step is zero, and
  panics on a negative slice index when the step is negative). Not reachable
  through any current caller — both callers already default to 200 before
  calling it — but the function itself had no guard. Now treats `maxWords <=
  0` as "don't split." Regression tests: `TestChunkText/zero_max_words_...`,
  `.../negative_max_words_...`, `TestIngestDocumentZeroChunkSizeDoesNotHang`.
- `internal/brokerclient.Client.call` indexed `c.addrs[c.current]`
  unconditionally, so `New(nil)` or `New([]string{})` panicked with "index
  out of range" on the first call instead of failing cleanly. Now returns an
  error. Regression test: `TestClientEmptyAddressListReturnsErrorNotPanic`.

**One small refactor in service of testability:** `broker/main.go`'s
`reEnqueueStalled` (the ticker-driven background goroutine) had its per-tick
body extracted into `checkStalledTasks()`, callable directly and
synchronously. Without this, testing Stage 2's original stale-heartbeat
re-enqueue mechanism would have meant either waiting `CheckInterval`+
`TaskTimeout` (15s) of real wall-clock time per test, or leaving it
untested — it had no coverage at all until this pass. Behavior-preserving:
existing tests passed unchanged before and after.

---

## 1. Unit tests for the broker's replicated-state logic — ✅ done

`broker/command_test.go`: `encodeSubmit`/`encodeComplete`/`decodeCommand`
round-trip (including the Raft `"noop"` marker decoding to a silently-ignored
empty op), `applyLoop`'s effect on `submitted`/`submittedOrder`/`done`/
`errors`/`inflight` given a synthetic `commitCh`, idempotent re-submission of
the same task ID, and `pendingOps` being woken with the committed term.

Not separately unit-tested: `refreshLeaderState`'s false→true reset logic
and `Poll`'s derived-pending-view scan. Both call `s.rf.GetState()` inline,
so isolating them from a real (or bufconn) Raft instance isn't possible
without adding a testing seam — they're covered instead by the cluster
integration tests in section 2, where `TestLeaderCrashDoesNotLoseSubmittedWork`
exercises the exact false→true transition on a real new leader.

---

## 2. Broker cluster integration tests — ✅ done

`broker/test_helpers_test.go` ports `hw3/server/test_helpers_test.go`'s
bufconn-based 3-node harness (`raft.NewPaused` + `SetPeerClient` + `Resume`,
a `proxyClient` for partitions) from the KVStore service to the Broker
service — `raft.go` was copied unmodified, so nothing about the harness
itself needed to change.

`broker/cluster_test.go`:
- `TestSubmitPollCompleteGetResultRoundTrip` — full RPC round trip on a
  healthy cluster, including that a second `Poll` doesn't hand out a task
  that's already in-flight.
- `TestClusterRedirectsNonLeaderCalls` — `Submit`/`Poll`/`Heartbeat`/
  `Complete` against a follower return `redirect_addr` pointing at the
  actual leader.
- `TestGetResultServedByAnyNode` / `TestGetResultUnknownTaskIDIsNotDone` —
  `GetResult` never redirects and reports `done=false` (not an error) for
  an ID that was never submitted.
- `TestLeaderCrashDoesNotLoseSubmittedWork` — the flagship test: submits a
  task, polls it (making it in-flight), kills the leader, confirms a new
  leader is elected, confirms previously-completed work survived, and
  confirms the in-flight-but-never-completed task becomes pollable again on
  the new leader. This is the automated version of the original manual
  "kill -9 the leader, watch the producer recover" check.
- `TestStaleHeartbeatTriggersReEnqueue` — Stage 2's *original*
  at-least-once mechanism (a stalled worker's task becomes available
  again), previously untested because it needed a real Raft cluster plus a
  way to trigger the scan without waiting on real time — both now solved by
  the harness plus the `checkStalledTasks()` extraction.
- `TestSubmitTimesOutWithoutQuorum` — a leader isolated from both followers
  (still locally believes it's leader; nothing tells it otherwise) can
  never reach quorum, so `Submit` must eventually give up (~5s
  `commitTimeout`) rather than block forever. The one genuinely slow test
  in the suite (~5s) — everything else finishes in well under a second.
- `TestDoubleCompleteIsIdempotent` — two workers finishing the same
  re-enqueued task and both calling `Complete` must not error or corrupt
  state.

Not covered: partition-and-heal (a node missing commits catching up via the
`nextIndex` backoff path) and repeated/cascading leader failures. Both are
straightforward extensions of the existing harness (`tc.isolate`/
`tc.reconnect` are already ported) — just not written yet.

---

## 3. `internal/brokerclient` tests — ✅ done

`internal/brokerclient/client_test.go`, against real loopback-TCP fake
`pb.BrokerServer` implementations (bufconn wasn't an option here — see the
file's doc comment for why): follows a redirect to a known address directly,
falls back to round-robin on a redirect to an address it doesn't recognize
(the Docker-vs-host mismatch case), advances past a dead address, returns an
error once the whole configured cluster is unreachable, confirms `GetResult`
never redirects, handles a single-address cluster, a cluster where every
node redirects to every other (no leader at all — e.g. mid-election), and
— the regression test for the panic found while writing these — an empty
address list.

---

## 4. Worker/producer pipeline tests — partially superseded, gap remains

The chunking and upsert logic this section originally called out
(`chunkText`, `chunkIDToUUID`, idempotent duplicate upserts) has since moved
into `internal/indexer` and is covered there (section 5). What's *not*
covered is `worker.go`'s and `producer.go`'s own remaining code — the poll
loop, heartbeat goroutine, and corpus-reading/submission loop — which is now
thin orchestration around already-tested packages rather than untested
business logic, but is still only verified by the manual end-to-end runs
done during development, not by an automated test.

---

## 5. Tests for the `TO_IMPLEMENT.md` additions — ✅ done

- **kNN search / retrieval:** `internal/retrieval/retrieval_test.go` — query
  embedding is sent to Qdrant, `ScoredPoint`s map to `Result` fields
  correctly, empty results return `[]` not `nil`, a point with missing
  payload keys doesn't panic, a Qdrant-layer error propagates rather than
  being swallowed, and `topK<=0` is passed through as-is (`Search` itself
  doesn't default/clamp — that's deliberately the HTTP layer's job, not
  duplicated here).
- **Persistent embedder:** `internal/embedder/embedder_test.go` — a single
  subprocess (same PID) serves multiple sequential `Embed` calls, identical
  text embeds identically, the actual regression test for the concurrency
  bug this work surfaced (20 goroutines × 20 calls, no interleaved/
  mismatched responses), plus failure paths: `Start` with a nonexistent or
  empty command errors instead of hanging, and `Embed` after the subprocess
  has been killed returns a clean error instead of hanging.
- **HTTP retrieval + ingestion API:** `queryserver/main_test.go` — `/search`
  and `/ingest` request validation, defaults (`top_k`, `chunk_size`),
  clamping, method checks (405 on GET), malformed-JSON/missing-field 400s,
  a table-driven sweep of `top_k`/`chunk_size` boundary values (omitted,
  explicit negative, at/over max), and a Qdrant-layer failure surfacing as
  500 rather than a panic — via `httptest` against the real handlers (fakes
  only at the Qdrant boundary).
- **Sync single-doc ingestion:** `internal/indexer/indexer_test.go` — a
  table-driven `ChunkText` sweep (empty, whitespace-only, single word, exact
  multiple, and the zero/negative regression cases above), `ChunkIDToUUID`
  determinism (including an empty chunk ID), `EnsureCollection`'s
  create-if-missing / skip-if-exists / Create-fails branches, `UpsertChunk`'s
  payload shape and Qdrant-error propagation, `IngestDocument`'s end-to-end
  chunk→embed→upsert count, the zero-chunk-size regression case, and a
  partial-failure case (second chunk's upsert fails — confirms
  `IngestDocument` returns the chunk IDs written *before* the failure, not
  an all-or-nothing nil, matching what its doc comment promises).

`internal/testutil/embedder.go` (not a `_test.go` file, so it's importable
across packages) builds `tools/mock_embedder` once per test binary for the
three packages (`embedder`, `indexer`, `retrieval`/`queryserver`) that need a
real subprocess because `IngestDocument`/`Search` take a concrete
`*embedder.Embedder`, not an interface.

---

## Remaining gaps

- **Partition-and-heal / repeated-failure cluster tests** (section 2) — the
  harness supports them, they're just not written.
- **Worker/producer orchestration itself** (section 4) — the logic it calls
  is tested; the loop/goroutine wiring isn't.
- **No Qdrant container tests.** Every test here uses a fake
  `qdrant.PointsClient`/`CollectionsClient` (interface embedding — only the
  methods actually called are overridden). That's deliberate: faster, no
  Docker dependency, and sufficient for testing this codebase's logic — but
  it never exercises the real Qdrant wire protocol, so a change that's
  correct against the fake but wrong against real Qdrant wouldn't be caught.
  `testcontainers-go` would close this gap if it's ever worth the added
  runtime and Docker dependency in tests.
- **No CI workflow.** Still no `.github/workflows/` anywhere in this repo —
  `go test ./... -race` is a command you run, not something that runs on
  every push.
