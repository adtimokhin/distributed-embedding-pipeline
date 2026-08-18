# TO_TEST — what's needed to actually test this software

Originally a scoping outline written when `hw4/` had **zero** `*_test.go`
files and every claim about the Raft-replicated broker was verified by hand.
Sections 1–3 and most of 5 are now implemented — 49 tests across 6 packages,
all passing under `-race`. What's left is noted inline and in the final
section.

```
go test ./... -race
```

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
- `TestGetResultServedByAnyNode` — `GetResult` never redirects; every node
  eventually agrees a completed task is done.
- `TestLeaderCrashDoesNotLoseSubmittedWork` — the flagship test: submits a
  task, polls it (making it in-flight), kills the leader, confirms a new
  leader is elected, confirms previously-completed work survived, and
  confirms the in-flight-but-never-completed task becomes pollable again on
  the new leader. This is the automated version of the original manual
  "kill -9 the leader, watch the producer recover" check.

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
error once the whole configured cluster is unreachable, and confirms
`GetResult` never redirects.

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
  correctly, empty results return `[]` not `nil`, and a point with missing
  payload keys doesn't panic.
- **Persistent embedder:** `internal/embedder/embedder_test.go` — a single
  subprocess (same PID) serves multiple sequential `Embed` calls, identical
  text embeds identically, and — the actual regression test for the
  concurrency bug this work surfaced — 20 goroutines × 20 calls each embed
  concurrently with no interleaved/mismatched responses.
- **HTTP retrieval + ingestion API:** `queryserver/main_test.go` — `/search`
  and `/ingest` request validation, defaults (`top_k`, `chunk_size`),
  clamping, method checks (405 on GET), and malformed-JSON/missing-field 400s,
  via `httptest` against the real handlers (fakes only at the Qdrant
  boundary).
- **Sync single-doc ingestion:** `internal/indexer/indexer_test.go` —
  `ChunkText` boundaries (including empty input and unicode whitespace),
  `ChunkIDToUUID` determinism, `EnsureCollection`'s create-if-missing /
  skip-if-exists branches, `UpsertChunk`'s payload shape, and
  `IngestDocument`'s end-to-end chunk→embed→upsert count.

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
