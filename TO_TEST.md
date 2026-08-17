# TO_TEST — what's needed to actually test this software

Scoping outline, companion to `TO_IMPLEMENT.md`. `hw4/` currently has **zero**
`*_test.go` files — every claim about the Raft-replicated broker (leader
election, failover, redirect-following) was verified by hand: running 3
broker processes locally, `kill -9`-ing the leader mid-run, and reading log
output / Qdrant point counts. That's fine for a one-time check, not for
catching a regression the next time this changes. This lists the test
infrastructure and suites needed to make that verification repeatable, plus
what to add once `TO_IMPLEMENT.md`'s items land. Nothing here is written yet.

---

## 1. Unit tests for the broker's replicated-state logic

**What's needed:** tests for the pure, network-free pieces of
`broker/main.go`:
- `encodeSubmit`/`encodeComplete`/`decodeCommand` (`broker/main.go:118-143`)
  round-trip correctly, including the case that currently relies on silent
  fallthrough: `decodeCommand` on Raft's own `"noop"` leader-election marker
  must decode to `op=""` and be ignored, not misparsed.
- `applyLoop` (`broker/main.go:145-172`) applied against a fed `commitCh`:
  `submit` populates `submitted`/`submittedOrder` and is a no-op on a
  duplicate id; `complete` sets `done`/`errors` and clears any matching
  `inflight` entry; a `pendingOps` waiter is woken with the committed term.
- `Poll`'s derived-pending-view scan (`broker/main.go:213-236`) — skips
  anything in `done` or `inflight`, returns tasks in `submittedOrder`.
- `refreshLeaderState` (`broker/main.go:302-315`) — the inflight-reset-on
  false→true transition. This one only exists because of a real bug class
  (stale inflight entries from an earlier leadership stint silently
  blocking a task forever) and currently has no coverage at all beyond the
  log line I read manually during the original smoke test.

**Why it's needed:** these are the parts of the design doc
(`README.md`'s "Design decisions actually implemented") that are the actual
novel logic of this extension — everything else is wiring. They're also the
cheapest tests to write: no network, no goroutines, just state transitions.

**Scope:** small. `brokerServer` fields are unexported but the test file
lives in `package main` alongside it, same as any Go internal test.

---

## 2. Broker cluster integration tests (the part that's only been checked by hand)

**What's needed:** automate the exact scenario I ran manually — a 3-node
cluster, `Submit`/`Poll`/`Complete`/`GetResult` round-tripping correctly,
and a leader crash mid-flight producing a new leader without losing
submitted-but-incomplete work.

**Don't build this from scratch** — `hw3/server/test_helpers_test.go` already
has exactly the harness this needs: `newTestCluster` wires a 3-node Raft
cluster over `bufconn` (in-memory transport, no real sockets) using
`raft.NewPaused` + `SetPeerClient` + `Resume`, with a proxy client that can
simulate a partition by refusing to forward RPCs. `hw4/broker/main.go`'s
`newBrokerServer` (`broker/main.go:94-108`) calls `raft.New` directly, which
would need a `NewPaused`-based constructor path (or a test-only variant) to
be wired into that harness the same way `hw3/server/main.go` is — `raft.go`
was copied unmodified, so nothing about the harness itself needs to change,
only how `brokerServer` is constructed in tests.

Minimum scenarios once the harness exists:
- Submit → Poll → Complete → GetResult on a healthy cluster.
- `Submit`/`Poll`/`Heartbeat`/`Complete` against a follower returns
  `redirect_addr` pointing at the actual leader.
- Kill/partition the leader mid-task; confirm a new leader is elected,
  in-flight tasks become pollable again (the `refreshLeaderState` reset),
  and previously committed `done` state survives on the new leader.
- Partition-and-heal: a node that missed several commits catches up (this
  is the `nextIndex` backoff path — already observed once manually taking
  196 round trips to resync a far-behind node; worth a bound so a
  regression doesn't silently make this pathological).

**Why it's needed:** this is the entire point of the extension. Right now
"the Raft-replicated broker survives a leader crash" is a claim backed by
one manual run, not a test that fails if someone changes `Poll`'s scan logic
and reintroduces a duplicate-assignment bug.

**Scope:** medium — the harness is a copy-and-adapt job from `hw3`, the
scenarios themselves are straightforward given the harness exists.

---

## 3. `internal/brokerclient` tests

**What's needed:** unit tests for `Client.call`'s retry/redirect logic
(`internal/brokerclient/client.go:74-111`) against a fake `pb.BrokerClient`
(no real broker needed) covering:
- A redirect to an address in the client's own list jumps straight there
  (`addrIndex` match, `client.go:60-72`).
- A redirect to an unknown address (the Docker-vs-host hostname mismatch
  case the design doc calls out) falls back to round-robin instead of
  failing.
- Exhausting `maxHops` returns an error rather than looping forever.
- A connection error (not just a redirect) also advances to the next
  address.

**Why it's needed:** this is the piece standing between "the broker cluster
tolerates a leader crash" and "the worker/producer actually notice and
recover" — and it's the one part of this extension that's pure logic with no
Raft or gRPC server involved, so it's the cheapest integration risk to
close.

**Scope:** small. A fake `pb.BrokerClient` implementation returning
scripted responses is enough; no real network or broker process needed.

---

## 4. Worker/producer pipeline tests

**What's needed:** an end-to-end test of chunk → embed → upsert using
`tools/mock_embedder` (already deterministic, already used for manual
verification) against either a real Qdrant container or a fake
`qdrant.PointsClient`. Minimum coverage:
- A chunk submitted twice (simulating the at-least-once re-enqueue path)
  produces one Qdrant point, not two — the idempotency claim
  `README.md` makes but that has no automated check.
- `chunkText` (producer) chunk boundaries and `taskPayload` JSON round-trip
  through the broker unchanged.

**Why it's needed:** the idempotent-upsert argument is the load-bearing
correctness claim for *both* the original at-least-once design and the new
"Poll/Heartbeat don't need to be replicated" argument in `TO_IMPLEMENT.md`'s
companion doc. It's asserted in prose in two places and tested in neither.

**Scope:** medium — mainly gated on picking a Qdrant test strategy (see
Testing infrastructure below).

---

## 5. Tests for the `TO_IMPLEMENT.md` additions (once built)

Forward-looking — add these alongside each item as it's implemented, not
before:
- **kNN search (`query.go`):** given known vectors upserted into a test
  Qdrant collection, `Search` returns the expected top-k ordering.
- **Persistent query embedder:** same input text embedded twice returns the
  same vector (determinism check), and the subprocess survives multiple
  sequential queries without being respawned.
- **Retrieval API (HTTP/MCP):** request/response contract tests — valid
  query returns well-formed JSON, malformed input is rejected cleanly,
  matches the shape documented in `TO_IMPLEMENT.md` item 3.
- **Sync single-doc ingestion path:** a document ingested through this path
  is immediately retrievable via the search endpoint (the actual point of
  bypassing the broker for one-off adds), and ingesting the same document
  twice doesn't duplicate points (same `chunkIDToUUID` idempotency as #4).

---

## Testing infrastructure gaps (blocking items 2 and 4)

- **Qdrant in tests:** no test double or container strategy exists yet.
  Options: `testcontainers-go` spinning up real `qdrant/qdrant`, or a small
  in-memory fake implementing just the `PointsClient`/`CollectionsClient`
  RPCs this codebase actually calls (`Get`, `Create`, `Upsert`, `Search`).
  The fake is faster and has no Docker dependency; the real container is
  higher-fidelity and would also exercise the actual gRPC wire format.
- **No CI workflow.** There's no `.github/workflows/` in this repo at all,
  for any homework — so none of the above runs automatically today even
  once written. Not blocking for local development, but worth flagging
  since "tests exist" and "tests run on every change" are different claims.
