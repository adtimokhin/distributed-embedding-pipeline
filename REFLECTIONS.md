# HW4 Reflections

**Name:** Aleksandr Timokhin
**GitHub Username:** adtimokhin
**Hours spent:** 10

---

## Stage Progress

- [x] Stage 1 - Broker, workers, and producer
- [x] Stage 2 - At-least-once delivery
- [x] Stage 3 - Containerized deployment
- [ ] Extension - Persistent broker WAL

---

## Scaling Experiment (Stage 3)

Record your wall-clock times here after running the corpus index with 1 and 4 workers.

| Workers | Time to index corpus (s) | Notes |
|---------|--------------------------|-------|
| 1       |                          |       |
| 4       |                          |       |

---

## Design Questions

### Q1 — Poll vs. push

Workers poll the broker for tasks. What are the tradeoffs compared to the broker
pushing tasks to workers? What does polling cost at idle? Under what conditions
would you switch to a push-based model?

**Your answer:**

Polling is simpler to implement and more fault-tolerant from the broker's perspective: the broker holds no per-worker connection state and does not need to detect dead workers to stop pushing at them. If a worker dies, the broker simply stops receiving Polls from it - no cleanup required. The tradeoff is wasted work at idle: with `PollInterval = 100ms` and N workers, the broker receives N×10 RPCs per second even when the queue is empty. Each RPC acquires the mutex, checks `len(s.pending) == 0`, and returns immediately. At small scale this is negligible; at thousands of workers it becomes measurable lock contention and CPU load.

A push-based model eliminates idle RPCs and reduces latency - the broker delivers a task to a worker the seconf one becomes available rather than waiting up to one poll interval. The main issue (cost) is that the broker must maintain a registry of connected workers, handle disconnected workers gracefully, and distribute tasks fairly.

Under which conditions I would switch to the pused-based model:

1. Task latency is latency-sensitive enough that 100ms scheduling delay matters.
2. The number of idle workers is large enough that polling overhead is measurable.
3. The system already uses a streaming transport (like Kafka) that makes push natural.

---

### Q2 — Persistent subprocess

You maintain one long-lived embedder subprocess per worker rather than spawning
one per task. What does this buy you? What does it cost? What happens if the
subprocess crashes mid-task? How does your implementation handle this?

**Your answer:**

Keeping one subprocess alive for the worker's lifetime saves time. Loading `all-MiniLM-L6-v2` requires downloading and deserializing ~90MB of weights plus initializing the PyTorch runtime - this takes several seconds on first start. If the model had do be loaded once per task, the time to run a task would increase, and the system would work slower overall.

This speed comes at a cost of reduced fault isolation. The subprocess holds the model in memory for the worker's entire lifetime. If it leaks memory or corrupts its internal state, the worker must be restarted to recover. A per-task subprocess would self-heal on every invocation but is impractically slow.

If the subprocess crashes mid-task, `e.stdout.Scan()` returns `false` with `e.stdout.Err() == nil` (subprocess closed stdout), and `embed` returns `"subprocess closed stdout unexpectedly"`. The worker logs the error, calls `Complete` with the error string, and returns from `run()`, causing the process to exit. In Stage 2, if the worker crashes hard enough to skip the `Complete` call, the broker's `reEnqueueStalled` goroutine detects the missing heartbeat after `TaskTimeout = 10s` and moves the task back to `pending` for a healthy worker to pick up.

---

### Q3 — Workers write directly to Qdrant

Embedding vectors are written to Qdrant by the worker, not returned through
the broker. What are the consistency implications? Describe a failure scenario
where a worker upserts successfully but fails to call `Complete`. What happens
to the task? Is the resulting duplicate upsert a problem?

**Your answer:**

The broker and Qdrant maintain independent state with no distributed transaction between them. A task can be `done=true` in the broker but absent from Qdrant (if the upsert failed before `Complete`), or present in Qdrant but still `inflight` in the broker (if `Complete` was never called). The producer has no way to verify which chunks actually landed in Qdrant - it only knows whether the broker acknowledged completion.

Consider a scenario: Worker W polls task T for chunk `doc42-3`. W computes the embedding, calls `Upsert` on Qdrant successfully, and then its process is killed by the OS before it can call `Complete`. From the broker's perspective, T is still inflight. In Stage 2, after `TaskTimeout = 10s` of missing heartbeats, `reEnqueueStalled` moves T back to `pending`. Worker W2 eventually polls T, re-embeds the same text (same model, deterministic output), and calls `Upsert` again with the same point ID (`chunkIDToUUID("doc42-3")` is deterministic). Qdrant's upsert is a blind overwrite by point ID - the second write lands with the same vector and payload as the first. W2 then calls `Complete` successfully.

The duplicate upsert is not a correctness problem precisely because `chunkIDToUUID` is deterministic: the same chunk always maps to the same numeric point ID, and the embedding model always produces the same vector for the same text. Qdrant's upsert semantics overwrite the existing point with identical data. The final state of the collection is the same as if the task had run exactly once.

---

### Q4 — In-memory broker

What happens to all queued and in-flight tasks if the broker process crashes?
What would you need to add to make the broker crash-tolerant? (Hint: you have
seen this pattern in HW3.)

**Your answer:**

All state - `s.pending`, `s.inflight`, `s.done`, and `s.errors` - live only in process memory. If the broker crashes, every task in all three states is lost. The producer holds a list of task IDs it submitted and is polling, but those IDs are now orphaned: `GetResult` will return `done=false` forever (the broker has no record of them after restart). The producer would block indefinitely waiting for tasks that will never complete.

To make the broker crash-tolerant I would add a write-ahead log (WAL) on disk. Before returning from any state-mutating RPC (`Submit`, `Poll`, `Complete`), the broker would append the operation and its arguments to a durable log file and call `fsync`. On startup, the broker would replay the log to reconstruct `pending`, `inflight`, `done`, and `errors` before accepting new RPCs. A `Submit` entry adds a task to `pending`; a `Poll` entry moves it to `inflight`; a `Complete` entry moves it to `done`. This guarantees that any operation acknowledged to the caller has been durably recorded and will survive a crash and restart.

---

### Q5 — Task timeout tuning

Your broker re-enqueues tasks after `TaskTimeout = 10s`. Embedding a passage
takes roughly 50–200ms on CPU. Is 10s appropriate? What happens if the timeout
is too short? Too long? How would you tune it for a production pipeline?

**Your answer:**

10s is reasonable for a development setup - it gives 50–200× headroom over the typical 50–200ms embedding time, which absorbs GC pauses, brief CPU saturation, and slow Qdrant writes without false re-enqueues. The heartbeat interval is 3s, so a healthy worker sends 3 heartbeats within the 10s window; it would need to miss all three consecutive heartbeats before being considered stalled.

A timeout that is too short causes false re-enqueues on healthy but momentarily slow workers: the broker moves the task back to `pending` while the original worker is still processing it, so two workers embed the same chunk simultaneously. This wastes CPU and triggers unnecessary duplicate Qdrant upserts.

A timeout that is too long means a crashed worker's tasks sit in `inflight` for a long time before a healthy worker can pick them up, increasing tail latency for the overall pipeline. With 10s and a 6,000-chunk corpus, a single crashed worker could stall its last 1–2 in-flight tasks for up to 10s, which is acceptable.

In production I would set `TaskTimeout` to the P99 task duration multiplied by a factor of 4. It should ve enough to absorb legitimate slowness without masking actual failures.

To be more specific: I would collect a histogram of task durations from a representative run, find P99, and set the timeout to `P99 * 4`. The heartbeat interval should be roughly `TaskTimeout / 4` so a healthy worker sends at least three heartbeats before the deadline.

---

## Reflection Questions

### R1 — Delivery semantics (Lec 16)

Your broker provides **at-least-once** delivery. Describe a concrete execution
trace in which the same chunk gets embedded twice. Given that Qdrant upserts are
idempotent (same `chunk_id` overwrites the existing point), is this a correctness
problem? What additional mechanism would you need for **exactly-once** delivery?

**Your answer:**

Example execution trace: Worker W1 polls task T (`chunk_id = "doc7-2"`). W1 starts embedding but enters a long GC pause - it does not call `Heartbeat` for 11 seconds. The broker's `reEnqueueStalled` goroutine fires, notices `time.Since(e.lastHeartbeat) > 10s`, removes T from `inflight`, and prepends it back to `pending`. Worker W2 polls T, embeds the chunk, upserts the vector to Qdrant, and calls `Complete` - T is now `done=true`. Meanwhile W1 wakes from its GC pause, finishes embedding the same text, and calls `Upsert` on Qdrant again. W1 then calls `Complete(T)` - the broker treats it as a no-op since T is already in `s.done`. The chunk has been embedded twice.

This is not a correctness problem. `chunkIDToUUID` is deterministic (FNV-64 of the chunk ID), and `all-MiniLM-L6-v2` is deterministic for the same input text. Both upserts write the same point ID with the same 384-dimensional vector and the same payload. The second upsert is a no-op from Qdrant's perspective - the collection state after two executions is identical to what it would be after one.

For exactly-once delivery I would need a distributed transaction spanning the broker's `Complete` and Qdrant's `Upsert` - atomically committing both or neither. One practical approach is the two-phase pattern used in Flink's exactly-once sinks: the worker writes the embedding to Qdrant inside a transaction, obtains a transaction ID, and only calls `Complete` after the transaction commits. The broker marks the task done only after receiving the confirmed commit. This requires transactional write support from Qdrant (available via its batch write API with `ordering=strong`) and a more complex `CompleteRequest` that carries the transaction handle.

---

### R2 — Re-execution vs. Raft (Lec 15 vs. Lec 8)

HW3 uses Raft to tolerate node failures. HW4 uses re-execution. Why is
re-execution appropriate here but not for a KV store? What property of embedding
tasks makes re-execution safe (hint: think about determinism and side effects)?

**Your answer:**

Re-execution is safe here because embedding is a pure function: given the same input text and the same model weights, `all-MiniLM-L6-v2` always produces the same 384-dimensional vector. The task has no shared mutable side effects. The only write is a Qdrant upsert keyed by a deterministic point ID, and that upsert is idempotent. Executing the same task ten times produces exactly the same database state as executing it once.

A KV store Put("x", "v") is not a pure function in the same sense. If a Put is applied twice - say because both the original leader and a newly elected leader replay the operation - the second application overwrites a value that may have been changed by a subsequent Put from a different client in between. Re-execution would violate linearizability. Raft solves this by ensuring each log entry is applied exactly once in a globally agreed order: the log is the mechanism that prevents the same command from being committed more than once, even under failures. There is no equivalent log-ordering requirement for embedding tasks because the idempotent upsert absorbs duplicates without visible effect.

---

### R3 — Stateless workers (Lec 14)

Why can workers scale horizontally while the broker cannot (without additional
engineering)? The embedding subprocess holds a 90MB model in memory - it is
stateful. Why doesn't that violate the definition of a stateless worker?

**Your answer:**

Workers can scale horizontally because they hold no inter-task state. After a worker calls `Complete`, it retains nothing about the task it just processed - no partial results, no task history, no coordination with other workers. The next `Poll` starts with exactly the same internal state as the first. Adding a fourth worker does not require any of the three existing workers to change behavior or redistribute data: each simply polls the same broker and writes to the same Qdrant collection independently. The broker is the single point of coordination, and all shared state (which tasks are pending, inflight, done) lives there.

The broker cannot scale horizontally in the same way because it holds the authoritative task state. Running two broker instances with independent in-memory maps would split the task queue - a producer's `Submit` would go to one broker, a worker's `Poll` might go to the other, and neither would be aware of the other's tasks. To scale the broker horizontally you would need a replicated state machine (like Raft, as in HW3) or an external durable store that all broker instances share.

The 90MB model does not violate statelessness because it is not task-specific state. The definition of a stateless worker is that it holds no information that must survive from one task to the next - not that it holds no memory at all. The model weights are the same for every task; they are loaded once and reused, like a cached read-only library. Each worker's model copy is independent of every other worker's. The worker's behavior for task T is entirely determined by the task payload and the model - not by any prior task it has processed.

---

### R4 — Throughput scaling

Was the speedup from 1 → 4 workers linear? What limits perfect linear scaling?
Where is the bottleneck in your setup - broker lock contention, subprocess
startup, Qdrant write throughput, or network? How did you determine this?

**Your answer:**

*(Times to be filled in after running the Stage 3 Docker Compose experiment.)*

I expect the speedup to be sub-linear - roughly 2–3× with 4 workers rather than 4×. On a single physical machine, all four worker containers and the Python subprocesses compete for the same CPU cores during inference. `all-MiniLM-L6-v2` is CPU-bound; four concurrent embedding calls contend for cores rather than executing in parallel on separate hardware. This is the same reason the background section notes "on a single machine, workers compete for CPU cores during inference rather than parallelizing."

The most likely bottleneck is CPU saturation during model inference, not the broker or Qdrant. Evidence: the broker's critical section is tiny (append to a slice or delete from a map), so lock contention at 4 workers is negligible. Qdrant writes are fast for 384-dim vectors. Subprocess startup is a one-time cost at worker initialization, not per-task.

To determine the actual bottleneck I would: (1) run `top` or `htop` while the pipeline is active - if all cores are pinned at 100%, inference is the limit; (2) compare the per-task latency reported in worker logs at 1 vs. 4 workers - if per-task time grows proportionally with N workers, it confirms CPU sharing, not broker contention; (3) add a broker-side counter of concurrent inflight tasks to confirm the mutex is never held long enough to matter.

---

## Challenges and Surprises

_What was the hardest bug you had to debug? How did you find it?
What would you do differently if you started over?_

**Your answer:**

The hardest issue was a `go vet` error that appeared only after implementing the broker: protobuf-generated `Task` structs embed `protoimpl.MessageState` which contains a `sync.Mutex`, making the type non-copyable. The original stub stored `[]pb.Task` (value slice) and `inFlightEntry{task pb.Task}`, which caused vet to reject the code with "assignment copies lock value." The fix was to switch every occurrence to `[]*pb.Task` and `*pb.Task` - a small change that required touching the struct definition, Submit, Poll, and inFlightEntry simultaneously to keep everything consistent.

A second subtle point was the ordering of `cancelHB()` relative to `Complete` in the worker. My first instinct was to call `cancelHB()` after `Complete` to keep the heartbeat running as long as possible - but this is wrong. If a heartbeat fires after `Complete` returns, the broker has already moved the task to `done`, and the heartbeat's `inflight[req.TaskId]` lookup quietly does nothing (the entry is gone). More importantly, in a failure path, calling `cancelHB()` after `Complete` means the heartbeat goroutine could fire in the gap between `Complete` returning and the function exiting, sending a heartbeat for a task the broker no longer tracks. Calling `cancelHB()` before `Complete` avoids the race entirely - by the time `Complete` is sent, the goroutine has already stopped.

If I started over I would test the Qdrant upsert path first with a single hard-coded vector before wiring it into the full pipeline, to verify the collection schema and client API calls in isolation before adding the broker and subprocess layers.
