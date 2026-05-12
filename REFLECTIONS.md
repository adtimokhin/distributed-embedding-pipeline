# HW4 Reflections

**Name:**
**GitHub Username:**
**Hours spent:**

---

## Stage Progress

- [ ] Stage 1 — Broker, workers, and producer
- [ ] Stage 2 — At-least-once delivery
- [ ] Stage 3 — Containerized deployment
- [ ] Extension — Persistent broker WAL

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

---

### Q2 — Persistent subprocess

You maintain one long-lived embedder subprocess per worker rather than spawning
one per task. What does this buy you? What does it cost? What happens if the
subprocess crashes mid-task? How does your implementation handle this?

**Your answer:**

---

### Q3 — Workers write directly to Qdrant

Embedding vectors are written to Qdrant by the worker, not returned through
the broker. What are the consistency implications? Describe a failure scenario
where a worker upserts successfully but fails to call `Complete`. What happens
to the task? Is the resulting duplicate upsert a problem?

**Your answer:**

---

### Q4 — In-memory broker

What happens to all queued and in-flight tasks if the broker process crashes?
What would you need to add to make the broker crash-tolerant? (Hint: you have
seen this pattern in HW3.)

**Your answer:**

---

### Q5 — Task timeout tuning

Your broker re-enqueues tasks after `TaskTimeout = 10s`. Embedding a passage
takes roughly 50–200ms on CPU. Is 10s appropriate? What happens if the timeout
is too short? Too long? How would you tune it for a production pipeline?

**Your answer:**

---

## Reflection Questions

### R1 — Delivery semantics (Lec 16)

Your broker provides **at-least-once** delivery. Describe a concrete execution
trace in which the same chunk gets embedded twice. Given that Qdrant upserts are
idempotent (same `chunk_id` overwrites the existing point), is this a correctness
problem? What additional mechanism would you need for **exactly-once** delivery?

**Your answer:**

---

### R2 — Re-execution vs. Raft (Lec 15 vs. Lec 8)

HW3 uses Raft to tolerate node failures. HW4 uses re-execution. Why is
re-execution appropriate here but not for a KV store? What property of embedding
tasks makes re-execution safe (hint: think about determinism and side effects)?

**Your answer:**

---

### R3 — Stateless workers (Lec 14)

Why can workers scale horizontally while the broker cannot (without additional
engineering)? The embedding subprocess holds a 90MB model in memory — it is
stateful. Why doesn't that violate the definition of a stateless worker?

**Your answer:**

---

### R4 — Throughput scaling

Was the speedup from 1 → 4 workers linear? What limits perfect linear scaling?
Where is the bottleneck in your setup — broker lock contention, subprocess
startup, Qdrant write throughput, or network? How did you determine this?

**Your answer:**

---

## Challenges and Surprises

_What was the hardest bug you had to debug? How did you find it?
What would you do differently if you started over?_

**Your answer:**
