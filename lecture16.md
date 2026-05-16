# Stream Processing Systems: Architecture, Kafka, and Event Time Progress

## 1. Introduction to Stream Processing
*   **Batch vs. Stream:** Traditional batch processing (like MapReduce or Spark) assumes bounded datasets with a beginning and an end. In contrast, **stream processing** handles an infinite sequence of events arriving over time, requiring continuous processing with low latency.
*   **Core Use Cases:** Critical for tasks batch processing cannot do, such as detecting credit card fraud within 100ms, updating real-time feeds, alerting engineers to server spikes within 30 seconds, and computing real-time ride-share surge pricing.
*   **Conceptual Shift:** Batch follows a "read → process → write → done" flow, while streaming follows a "**read continuously → process continuously → write continuously → never done**" model.
*   **The Unit of Streaming:** An **event** is an **immutable** record of a fact at a point in time. Since they cannot be changed, event streams are append-only logs that allow for replayable processing.

## 2. Event Brokers and Apache Kafka
*   **Decoupling:** Message brokers or event brokers decouple producers (applications, sensors) from consumers (stream processors).
*   **Kafka Architecture:** Kafka models the queue as a **distributed, partitioned, and replicated commit log**.
    *   **Topics & Partitions:** Topics are named streams (e.g., "purchases"), which are divided into **partitions** (ordered, append-only logs) to allow for **partitioned parallelism**.
    *   **Offsets:** Each event has a unique offset within its partition. Consumers track their current offset, giving them explicit control over their reading pace.
*   **High Throughput:** Kafka achieves millions of events per second per node by writing sequentially to disk and using **zero-copy transfer** (sendfile syscall) to avoid data copying.
*   **Durability and Replay:** Unlike traditional queues that delete messages after consumption, Kafka retains them for a configured period, allowing consumers to rewind offsets and replay data.
*   **Producer API & Acknowledgments:** Producers choose partitions via key hashing. Delivery guarantees depend on "acks": `acks=0` (fire and forget), `acks=1` (leader written), or `acks=all` (all replicas written for maximum durability).

## 3. Stream Processing Engines (Flink, Kafka Streams)
*   **Why Use Them?** While raw consumer APIs handle stateless transformations, stream processors are necessary for windowed aggregations, joins, and **exactly-once guarantees**.
*   **Key Components:**
    *   **Operator DAG:** Programs are compiled into a Directed Acyclic Graph of stateful or stateless transformations (map, filter, join).
    *   **State Backend:** Keyed state is stored locally (often in **RocksDB**) for fast access and periodically checkpointed to durable storage.
    *   **Checkpoint Coordinator:** Injects barriers into the stream to ensure distributed state snapshots are consistent.
*   **Flow Control:** Systems like Flink use **TCP flow control** and bounded network buffers to propagate backpressure; if a "sink" (output) is slow, the entire upstream pipeline naturally slows down to prevent memory issues.

## 4. Handling Time and Windowing
*   **Event Time vs. Processing Time:** **Event time** is when a fact occurred; **processing time** is when the processor sees it. Events often arrive late or out-of-order due to network delays or mobile app buffering.
*   **Watermarks:** These are logical progress indicators stating that "all events with timestamp $\leq W$ have been observed". They allow the system to know when it can safely close and emit a time window.
*   **Window Types:**
    *   **Tumbling:** Fixed-size, non-overlapping windows (e.g., "every minute").
    *   **Sliding:** Fixed-size, overlapping windows (e.g., "5-minute rolling average updated every minute").
    *   **Session:** Variable-sized windows defined by periods of activity and gaps of inactivity.

## 5. Fault Tolerance and Exactly-Once Semantics
*   **Stateful Needs:** State is required for aggregations (running counts), joins (buffering events), and deduplication.
*   **Consistent Snapshots:** In a distributed system, taking a snapshot is difficult because there is no shared clock. A **consistent cut** ensures that if a snapshot records the receipt of a message, it must also record its sending.
*   **Chandy-Lamport Algorithm:** Flink uses a variation of this algorithm by injecting **barriers** into the stream. When an operator receives barriers from all inputs, it snapshots its local state and forwards the barrier downstream.
*   **Exactly-Once:** Flink achieves this by rolling back all tasks to the last committed checkpoint and replaying Kafka events from the recorded offsets upon failure, ensuring no event is double-counted or missed.

## 6. The Dataflow Model and Stream-Table Duality
*   **Duality:** A table is a snapshot of state, while a stream is the sequence of changes (changelog) that produced it; one can always be reconstructed from the other.
*   **Dataflow Model (What, Where, When, How):** This model unifies batch and stream processing by viewing batch as a stream over a bounded dataset. It defines:
    *   **What** results are computed (aggregations).
    *   **Where** in event time (windowing).
    *   **When** in processing time to emit (triggers/watermarks).
    *   **How** to refine results (accumulation modes).
*   **Unified Engines:** Unlike Spark (which treats streaming as "mini-batches"), Flink is a native streaming engine where batch is a special case of streaming.

## 7. Practical Considerations
*   **Data Quality:** Systems must handle out-of-order data and duplicate events (often using Bloom filters for deduplication).
*   **State Management:** Long-running jobs must manage state size using Time-To-Live (TTL) to expire old data.
*   **Operations:** Scaling requires redistributing key-partitioned state, which involves stopping a job and restarting it from a **savepoint** with a new partition assignment.
*   **End-to-End Exactly-Once:** Requires the stream processor's internal guarantees to be paired with **idempotent or transactional sinks** (like Kafka transactions) to ensure external systems don't receive duplicate outputs.