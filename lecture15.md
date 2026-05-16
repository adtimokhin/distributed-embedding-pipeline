# Summary of Distributed Batch and Dataflow Systems

## 1. Introduction to Batch Processing
The dominant workload in large datacenters is **batch data processing**, which involves transforming and analyzing massive datasets that cannot fit on a single machine. Common examples include generating search indices, computing analytics over petabytes of logs, and training machine learning models. Distributed computation frameworks provide a **programming model** for expressing logic and a **runtime** that handles scheduling, data movement, and fault recovery.

## 2. MapReduce (2004)
MapReduce was designed to parallelize computations across thousands of commodity machines by drawing inspiration from functional programming.

*   **Programming Model:** Users define two functions: **Map** (takes an input pair and emits intermediate key-value pairs) and **Reduce** (aggregates values associated with a specific key).
*   **Execution and Data Flow:**
    *   The master process coordinates tasks, assigning input splits to map workers and then notifying reduce workers of intermediate output locations.
    *   **Shuffle Phase:** Intermediate data is sorted and partitioned by key between the map and reduce stages.
    *   **Data Locality:** To minimize network traffic, the master schedules map tasks on machines that already store the input data locally in GFS.
*   **Fault Tolerance:** MapReduce uses **idempotent re-execution**. Because tasks are pure functions with no side effects, the master simply re-runs failed tasks on different workers.
*   **Straggler Mitigation:** To prevent slow machines from delaying a job, the framework uses **speculative execution**, spawning backup copies of remaining tasks near the end of a job.
*   **Limitations:** MapReduce forces a rigid two-stage structure and requires **Disk I/O between every stage**. This makes it inefficient for iterative algorithms (like machine learning) where data must be read from and written to disk repeatedly.

## 3. Spark (2012)
Spark was developed to address MapReduce’s limitations, particularly for iterative and interactive workloads.

*   **Resilient Distributed Datasets (RDDs):** RDDs are read-only, partitioned collections of records defined by a **lineage** (a history of transformations).
*   **In-Memory Computation:** Spark can persist RDDs in memory across operations, avoiding the expensive disk reads required by MapReduce.
*   **Fault Tolerance via Lineage:** If a partition is lost, Spark recomputes it by replaying its lineage from stable storage, avoiding the need for expensive replication or checkpointing.
*   **Lazy Evaluation:** Spark builds a Directed Acyclic Graph (DAG) of the entire computation and only executes it when an "action" (like `count()` or `collect()`) is called. This allows the scheduler to optimize the execution plan.
*   **Performance:** For iterative algorithms, Spark can be **20–40x faster** than MapReduce due to in-memory caching, lower task overhead, and the elimination of intermediate disk writes.

## 4. TensorFlow and ML Dataflow (2016)
As machine learning scale increased, frameworks like TensorFlow specialized the dataflow model for tensor operations on GPUs.

*   **TensorFlow Model:** Represents ML training as a fine-grained DAG of tensor operations (e.g., matrix multiplication, convolutions).
*   **Distributed Training:**
    *   Initially used a **Parameter Server (PS)** architecture where stateless workers compute gradients and push updates to stateful servers holding model weights.
    *   As models grew (e.g., GPT-3), the PS became a bottleneck. Modern training uses **All-Reduce** (collective communication without a central leader) and **Model Parallelism** (splitting weights across multiple GPUs).
*   **Model Parallelism Techniques:** Includes **Tensor parallelism** (sharding weight matrices), **Pipeline parallelism** (assigning different layers to different GPUs), and **ZeRO** (sharding optimizer state and gradients).

## 5. Summary and Comparison
The evolution of these systems demonstrates a shift toward more general and efficient dataflow abstractions.

| Dimension | MapReduce | Spark | TensorFlow |
| :--- | :--- | :--- | :--- |
| **Innovation** | 2-stage batch processing | In-memory lineage | Fine-grained tensor DAGs |
| **State** | Stateless operations | Stateless transformations | Stateful variables (parameters) |
| **Fault Tolerance** | Re-run failed tasks | Lineage re-execution | Checkpoint + restart |
| **Primary Hardware** | CPUs | CPUs, distributed memory | GPUs, tensor cores |

**The Distributed Systems Stack:** These frameworks reside between the **Cluster Manager** (e.g., Borg, which allocates resources) and the **Applications** (e.g., web indexing, ML training). They all share the core principle of decomposing computation into a **DAG of operations** over partitioned data to achieve scalability and fault tolerance.