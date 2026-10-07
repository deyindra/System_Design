# graphexecutor-neo4j: a graph in Neo4j, processed by workers on many VMs

The `core` SPIs over Neo4j 5 (Bolt, `neo4j-java-driver`). As with AGE, the graph, the completion log and the
cluster state live in one database, which is all the workers share. See the
[parent README §4](../README.md#4-across-machines-a-graph-in-a-database-workers-on-many-vms) for the protocol.

| Class | Implements | Storage |
|---|---|---|
| `Neo4jGraphStore<T>` | `GraphStore<T>` | nodes `(:<label> {key, shard})` and relationships `<label>_EDGE`; batched `UNWIND $keys` |
| `Neo4jGraphLoader<T>` | | key constraint and `(shard, key)` index; `UNWIND … MERGE` nodes and `CREATE` edges |
| `Neo4jCompletionLog<T>` | `CompletionLog<T>` | `(:Outcome {run, key})`, unique; `MERGE … ON CREATE SET` |
| `Neo4jClusterStore` | `ClusterStore` | `ClusterRun`, `ClusterLease`, `ClusterAttempt`, `ClusterMessage` and `ClusterArrival` nodes, each with a uniqueness constraint; leases on `datetime()` |
| `Neo4jSessions` | | managed read and write transactions, plus `lock()`: `SET n._lock = true REMOVE n._lock` |
| `Neo4jWorkerContext` | | a worker process's stores and `Worker.Config`, read from the environment; `close()` closes the driver |

**Check, then write.** Neo4j reads without taking locks. So every check-then-write first takes the node's
write lock (`Neo4jSessions.lock`), then reads and writes in the same transaction. Those operations are
claim, renew, arrive, advance, fail and beginAttempt. This is the counterpart of PostgreSQL's `FOR UPDATE`.

**Schema races.** Workers that start together each run `CREATE … IF NOT EXISTS`. A concurrent duplicate
fails with `EquivalentSchemaRuleAlreadyExists`, and `schema()` treats that as success.

**Worker environment:** the same as AGE's, with the database settings replaced by `NEO4J_URI`,
`NEO4J_USER` (default `neo4j`), `NEO4J_PASSWORD` and `NEO4J_DATABASE` (default `neo4j`). `GRAPH` is the
node label (default `Node`).

**Your own workers.** This module is a library: it has no `main` and ships no image. Put your executor
in a module of its own that depends on `graphexecutor-neo4j`, with a `main` that opens a `Neo4jWorkerContext`
(try-with-resources), builds a `Worker` from a `Distributed…Executor` subclass with your `executeTask` and
the context's stores and `config()`, runs it, and exits 0 when the run is `DONE`. Give the module the
`copy-dependencies` execution and a Dockerfile that runs your `main` on its jar plus `target/lib`.
[`graphexecutor-neo4j-demo`](../graphexecutor-neo4j-demo/README.md) is one such module, whose `DemoWorkerMain`
picks the executor by `WORKER_FACTORY`. The web crawler's
[`adapter-graph-neo4j`](../../webcrawler/adapter-graph-neo4j) uses only `Neo4jGraphLoader` and
`Neo4jGraphStore`, for its sitemap graphs.

**Run the demo:** [`graphexecutor-neo4j-demo`](../graphexecutor-neo4j-demo/README.md) has a loader job, demo
factories, a compose cluster, and the Docker ITs.

> **Caveat: storage does not scale out on Community.** Neo4j Community edition is a single database
> instance. The workers still spread the compute and the memory over many VMs: each holds only its frontier
> and batches. But the graph itself must fit one server. To shard storage across servers, you need
> Enterprise, with Fabric or composite databases.

**Tests** (13), none needing a database: `Neo4jGraphTest`, the label check; `Neo4jWorkerContextTest`, the
environment; `Neo4jSessionsTest`, sessions and the schema race, on a fake driver. The ITs against a real Neo4j,
`Neo4jStoresIT` and `Neo4jWorkerContainersIT`, are in the demo module.
