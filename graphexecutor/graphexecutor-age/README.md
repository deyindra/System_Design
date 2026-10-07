# graphexecutor-age: a graph in Apache AGE, processed by workers on many VMs

The `core` SPIs over PostgreSQL with the [Apache AGE](https://age.apache.org) extension. The graph, the run's
completion log and its cluster state all live in one database, which is the only thing the workers share. See
the [parent README §4](../README.md#4-across-machines-a-graph-in-a-database-workers-on-many-vms) for the protocol.

| Class | Implements | Storage |
|---|---|---|
| `AgeGraphStore<T>` | `GraphStore<T>` | vertices `{key, shard}` and edges of one AGE graph; Cypher through `cypher()` with agtype parameters |
| `AgeGraphLoader<T>` | | creates the graph, its labels and the `(shard, key)` indexes; batched `UNWIND … CREATE` |
| `PostgresCompletionLog<T>` | `CompletionLog<T>` | `completion_log`, key `(run_id, node_key)`, `INSERT … ON CONFLICT DO NOTHING` |
| `PostgresClusterStore` | `ClusterStore` | `cluster_run`, `cluster_lease`, `cluster_attempt`, `cluster_message`, `cluster_arrival`; leases on `now()`, checks under `SELECT … FOR UPDATE` |
| `PgConnections` | | a small JDBC pool. Graph connections run `LOAD 'age'` and set `search_path` |
| `AgeWorkerContext` | | a worker process's stores and `Worker.Config`, read from the environment; `close()` closes the pools |

DDL runs under a transaction-scoped advisory lock, so workers that start together can each create the tables.

**Worker environment**, read by `AgeWorkerContext.of(env)`:

| Variable | Default | Meaning |
|---|---|---|
| `DB_URL` | required | e.g. `jdbc:postgresql://age:5432/postgres` |
| `DB_USER`, `DB_PASSWORD` | `postgres`, empty | |
| `GRAPH` | `graph` | the AGE graph's name |
| `SHARDS` | required | the shard count the graph was loaded with |
| `RUN_ID` | required | the run every worker joins |
| `WORKER_ID` | `$HOSTNAME`, else random | unique per worker |
| `SHARDS_PER_WORKER` | 4 | `Worker.Config.maxShards` |
| `LEASE_TTL`, `POLL_INTERVAL` | `PT10S`, `PT0.1S` | how soon a dead worker is replaced; idle wait |
| `BATCH_SIZE` | 500 | nodes per store read |

**Your own workers.** This module is a library: it has no `main` and ships no image. Put your executor
in a module of its own that depends on `graphexecutor-age`, with a `main` that opens a `AgeWorkerContext`
(try-with-resources), builds a `Worker` from a `Distributed…Executor` subclass with your `executeTask` and
the context's stores and `config()`, runs it, and exits 0 when the run is `DONE`. Give the module the
`copy-dependencies` execution and a Dockerfile that runs your `main` on its jar plus `target/lib`.
[`graphexecutor-age-demo`](../graphexecutor-age-demo/README.md) is one such module, whose `DemoWorkerMain`
picks the executor by `WORKER_FACTORY`. The web crawler's
[`adapter-graph-age`](../../webcrawler/adapter-graph-age) uses only `AgeGraphLoader` and `AgeGraphStore`, for
its sitemap graphs.

**Run the demo:** [`graphexecutor-age-demo`](../graphexecutor-age-demo/README.md) has a loader job, demo
factories, a compose cluster, and the Docker ITs.

**Tests** (4): `AgtypeTest`, unit tests of the agtype encoding. The ITs against a real AGE,
`AgeStoresIT` and `AgeWorkerContainersIT`, are in the demo module.
