# graphexecutor-age-demo: a demo cluster over Apache AGE, and the Docker ITs

A worked example of [`graphexecutor-age`](../graphexecutor-age/README.md), kept out of its jar. Its image
holds the demo jar plus `graphexecutor-age` and its driver (`target/lib`), and runs `DemoWorkerMain`:
a worker process that runs the factory `WORKER_FACTORY` names.

| Class (`…graphexecutor.age.demo`) | Role |
|---|---|
| `RandomGraph` | the loader job: a reproducible random graph (`n0…n<NODES-1>`), loaded with `SHARDS` shards |
| `RecordingTraversal` | a `DemoWorkerMain.Factory`: a breadth-first traversal from `ROOTS` |
| `RecordingTopological` | a `DemoWorkerMain.Factory`: a dependency-ordered run (a node runs once every predecessor has) |
| `TaskRuns` | the demo task: a `task_run(run_id, node_key, worker, at)` row per run, then `TASK_TIME` of work. Because the record is in the database, a test can count every run of every task, whichever container ran it |

**Environment**, beside the worker's (see the adapter README):

| Variable | Default | Used by | Meaning |
|---|---|---|---|
| `WORKER_FACTORY` | required | `DemoWorkerMain` | `…demo.RecordingTraversal` or `…demo.RecordingTopological` |
| `NODES`, `EDGES` | 1000, 3000 | `RandomGraph` | graph size |
| `SEED` | 1 | `RandomGraph` | the same seed gives the same graph, in AGE and Neo4j alike |
| `ACYCLIC` | false | `RandomGraph` | edges only from a lower to a higher index, for `RecordingTopological` |
| `ROOTS` | every source | `RecordingTraversal` | comma-separated node keys |
| `TASK_TIME` | `PT0S` | both factories | how long each task takes |

`RandomGraph` also reads the database settings (`DB_URL`, `DB_USER`, `DB_PASSWORD`), `GRAPH` and `SHARDS`.

**Run it:**

```bash
mvn -q -pl graphexecutor/graphexecutor-age-demo -am package -DskipTests      # from System_Design
docker compose -f graphexecutor/graphexecutor-age-demo/docker/compose.yml up --scale worker=3
docker kill <one worker>        # the others take its shards over; the run still finishes
```

The compose file starts Apache AGE, then the loader job, then the workers (`RecordingTraversal` from `n0`).

**Tests** (7). They need Docker and are skipped without it. They run with failsafe after `package`, because
they build the image from `target/`:

- `AgeStoresIT` (5): the store, loader, completion log and cluster store against a real Apache AGE, and
  three in-process workers over them.
- `AgeWorkerContainersIT` (2): a traversal and a dependency-ordered run on three worker containers.
  One container is SIGKILLed while inside a task. The run must finish with every node in the round a single
  process gives it, and no task logged before the kill may run again.
