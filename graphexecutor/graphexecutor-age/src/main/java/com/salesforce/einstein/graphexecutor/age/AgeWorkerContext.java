package com.salesforce.einstein.graphexecutor.age;

import com.salesforce.einstein.graphexecutor.distributed.Worker;
import com.salesforce.einstein.graphexecutor.spi.ClusterStore;
import com.salesforce.einstein.graphexecutor.spi.CompletionLog;
import com.salesforce.einstein.graphexecutor.spi.GraphStore;
import com.salesforce.einstein.graphexecutor.spi.NodeCodec;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.ToIntFunction;

/**
 * What a worker process (one per VM or container) of a run over a graph in AGE builds its {@link Worker}
 * from: the stores of this database, and the worker's config. Read from the environment, so one image runs
 * everywhere:
 * <pre>DB_URL             jdbc:postgresql://host:5432/db      (required)
 * DB_USER, DB_PASSWORD                                    (postgres, empty)
 * GRAPH              the AGE graph's name                 (graph)
 * SHARDS             the graph's shard count, as loaded   (required)
 * RUN_ID             the run every worker joins           (required)
 * WORKER_ID          unique per worker                    ($HOSTNAME, else random)
 * SHARDS_PER_WORKER  {@link Worker.Config#maxShards()}         (4)
 * LEASE_TTL, POLL_INTERVAL  ISO-8601 durations            (PT10S, PT0.1S)
 * BATCH_SIZE         nodes per store read                 (500)</pre>
 *
 * @param graphConnections for Cypher (they ran {@link PgConnections#AGE})
 * @param tableConnections for the log and cluster tables
 */
public record AgeWorkerContext(Map<String, String> env, PgConnections graphConnections, PgConnections tableConnections,
                               AgeGraph graph, int shards, Worker.Config config) implements AutoCloseable {

    /** Reads the environment described above. */
    public static AgeWorkerContext of(Map<String, String> env) {
        int shards = Integer.parseInt(require(env, "SHARDS"));
        String worker = env.getOrDefault("WORKER_ID", env.getOrDefault("HOSTNAME", UUID.randomUUID().toString()));
        Worker.Config config = Worker.Config.of(require(env, "RUN_ID"), worker)
                .withMaxShards(Integer.parseInt(env.getOrDefault("SHARDS_PER_WORKER", "4")))
                .withLease(Duration.parse(env.getOrDefault("LEASE_TTL", "PT10S")),
                        Duration.parse(env.getOrDefault("POLL_INTERVAL", "PT0.1S")))
                .withBatchSize(Integer.parseInt(env.getOrDefault("BATCH_SIZE", "500")));
        String url = require(env, "DB_URL");
        String user = env.getOrDefault("DB_USER", "postgres");
        String password = env.getOrDefault("DB_PASSWORD", "");
        int connections = config.maxShards() + 2;   // a shard thread each, the run loop, the heartbeat
        return new AgeWorkerContext(Map.copyOf(env),
                new PgConnections(url, user, password, connections, PgConnections.AGE),
                new PgConnections(url, user, password, connections, List.of()),
                AgeGraph.named(env.getOrDefault("GRAPH", "graph")), shards, config);
    }

    public <T> GraphStore<T> graphStore(NodeCodec<T> codec, ToIntFunction<? super T> shardOf) {
        return new AgeGraphStore<>(graphConnections, graph, codec, shards, shardOf);
    }

    /** Sharded by {@link GraphStore#byKey} on the codec's key. */
    public <T> GraphStore<T> graphStore(NodeCodec<T> codec) {
        return AgeGraphStore.byKey(graphConnections, graph, codec, shards);
    }

    public <T> CompletionLog<T> completionLog(NodeCodec<T> codec) {
        return new PostgresCompletionLog<>(tableConnections, config.runId(), codec);
    }

    public ClusterStore cluster() {
        return new PostgresClusterStore(tableConnections);
    }

    public String env(String name, String fallback) {
        return env.getOrDefault(name, fallback);
    }

    public static String require(Map<String, String> env, String name) {
        String value = env.get(name);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("environment variable " + name + " is required");
        }
        return value;
    }

    /** Closes the connection pools. */
    @Override
    public void close() {
        graphConnections.close();
        tableConnections.close();
    }
}
