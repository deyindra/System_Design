package com.salesforce.einstein.graphexecutor.neo4j;

import com.salesforce.einstein.graphexecutor.distributed.Worker;
import com.salesforce.einstein.graphexecutor.spi.ClusterStore;
import com.salesforce.einstein.graphexecutor.spi.CompletionLog;
import com.salesforce.einstein.graphexecutor.spi.GraphStore;
import com.salesforce.einstein.graphexecutor.spi.NodeCodec;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.function.ToIntFunction;

/**
 * What a worker process (one per VM or container) of a run over a graph in Neo4j builds its {@link Worker}
 * from: the stores of this database, and the worker's config. Read from the environment, so one image runs
 * everywhere:
 * <pre>NEO4J_URI          bolt://host:7687 or neo4j://…       (required)
 * NEO4J_USER, NEO4J_PASSWORD                              (neo4j, empty)
 * NEO4J_DATABASE                                          (neo4j)
 * GRAPH              the graph's node label               (Node)
 * SHARDS             the graph's shard count, as loaded   (required)
 * RUN_ID             the run every worker joins           (required)
 * WORKER_ID          unique per worker                    ($HOSTNAME, else random)
 * SHARDS_PER_WORKER  {@link Worker.Config#maxShards()}         (4)
 * LEASE_TTL, POLL_INTERVAL  ISO-8601 durations            (PT10S, PT0.1S)
 * BATCH_SIZE         nodes per store read                 (500)</pre>
 */
public record Neo4jWorkerContext(Map<String, String> env, Driver driver, Neo4jSessions sessions, Neo4jGraph graph,
                                 int shards, Worker.Config config) implements AutoCloseable {

    /** Reads the environment described above. */
    public static Neo4jWorkerContext of(Map<String, String> env) {
        int shards = Integer.parseInt(require(env, "SHARDS"));
        String worker = env.getOrDefault("WORKER_ID", env.getOrDefault("HOSTNAME", UUID.randomUUID().toString()));
        Worker.Config config = Worker.Config.of(require(env, "RUN_ID"), worker)
                .withMaxShards(Integer.parseInt(env.getOrDefault("SHARDS_PER_WORKER", "4")))
                .withLease(Duration.parse(env.getOrDefault("LEASE_TTL", "PT10S")),
                        Duration.parse(env.getOrDefault("POLL_INTERVAL", "PT0.1S")))
                .withBatchSize(Integer.parseInt(env.getOrDefault("BATCH_SIZE", "500")));
        Driver driver = driver(env);
        return new Neo4jWorkerContext(Map.copyOf(env), driver,
                new Neo4jSessions(driver, env.getOrDefault("NEO4J_DATABASE", "neo4j")),
                Neo4jGraph.named(env.getOrDefault("GRAPH", "Node")), shards, config);
    }

    /** A driver for {@code NEO4J_URI}, {@code NEO4J_USER} and {@code NEO4J_PASSWORD}. */
    public static Driver driver(Map<String, String> env) {
        return GraphDatabase.driver(require(env, "NEO4J_URI"),
                AuthTokens.basic(env.getOrDefault("NEO4J_USER", "neo4j"), env.getOrDefault("NEO4J_PASSWORD", "")));
    }

    /** @param shardOf the shard function the graph was loaded with */
    public <T> GraphStore<T> graphStore(NodeCodec<T> codec, ToIntFunction<? super T> shardOf) {
        return new Neo4jGraphStore<>(sessions, graph, codec, shards, shardOf);
    }

    /** Sharded by {@link GraphStore#byKey} on the codec's key. */
    public <T> GraphStore<T> graphStore(NodeCodec<T> codec) {
        return graphStore(codec, GraphStore.byKey(codec::encode, shards));
    }

    public <T> CompletionLog<T> completionLog(NodeCodec<T> codec) {
        return new Neo4jCompletionLog<>(sessions, config.runId(), codec);
    }

    public ClusterStore cluster() {
        return new Neo4jClusterStore(sessions);
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

    /** Closes the driver. */
    @Override
    public void close() {
        driver.close();
    }
}
