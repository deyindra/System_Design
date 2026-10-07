package com.salesforce.einstein.graphexecutor.neo4j;

import com.salesforce.einstein.ds.graph.Graph;
import com.salesforce.einstein.graphexecutor.spi.GraphStore;
import com.salesforce.einstein.graphexecutor.spi.NodeCodec;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.ToIntFunction;

/**
 * Writes a graph into Neo4j in the layout {@link Neo4jGraphStore} reads: a node per graph node with {@code key}
 * (the codec's) and {@code shard} (the shard function's), and a relationship per edge. {@link #create} makes the
 * uniqueness constraint on {@code key} and the {@code (shard, key)} index; {@link #addNodes} and {@link #addEdges}
 * write in batches (one {@code UNWIND} query each), so a graph larger than memory can be streamed in.
 *
 * <p>Nodes are {@code MERGE}d, so loading a node twice is harmless; adding an edge twice makes two relationships.
 *
 * @param <T> node type
 */
public final class Neo4jGraphLoader<T> {

    private final Neo4jSessions sessions;
    private final Neo4jGraph graph;
    private final NodeCodec<T> codec;
    private final ToIntFunction<? super T> shardOf;
    private final String createNodes;
    private final String createEdges;

    public Neo4jGraphLoader(Neo4jSessions sessions, Neo4jGraph graph, NodeCodec<T> codec,
                            ToIntFunction<? super T> shardOf) {
        this.sessions = Objects.requireNonNull(sessions, "sessions");
        this.graph = Objects.requireNonNull(graph, "graph");
        this.codec = Objects.requireNonNull(codec, "codec");
        this.shardOf = Objects.requireNonNull(shardOf, "shardOf");
        String v = graph.nodeLabel();
        createNodes = "UNWIND $rows AS r MERGE (n:" + v + " {key: r.key}) SET n.shard = r.shard";
        createEdges = "UNWIND $edges AS e MATCH (a:" + v + " {key: e[0]}), (b:" + v + " {key: e[1]}) CREATE (a)-[:"
                + graph.edgeType() + "]->(b)";
    }

    /** Shards by {@link GraphStore#byKey} on the codec's key, as {@link Neo4jGraphStore#byKey} reads. */
    public static <T> Neo4jGraphLoader<T> byKey(Neo4jSessions sessions, Neo4jGraph graph, NodeCodec<T> codec,
                                                int shards) {
        return new Neo4jGraphLoader<>(sessions, graph, codec, GraphStore.byKey(codec::encode, shards));
    }

    /** Creates the constraint and the index the store's queries use, unless they exist, and waits for them. */
    public Neo4jGraphLoader<T> create() {
        String v = graph.nodeLabel();
        sessions.schema(
                "CREATE CONSTRAINT " + v + "_key IF NOT EXISTS FOR (n:" + v + ") REQUIRE n.key IS UNIQUE",
                "CREATE INDEX " + v + "_shard_key IF NOT EXISTS FOR (n:" + v + ") ON (n.shard, n.key)",
                "CALL db.awaitIndexes(300)");
        return this;
    }

    /** Writes every node, then every edge, of {@code source}, {@code batchSize} per query. */
    public void load(Graph<T> source, int batchSize) {
        List<T> nodes = new ArrayList<>(source.nodes());
        for (int i = 0; i < nodes.size(); i += batchSize) {
            addNodes(nodes.subList(i, Math.min(nodes.size(), i + batchSize)));
        }
        List<Map.Entry<T, T>> edges = new ArrayList<>();
        for (T from : nodes) {
            for (T to : source.successors(from)) {
                edges.add(Map.entry(from, to));
                if (edges.size() == batchSize) {
                    addEdges(edges);
                    edges.clear();
                }
            }
        }
        addEdges(edges);
    }

    public void addNodes(Collection<? extends T> nodes) {
        if (nodes.isEmpty()) {
            return;
        }
        List<Map<String, Object>> rows = new ArrayList<>();
        for (T node : nodes) {
            rows.add(Map.of("key", codec.encode(node), "shard", shardOf.applyAsInt(node)));
        }
        sessions.write(tx -> tx.run(createNodes, Map.of("rows", rows)).consume());
    }

    /** Adds a relationship per entry, from key to value. Both nodes must have been added. */
    public void addEdges(Collection<? extends Map.Entry<? extends T, ? extends T>> edges) {
        if (edges.isEmpty()) {
            return;
        }
        List<List<String>> pairs = new ArrayList<>();
        for (Map.Entry<? extends T, ? extends T> edge : edges) {
            pairs.add(List.of(codec.encode(edge.getKey()), codec.encode(edge.getValue())));
        }
        sessions.write(tx -> tx.run(createEdges, Map.of("edges", pairs)).consume());
    }
}
