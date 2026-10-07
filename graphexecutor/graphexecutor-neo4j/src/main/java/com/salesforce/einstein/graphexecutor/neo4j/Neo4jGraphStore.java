package com.salesforce.einstein.graphexecutor.neo4j;

import com.salesforce.einstein.graphexecutor.spi.GraphStore;
import com.salesforce.einstein.graphexecutor.spi.NodeCodec;
import org.neo4j.driver.Record;
import org.neo4j.driver.Value;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.ToIntFunction;

/**
 * A {@link GraphStore} over a graph in Neo4j, as {@link Neo4jGraphLoader} writes it: one node per graph node
 * with its {@code key} and {@code shard}, one relationship per edge. Every read is one Cypher query: a page of a
 * shard is keyset-paged on the {@code (shard, key)} index, and the edges of a batch of nodes are an
 * {@code UNWIND $keys} served by the uniqueness constraint on {@code key}.
 *
 * <p>An undirected graph is stored as an edge each way (the loader writes every node's successors), so the
 * queries are the same; only {@link #isDirected()} differs.
 *
 * <p>The shard function is given, not read from the nodes, so every worker computes it without a query; it
 * must be the one the graph was loaded with.
 *
 * @param <T> node type
 */
public final class Neo4jGraphStore<T> implements GraphStore<T> {

    private final Neo4jSessions sessions;
    private final NodeCodec<T> codec;
    private final int shards;
    private final ToIntFunction<? super T> shardOf;
    private final boolean directed;
    private final String nodesFrom;
    private final String nodesAfter;
    private final String sourcesFrom;
    private final String sourcesAfter;
    private final String successors;
    private final String predecessors;

    /** @param shardOf the loader's shard function */
    public Neo4jGraphStore(Neo4jSessions sessions, Neo4jGraph graph, NodeCodec<T> codec, int shards,
                           ToIntFunction<? super T> shardOf) {
        this(sessions, graph, codec, shards, shardOf, true);
    }

    /**
     * @param shardOf  the loader's shard function
     * @param directed false for an undirected graph, which the loader wrote as an edge each way
     */
    public Neo4jGraphStore(Neo4jSessions sessions, Neo4jGraph graph, NodeCodec<T> codec, int shards,
                           ToIntFunction<? super T> shardOf, boolean directed) {
        this.directed = directed;
        this.sessions = Objects.requireNonNull(sessions, "sessions");
        this.codec = Objects.requireNonNull(codec, "codec");
        this.shardOf = Objects.requireNonNull(shardOf, "shardOf");
        if (shards < 1) {
            throw new IllegalArgumentException("shards must be >= 1");
        }
        this.shards = shards;
        String v = graph.nodeLabel();
        String e = graph.edgeType();
        // two forms each, not "$after IS NULL OR …", so the planner can seek the (shard, key) index
        String from = "MATCH (n:" + v + ") WHERE n.shard = $shard";
        String after = from + " AND n.key > $after";
        String source = " AND NOT EXISTS { (n)<-[:" + e + "]-() }";
        String order = " RETURN n.key ORDER BY n.key LIMIT $limit";
        nodesFrom = from + order;
        nodesAfter = after + order;
        sourcesFrom = from + source + order;
        sourcesAfter = after + source + order;
        // OPTIONAL MATCH: a node without edges is still in the answer, with none
        successors = "UNWIND $keys AS k MATCH (n:" + v + " {key: k}) OPTIONAL MATCH (n)-[:" + e + "]->(m:" + v
                + ") RETURN n.key, m.key";
        predecessors = "UNWIND $keys AS k MATCH (n:" + v + " {key: k}) OPTIONAL MATCH (n)<-[:" + e + "]-(m:" + v
                + ") RETURN n.key, m.key";
    }

    /** Shards by {@link GraphStore#byKey} on the codec's key: what {@link Neo4jGraphLoader#byKey} loads with. */
    public static <T> Neo4jGraphStore<T> byKey(Neo4jSessions sessions, Neo4jGraph graph, NodeCodec<T> codec,
                                               int shards) {
        return byKey(sessions, graph, codec, shards, true);
    }

    /** As {@link #byKey(Neo4jSessions, Neo4jGraph, NodeCodec, int)}, directed or not. */
    public static <T> Neo4jGraphStore<T> byKey(Neo4jSessions sessions, Neo4jGraph graph, NodeCodec<T> codec,
                                               int shards, boolean directed) {
        return new Neo4jGraphStore<>(sessions, graph, codec, shards, GraphStore.byKey(codec::encode, shards), directed);
    }

    @Override
    public boolean isDirected() {
        return directed;
    }

    @Override
    public int shards() {
        return shards;
    }

    @Override
    public int shardOf(T node) {
        return shardOf.applyAsInt(node);
    }

    @Override
    public List<T> nodes(int shard, T after, int limit) {
        return page(after == null ? nodesFrom : nodesAfter, shard, after, limit);
    }

    @Override
    public List<T> sources(int shard, T after, int limit) {
        return page(after == null ? sourcesFrom : sourcesAfter, shard, after, limit);
    }

    private List<T> page(String query, int shard, T after, int limit) {
        Map<String, Object> params = new HashMap<>();
        params.put("shard", shard);
        params.put("limit", limit);
        if (after != null) {
            params.put("after", codec.encode(after));
        }
        return sessions.read(tx -> {
            List<T> page = new ArrayList<>();
            for (Record row : tx.run(query, params).list()) {
                page.add(codec.decode(row.get(0).asString()));
            }
            return page;
        });
    }

    @Override
    public Map<T, List<T>> successors(Collection<? extends T> nodes) {
        return edges(successors, nodes);
    }

    @Override
    public Map<T, List<T>> predecessors(Collection<? extends T> nodes) {
        return edges(predecessors, nodes);
    }

    private Map<T, List<T>> edges(String query, Collection<? extends T> nodes) {
        if (nodes.isEmpty()) {
            return Map.of();
        }
        Map<String, T> byKey = new HashMap<>();
        for (T node : nodes) {
            byKey.put(codec.encode(node), node);
        }
        return sessions.read(tx -> {
            Map<T, List<T>> edges = new HashMap<>();
            for (Record row : tx.run(query, Map.of("keys", List.copyOf(byKey.keySet()))).list()) {
                List<T> others = edges.computeIfAbsent(byKey.get(row.get(0).asString()), node -> new ArrayList<>());
                Value other = row.get(1);
                if (!other.isNull()) {
                    others.add(codec.decode(other.asString()));
                }
            }
            return edges;
        });
    }

    /** Leaves the driver open: it is the caller's, and usually shared. */
    @Override
    public void close() {
    }
}
