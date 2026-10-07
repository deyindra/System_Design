package com.salesforce.einstein.graphexecutor.age;

import com.salesforce.einstein.graphexecutor.spi.GraphStore;
import com.salesforce.einstein.graphexecutor.spi.NodeCodec;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.ToIntFunction;

/**
 * A {@link GraphStore} over a graph in Apache AGE, as {@link AgeGraphLoader} writes it: one vertex per node with
 * its {@code key} and {@code shard}, one edge per edge. Every read is one Cypher query: a page of a shard is
 * keyset-paged on {@code (shard, key)}, and the edges of a batch of nodes are matched with {@code key IN $keys},
 * both served by the loader's indexes.
 *
 * <p>The shard function is given, not read from the vertices, so every worker computes it without a query;
 * it must be the one the graph was loaded with.
 *
 * @param <T> node type
 */
public final class AgeGraphStore<T> implements GraphStore<T> {

    private final PgConnections connections;
    private final NodeCodec<T> codec;
    private final int shards;
    private final ToIntFunction<? super T> shardOf;
    private final String nodesFrom;
    private final String nodesAfter;
    private final String sourcesFrom;
    private final String sourcesAfter;
    private final String successors;
    private final String predecessors;

    /**
     * @param connections connections that ran {@link PgConnections#AGE}
     * @param shardOf     the loader's shard function
     */
    public AgeGraphStore(PgConnections connections, AgeGraph graph, NodeCodec<T> codec, int shards,
                         ToIntFunction<? super T> shardOf) {
        this.connections = Objects.requireNonNull(connections, "connections");
        this.codec = Objects.requireNonNull(codec, "codec");
        this.shardOf = Objects.requireNonNull(shardOf, "shardOf");
        if (shards < 1) {
            throw new IllegalArgumentException("shards must be >= 1");
        }
        this.shards = shards;
        String v = graph.vertexLabel();
        String e = graph.edgeLabel();
        String page = " RETURN n.key ORDER BY n.key LIMIT $limit";
        String source = " AND NOT exists((n)<-[:" + e + "]-())";
        nodesFrom = graph.cypher("MATCH (n:" + v + ") WHERE n.shard = $shard" + page, "key agtype");
        nodesAfter = graph.cypher("MATCH (n:" + v + ") WHERE n.shard = $shard AND n.key > $after" + page, "key agtype");
        sourcesFrom = graph.cypher("MATCH (n:" + v + ") WHERE n.shard = $shard" + source + page, "key agtype");
        sourcesAfter = graph.cypher("MATCH (n:" + v + ") WHERE n.shard = $shard AND n.key > $after" + source + page,
                "key agtype");
        // OPTIONAL MATCH: a node without edges is still in the answer, with none
        successors = graph.cypher("MATCH (n:" + v + ") WHERE n.key IN $keys OPTIONAL MATCH (n)-[:" + e + "]->(m:"
                + v + ") RETURN n.key, m.key", "key agtype, other agtype");
        predecessors = graph.cypher("MATCH (n:" + v + ") WHERE n.key IN $keys OPTIONAL MATCH (n)<-[:" + e + "]-(m:"
                + v + ") RETURN n.key, m.key", "key agtype, other agtype");
    }

    /** Shards by {@link GraphStore#byKey} on the codec's key: what {@link AgeGraphLoader#byKey} loads with. */
    public static <T> AgeGraphStore<T> byKey(PgConnections connections, AgeGraph graph, NodeCodec<T> codec,
                                             int shards) {
        return new AgeGraphStore<>(connections, graph, codec, shards, GraphStore.byKey(codec::encode, shards));
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

    private List<T> page(String sql, int shard, T after, int limit) {
        Map<String, Object> params = new LinkedHashMap<>();
        params.put("shard", shard);
        params.put("limit", limit);
        if (after != null) {
            params.put("after", codec.encode(after));
        }
        return connections.call(connection -> {
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setObject(1, Agtype.params(params));
                List<T> page = new ArrayList<>();
                try (ResultSet rows = statement.executeQuery()) {
                    while (rows.next()) {
                        page.add(codec.decode(Agtype.string(rows.getString(1))));
                    }
                }
                return page;
            }
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

    private Map<T, List<T>> edges(String sql, Collection<? extends T> nodes) {
        if (nodes.isEmpty()) {
            return Map.of();
        }
        Map<String, T> byKey = new HashMap<>();
        for (T node : nodes) {
            byKey.put(codec.encode(node), node);
        }
        return connections.call(connection -> {
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setObject(1, Agtype.params(Map.of("keys", List.copyOf(byKey.keySet()))));
                Map<T, List<T>> edges = new HashMap<>();
                try (ResultSet rows = statement.executeQuery()) {
                    while (rows.next()) {
                        List<T> others = edges.computeIfAbsent(byKey.get(Agtype.string(rows.getString(1))),
                                node -> new ArrayList<>());
                        String other = Agtype.string(rows.getString(2));
                        if (other != null) {
                            others.add(codec.decode(other));
                        }
                    }
                }
                return edges;
            }
        });
    }

    /** Leaves the connections open: they are the caller's, and usually shared. */
    @Override
    public void close() {
    }
}
