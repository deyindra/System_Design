package com.salesforce.einstein.graphexecutor.age;

import com.salesforce.einstein.ds.graph.Graph;
import com.salesforce.einstein.graphexecutor.spi.GraphStore;
import com.salesforce.einstein.graphexecutor.spi.NodeCodec;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.ToIntFunction;

/**
 * Writes a graph into AGE in the layout {@link AgeGraphStore} reads: a vertex per node with {@code key} (the
 * codec's) and {@code shard} (the shard function's), and an edge per edge. {@link #create} makes the graph, its
 * labels and the indexes the store's queries need; {@link #addNodes} and {@link #addEdges} write in batches (one
 * {@code UNWIND} query each), so a graph larger than memory can be streamed in.
 *
 * <p>Loads into a graph that does not have the nodes yet: adding a node twice makes two vertices.
 *
 * @param <T> node type
 */
public final class AgeGraphLoader<T> {

    private final PgConnections connections;
    private final AgeGraph graph;
    private final NodeCodec<T> codec;
    private final ToIntFunction<? super T> shardOf;
    private final String createNodes;
    private final String createEdges;

    /** @param connections connections that ran {@link PgConnections#AGE} */
    public AgeGraphLoader(PgConnections connections, AgeGraph graph, NodeCodec<T> codec,
                          ToIntFunction<? super T> shardOf) {
        this.connections = Objects.requireNonNull(connections, "connections");
        this.graph = Objects.requireNonNull(graph, "graph");
        this.codec = Objects.requireNonNull(codec, "codec");
        this.shardOf = Objects.requireNonNull(shardOf, "shardOf");
        String v = graph.vertexLabel();
        createNodes = graph.cypher("UNWIND $rows AS r CREATE (:" + v + " {key: r.key, shard: r.shard})", "x agtype");
        createEdges = graph.cypher("UNWIND $edges AS e MATCH (a:" + v + "), (b:" + v
                + ") WHERE a.key = e[0] AND b.key = e[1] CREATE (a)-[:" + graph.edgeLabel() + "]->(b)", "x agtype");
    }

    /** Shards by {@link GraphStore#byKey} on the codec's key, as {@link AgeGraphStore#byKey} reads. */
    public static <T> AgeGraphLoader<T> byKey(PgConnections connections, AgeGraph graph, NodeCodec<T> codec,
                                              int shards) {
        return new AgeGraphLoader<>(connections, graph, codec, GraphStore.byKey(codec::encode, shards));
    }

    /** Creates the graph, its two labels and their indexes, each unless it exists. */
    public AgeGraphLoader<T> create() {
        connections.transaction(connection -> {
            try (Statement statement = connection.createStatement()) {
                statement.execute("SELECT pg_advisory_xact_lock(hashtext('graphexecutor-age:" + graph.name() + "'))");
            }
            if (absent(connection, "SELECT 1 FROM ag_catalog.ag_graph WHERE name = ?", graph.name())) {
                select(connection, "SELECT ag_catalog.create_graph(?::name)", graph.name());
            }
            String label = "SELECT 1 FROM ag_catalog.ag_label l JOIN ag_catalog.ag_graph g ON l.graph = g.graphid"
                    + " WHERE g.name = ? AND l.name = ?";
            if (absent(connection, label, graph.name(), graph.vertexLabel())) {
                select(connection, "SELECT ag_catalog.create_vlabel(?::name, ?::name)", graph.name(), graph.vertexLabel());
            }
            if (absent(connection, label, graph.name(), graph.edgeLabel())) {
                select(connection, "SELECT ag_catalog.create_elabel(?::name, ?::name)", graph.name(), graph.edgeLabel());
            }
            String vertices = graph.table(graph.vertexLabel());
            String edges = graph.table(graph.edgeLabel());
            String key = "ag_catalog.agtype_access_operator(VARIADIC ARRAY[properties, '\"key\"'::ag_catalog.agtype])";
            String shard = "ag_catalog.agtype_access_operator(VARIADIC ARRAY[properties, '\"shard\"'::ag_catalog.agtype])";
            String v = graph.vertexLabel();
            String e = graph.edgeLabel();
            try (Statement statement = connection.createStatement()) {
                // id: the joins from edges to vertices; key: edges of a batch; (shard, key): pages of a shard
                statement.execute("CREATE INDEX IF NOT EXISTS " + v + "_id ON " + vertices + " (id)");
                statement.execute("CREATE INDEX IF NOT EXISTS " + v + "_key ON " + vertices + " (" + key + ")");
                statement.execute("CREATE INDEX IF NOT EXISTS " + v + "_shard_key ON " + vertices
                        + " (" + shard + ", " + key + ")");
                statement.execute("CREATE INDEX IF NOT EXISTS " + e + "_start ON " + edges + " (start_id)");
                statement.execute("CREATE INDEX IF NOT EXISTS " + e + "_end ON " + edges + " (end_id)");
            }
            return null;
        });
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
        analyze();
    }

    public void addNodes(Collection<? extends T> nodes) {
        if (nodes.isEmpty()) {
            return;
        }
        List<Map<String, Object>> rows = new ArrayList<>();
        for (T node : nodes) {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("key", codec.encode(node));
            row.put("shard", shardOf.applyAsInt(node));
            rows.add(row);
        }
        run(createNodes, Map.of("rows", rows));
    }

    /** Adds an edge per entry, from key to value. Both nodes must have been added. */
    public void addEdges(Collection<? extends Map.Entry<? extends T, ? extends T>> edges) {
        if (edges.isEmpty()) {
            return;
        }
        List<List<String>> pairs = new ArrayList<>();
        for (Map.Entry<? extends T, ? extends T> edge : edges) {
            pairs.add(List.of(codec.encode(edge.getKey()), codec.encode(edge.getValue())));
        }
        run(createEdges, Map.of("edges", pairs));
    }

    /** Refreshes the planner's statistics, so the store's queries use the indexes. Run after a load. */
    public void analyze() {
        connections.call(connection -> {
            try (Statement statement = connection.createStatement()) {
                statement.execute("ANALYZE " + graph.table(graph.vertexLabel()));
                statement.execute("ANALYZE " + graph.table(graph.edgeLabel()));
            }
            return null;
        });
    }

    private void run(String sql, Map<String, ?> params) {
        connections.call(connection -> {
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setObject(1, Agtype.params(params));
                statement.execute();
            }
            return null;
        });
    }

    /** Whether {@code sql} returns no row. */
    private static boolean absent(Connection connection, String sql, String... args) throws SQLException {
        try (PreparedStatement statement = prepare(connection, sql, args); ResultSet rows = statement.executeQuery()) {
            return !rows.next();
        }
    }

    private static void select(Connection connection, String sql, String... args) throws SQLException {
        try (PreparedStatement statement = prepare(connection, sql, args)) {
            statement.execute();
        }
    }

    private static PreparedStatement prepare(Connection connection, String sql, String... args) throws SQLException {
        PreparedStatement statement = connection.prepareStatement(sql);
        for (int i = 0; i < args.length; i++) {
            statement.setString(i + 1, args[i]);
        }
        return statement;
    }
}
