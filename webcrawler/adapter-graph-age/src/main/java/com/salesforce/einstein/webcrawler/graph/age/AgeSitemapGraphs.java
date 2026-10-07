package com.salesforce.einstein.webcrawler.graph.age;

import com.salesforce.einstein.ds.graph.Graph;
import com.salesforce.einstein.graphexecutor.age.AgeGraph;
import com.salesforce.einstein.graphexecutor.age.AgeGraphLoader;
import com.salesforce.einstein.graphexecutor.age.AgeGraphStore;
import com.salesforce.einstein.graphexecutor.age.PgConnections;
import com.salesforce.einstein.graphexecutor.spi.GraphStore;
import com.salesforce.einstein.webcrawler.model.CanonicalUrl;
import com.salesforce.einstein.webcrawler.sitemap.CanonicalUrlCodec;
import com.salesforce.einstein.webcrawler.sitemap.SitemapGraphRecord;
import com.salesforce.einstein.webcrawler.sitemap.SitemapGraphs;

import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * {@link SitemapGraphs} in Apache AGE: each sitemap is an AGE graph of its own name, with vertices labelled
 * {@code V} and edges {@code E} ({@link AgeGraph#named}), written by {@link AgeGraphLoader} and read by
 * {@link AgeGraphStore}. A bulk import writes {@code V} rows with {@code {"key": <url>, "shard": <shard>}} properties
 * into {@code "<name>"."V"}, and {@code E} rows between them into {@code "<name>"."E"}.
 *
 * <p>The catalog is the table {@value #CATALOG}, created if missing.
 */
public final class AgeSitemapGraphs implements SitemapGraphs {

    static final String CATALOG = "public.webcrawler_sitemap_graphs";
    private static final String COLUMNS = "name, load_id, tenant_id, sitemap_url, status, pages, edges, roots, "
            + "root_count, error, created_at, finished_at";

    private final PgConnections connections;
    private final boolean ownsConnections;
    private final int shards;
    private final int batchSize;
    private final Map<String, Opened> open = new ConcurrentHashMap<>();

    /** An opened graph's store, which holds nothing to release; in a record, so the map's values are not resources. */
    private record Opened(GraphStore<CanonicalUrl> store) { }

    /** @param connections connections that ran {@link PgConnections#AGE}; the caller closes them */
    public AgeSitemapGraphs(PgConnections connections, int shards, int batchSize) {
        this(connections, false, shards, batchSize);
    }

    private AgeSitemapGraphs(PgConnections connections, boolean ownsConnections, int shards, int batchSize) {
        if (shards < 1 || batchSize < 1) throw new IllegalArgumentException("shards and batchSize must be >= 1");
        this.connections = connections;
        this.ownsConnections = ownsConnections;
        this.shards = shards;
        this.batchSize = batchSize;
        connections.transaction(c -> {
            try (var s = c.createStatement()) {           // the lock: nodes starting together create it once
                s.execute("SELECT pg_advisory_xact_lock(hashtext('" + CATALOG + "'))");
                s.execute("CREATE TABLE IF NOT EXISTS " + CATALOG + " (name text PRIMARY KEY, load_id text NOT NULL, "
                        + "tenant_id text NOT NULL, sitemap_url text NOT NULL, status text NOT NULL, "
                        + "pages bigint NOT NULL, edges bigint NOT NULL, roots text[] NOT NULL, root_count int NOT NULL, "
                        + "error text, created_at timestamptz NOT NULL, finished_at timestamptz)");
            }
            return null;
        });
    }

    /** With a pool of its own, closed by {@link #close}. */
    public static AgeSitemapGraphs connect(AgeSitemapSettings s, int shards) {
        return new AgeSitemapGraphs(new PgConnections(s.url(), s.user(), s.password(), s.poolSize(), PgConnections.AGE),
                true, shards, s.batchSize());
    }

    @Override
    public GraphStore<CanonicalUrl> open(String name) {
        return open.computeIfAbsent(name, n -> new Opened(new AgeGraphStore<>(connections, AgeGraph.named(n),
                CanonicalUrlCodec.INSTANCE, shards, CanonicalUrlCodec.shardByHost(shards)))).store();
    }

    @Override
    public boolean exists(String name) {
        AgeGraph.named(name);                                            // an identifier, or no such graph
        return connections.call(c -> exists(c, name));
    }

    @Override
    public void load(String name, Graph<CanonicalUrl> graph) {
        if (exists(name)) throw new IllegalStateException("sitemap graph exists: " + name);
        new AgeGraphLoader<>(connections, AgeGraph.named(name), CanonicalUrlCodec.INSTANCE,
                CanonicalUrlCodec.shardByHost(shards)).create().load(graph, batchSize);
    }

    @Override
    public void drop(String name) {
        AgeGraph.named(name);
        open.remove(name);
        connections.transaction(c -> {
            if (exists(c, name)) {
                try (PreparedStatement s = c.prepareStatement("SELECT ag_catalog.drop_graph(?::name, true)")) {
                    s.setString(1, name);
                    s.execute();
                }
            }
            return null;
        });
    }

    @Override
    public boolean register(SitemapGraphRecord entry) {
        return connections.call(c -> {
            try (PreparedStatement s = c.prepareStatement("INSERT INTO " + CATALOG + " (" + COLUMNS + ") "
                    + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (name) DO NOTHING")) {
                bind(c, s, entry);
                return s.executeUpdate() == 1;
            }
        });
    }

    @Override
    public Optional<SitemapGraphRecord> record(String name) {
        return connections.call(c -> {
            try (PreparedStatement s = c.prepareStatement("SELECT " + COLUMNS + " FROM " + CATALOG + " WHERE name = ?")) {
                s.setString(1, name);
                try (ResultSet rows = s.executeQuery()) {
                    return rows.next() ? Optional.of(read(rows)) : Optional.empty();
                }
            }
        });
    }

    @Override
    public boolean update(SitemapGraphRecord entry) {
        return connections.call(c -> {
            try (PreparedStatement s = c.prepareStatement("UPDATE " + CATALOG + " SET (" + COLUMNS + ") = "
                    + "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) WHERE name = ? AND load_id = ?")) {
                bind(c, s, entry);
                s.setString(13, entry.name());
                s.setString(14, entry.loadId());
                return s.executeUpdate() == 1;
            }
        });
    }

    @Override
    public void unregister(String name) {
        connections.call(c -> {
            try (PreparedStatement s = c.prepareStatement("DELETE FROM " + CATALOG + " WHERE name = ?")) {
                s.setString(1, name);
                return s.executeUpdate();
            }
        });
    }

    @Override
    public void close() {
        if (ownsConnections) connections.close();
    }

    private static void bind(Connection c, PreparedStatement s, SitemapGraphRecord e) throws SQLException {
        s.setString(1, e.name());
        s.setString(2, e.loadId());
        s.setString(3, e.tenantId());
        s.setString(4, e.sitemapUrl());
        s.setString(5, e.status().name());
        s.setLong(6, e.pages());
        s.setLong(7, e.edges());
        s.setArray(8, c.createArrayOf("text", e.roots().toArray()));
        s.setInt(9, e.rootCount());
        s.setString(10, e.error());
        s.setTimestamp(11, Timestamp.from(e.createdAt()));
        s.setTimestamp(12, e.finishedAt() == null ? null : Timestamp.from(e.finishedAt()));
    }

    private static SitemapGraphRecord read(ResultSet r) throws SQLException {
        Array roots = r.getArray("roots");
        Timestamp finished = r.getTimestamp("finished_at");
        return new SitemapGraphRecord(r.getString("name"), r.getString("load_id"), r.getString("tenant_id"),
                r.getString("sitemap_url"), SitemapGraphRecord.Status.valueOf(r.getString("status")), r.getLong("pages"),
                r.getLong("edges"), Arrays.stream((Object[]) roots.getArray()).map(String.class::cast).toList(),
                r.getInt("root_count"), r.getString("error"), r.getTimestamp("created_at").toInstant(),
                finished == null ? null : finished.toInstant());
    }

    private static boolean exists(Connection c, String name) throws SQLException {
        try (PreparedStatement s = c.prepareStatement("SELECT 1 FROM ag_catalog.ag_graph WHERE name = ?")) {
            s.setString(1, name);
            try (ResultSet rows = s.executeQuery()) {
                return rows.next();
            }
        }
    }
}
