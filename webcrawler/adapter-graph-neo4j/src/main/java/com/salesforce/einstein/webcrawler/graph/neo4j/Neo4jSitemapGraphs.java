package com.salesforce.einstein.webcrawler.graph.neo4j;

import com.salesforce.einstein.ds.graph.Graph;
import com.salesforce.einstein.graphexecutor.neo4j.Neo4jGraph;
import com.salesforce.einstein.graphexecutor.neo4j.Neo4jGraphLoader;
import com.salesforce.einstein.graphexecutor.neo4j.Neo4jGraphStore;
import com.salesforce.einstein.graphexecutor.neo4j.Neo4jSessions;
import com.salesforce.einstein.graphexecutor.spi.GraphStore;
import com.salesforce.einstein.webcrawler.model.CanonicalUrl;
import com.salesforce.einstein.webcrawler.sitemap.CanonicalUrlCodec;
import com.salesforce.einstein.webcrawler.sitemap.SitemapGraphRecord;
import com.salesforce.einstein.webcrawler.sitemap.SitemapGraphs;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Value;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * {@link SitemapGraphs} in Neo4j: each sitemap is the nodes labelled with its name, related by {@code <name>_EDGE}
 * ({@link Neo4jGraph#named}), written by {@link Neo4jGraphLoader} and read by {@link Neo4jGraphStore}. A bulk
 * import ({@code neo4j-admin database import}) writes nodes with {@code key} and {@code shard} properties, then
 * creates the constraint {@code <name>_key} on {@code key} and the index {@code <name>_shard_key} on
 * {@code (shard, key)}.
 *
 * <p>The catalog is a node per graph labelled {@value #CATALOG}, which no graph's label can be, as it is not an
 * identifier; instants are epoch milliseconds.
 */
public final class Neo4jSitemapGraphs implements SitemapGraphs {

    /** Nodes deleted per transaction by {@link #drop}, so a large graph does not need one huge transaction. */
    private static final int DROP_BATCH = 10_000;

    static final String CATALOG = "`webcrawler-sitemap-graph`";

    private final Neo4jSessions sessions;
    private final Driver owned;
    private final int shards;
    private final int batchSize;
    private final Map<String, Opened> open = new ConcurrentHashMap<>();

    /** An opened graph's store, which holds nothing to release; in a record, so the map's values are not resources. */
    private record Opened(GraphStore<CanonicalUrl> store) { }

    /** @param sessions sessions on a driver the caller closes */
    public Neo4jSitemapGraphs(Neo4jSessions sessions, int shards, int batchSize) {
        this(sessions, null, shards, batchSize);
    }

    private Neo4jSitemapGraphs(Neo4jSessions sessions, Driver owned, int shards, int batchSize) {
        if (shards < 1 || batchSize < 1) throw new IllegalArgumentException("shards and batchSize must be >= 1");
        this.sessions = sessions;
        this.owned = owned;
        this.shards = shards;
        this.batchSize = batchSize;
        sessions.schema("CREATE CONSTRAINT webcrawler_sitemap_graph_name IF NOT EXISTS FOR (g:" + CATALOG
                + ") REQUIRE g.name IS UNIQUE");
    }

    /** With a driver of its own, closed by {@link #close}. */
    public static Neo4jSitemapGraphs connect(Neo4jSitemapSettings s, int shards) {
        Driver driver = GraphDatabase.driver(s.uri(), AuthTokens.basic(s.user(), s.password()));
        return new Neo4jSitemapGraphs(new Neo4jSessions(driver, s.database()), driver, shards, s.batchSize());
    }

    @Override
    public GraphStore<CanonicalUrl> open(String name) {
        return open.computeIfAbsent(name, n -> new Opened(new Neo4jGraphStore<>(sessions, Neo4jGraph.named(n),
                CanonicalUrlCodec.INSTANCE, shards, CanonicalUrlCodec.shardByHost(shards)))).store();
    }

    @Override
    public boolean exists(String name) {
        String label = Neo4jGraph.named(name).nodeLabel();
        return sessions.read(tx -> tx.run("MATCH (n:" + label + ") RETURN 1 LIMIT 1").hasNext());
    }

    @Override
    public void load(String name, Graph<CanonicalUrl> graph) {
        if (exists(name)) throw new IllegalStateException("sitemap graph exists: " + name);
        new Neo4jGraphLoader<>(sessions, Neo4jGraph.named(name), CanonicalUrlCodec.INSTANCE,
                CanonicalUrlCodec.shardByHost(shards)).create().load(graph, batchSize);
    }

    @Override
    public void drop(String name) {
        String label = Neo4jGraph.named(name).nodeLabel();
        open.remove(name);
        String delete = "MATCH (n:" + label + ") WITH n LIMIT " + DROP_BATCH + " DETACH DELETE n RETURN count(*) AS deleted";
        long deleted;
        do {
            deleted = sessions.write(tx -> tx.run(delete).single().get("deleted").asLong());
        } while (deleted > 0);
        sessions.schema("DROP CONSTRAINT " + label + "_key IF EXISTS", "DROP INDEX " + label + "_shard_key IF EXISTS");
    }

    @Override
    public boolean register(SitemapGraphRecord entry) {
        return sessions.write(tx -> tx.run("MERGE (g:" + CATALOG + " {name: $name}) ON CREATE SET g = $props "
                        + "RETURN g.loadId = $loadId AS won",
                Map.of("name", entry.name(), "props", properties(entry), "loadId", entry.loadId()))
                .single().get("won").asBoolean());
    }

    @Override
    public Optional<SitemapGraphRecord> record(String name) {
        return sessions.read(tx -> {
            var rows = tx.run("MATCH (g:" + CATALOG + " {name: $name}) RETURN g", Map.of("name", name));
            return rows.hasNext() ? Optional.of(read(rows.single().get("g"))) : Optional.empty();
        });
    }

    @Override
    public boolean update(SitemapGraphRecord entry) {
        return sessions.write(tx -> tx.run("MATCH (g:" + CATALOG + " {name: $name}) WHERE g.loadId = $loadId "
                        + "SET g = $props RETURN count(g) AS updated",
                Map.of("name", entry.name(), "props", properties(entry), "loadId", entry.loadId()))
                .single().get("updated").asLong() == 1);
    }

    @Override
    public void unregister(String name) {
        sessions.write(tx -> tx.run("MATCH (g:" + CATALOG + " {name: $name}) DELETE g", Map.of("name", name)).consume());
    }

    @Override
    public void close() {
        if (owned != null) owned.close();
    }

    private static Map<String, Object> properties(SitemapGraphRecord e) {
        Map<String, Object> p = new HashMap<>();                    // nulls are absent properties
        p.put("name", e.name());
        p.put("loadId", e.loadId());
        p.put("tenantId", e.tenantId());
        p.put("sitemapUrl", e.sitemapUrl());
        p.put("status", e.status().name());
        p.put("pages", e.pages());
        p.put("edges", e.edges());
        p.put("roots", e.roots());
        p.put("rootCount", e.rootCount());
        p.put("error", e.error());
        p.put("createdAt", e.createdAt().toEpochMilli());
        p.put("finishedAt", e.finishedAt() == null ? null : e.finishedAt().toEpochMilli());
        return p;
    }

    private static SitemapGraphRecord read(Value g) {
        Value finished = g.get("finishedAt");
        return new SitemapGraphRecord(g.get("name").asString(), g.get("loadId").asString(), g.get("tenantId").asString(),
                g.get("sitemapUrl").asString(), SitemapGraphRecord.Status.valueOf(g.get("status").asString()),
                g.get("pages").asLong(), g.get("edges").asLong(), g.get("roots").asList(Value::asString),
                g.get("rootCount").asInt(), g.get("error").asString(null),
                Instant.ofEpochMilli(g.get("createdAt").asLong()),
                finished.isNull() ? null : Instant.ofEpochMilli(finished.asLong()));
    }
}
