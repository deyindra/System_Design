package com.salesforce.einstein.webcrawler.adapter.rediscql;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/** Connections and schema for the three stores, so the auto-configuration and the tests build them one way. */
public final class RedisCqlSchema {

    private RedisCqlSchema() { }

    /**
     * Not bound to the keyspace (it may not exist yet); statements name it. LOCAL_QUORUM for reads and writes, so a
     * read sees every acknowledged write in the local data centre.
     */
    public static CqlSession cqlSession(RedisCqlSettings s) {
        List<InetSocketAddress> points = new ArrayList<>();
        for (String hp : s.cassandraContactPoints()) {
            int colon = hp.lastIndexOf(':');
            points.add(colon < 0 ? new InetSocketAddress(hp, 9042)
                    : new InetSocketAddress(hp.substring(0, colon), Integer.parseInt(hp.substring(colon + 1))));
        }
        return CqlSession.builder()
                .addContactPoints(points)
                .withLocalDatacenter(s.cassandraLocalDatacenter())
                .withConfigLoader(DriverConfigLoader.programmaticBuilder()
                        .withString(DefaultDriverOption.REQUEST_CONSISTENCY, "LOCAL_QUORUM")
                        .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(10))
                        .withDuration(DefaultDriverOption.METADATA_SCHEMA_REQUEST_TIMEOUT, Duration.ofSeconds(30))
                        .withDuration(DefaultDriverOption.CONNECTION_INIT_QUERY_TIMEOUT, Duration.ofSeconds(10))
                        .build())
                .build();
    }

    public static HikariDataSource dataSource(RedisCqlSettings s) {
        HikariConfig c = new HikariConfig();
        c.setJdbcUrl(s.jdbcUrl());
        c.setUsername(s.jdbcUser());
        c.setPassword(s.jdbcPassword());
        c.setMaximumPoolSize(s.jdbcPoolSize());
        c.setPoolName("webcrawler-jobs");
        return new HikariDataSource(c);
    }

    /** Idempotent: every statement is {@code IF NOT EXISTS}. */
    public static void create(CqlSession cql, String keyspace, String replication, DataSource db) {
        cql.execute("CREATE KEYSPACE IF NOT EXISTS " + keyspace + " WITH replication = " + replication);
        for (String stmt : statements(String.format(resource("cassandra.cql"), keyspace))) cql.execute(stmt);
        try (Connection c = db.getConnection(); Statement st = c.createStatement()) {
            for (String stmt : statements(resource("postgres.sql"))) st.execute(stmt);
        } catch (SQLException e) {
            throw new IllegalStateException("could not create crawl_jobs", e);
        }
    }

    /** Statements end with ';' at the end of a line; '--' lines are comments. */
    private static List<String> statements(String script) {
        List<String> out = new ArrayList<>();
        StringBuilder cur = new StringBuilder();
        for (String line : script.split("\n")) {
            String t = line.strip();
            if (t.isEmpty() || t.startsWith("--")) continue;
            cur.append(line).append('\n');
            if (t.endsWith(";")) {
                out.add(cur.substring(0, cur.lastIndexOf(";")).strip());
                cur.setLength(0);
            }
        }
        if (!cur.toString().isBlank()) out.add(cur.toString().strip());
        return out;
    }

    private static String resource(String name) {
        try (InputStream in = RedisCqlSchema.class.getResourceAsStream("/webcrawler/schema/" + name)) {
            if (in == null) throw new IllegalStateException("missing schema " + name);
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
