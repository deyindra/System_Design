package com.salesforce.einstein.hierarchy;

import com.salesforce.einstein.hierarchy.store.JdbcSchema;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.testcontainers.containers.PostgreSQLContainer;

import javax.sql.DataSource;

/**
 * One Postgres 16 for every IT in the JVM, migrated once. Tests isolate by tenant (a random UUID each), the same way
 * production isolates tenants that share a shard.
 */
public final class Pg {
    /** Never closed: it lives as long as the JVM, and Testcontainers removes it at exit. */
    private static PostgreSQLContainer<?> container;
    private static Connection connection;
    private static DataSource dataSource;

    private record Connection(String jdbcUrl, String username, String password) {
    }

    private Pg() {
    }

    public static String jdbcUrl() {
        return connection().jdbcUrl();
    }

    public static String username() {
        return connection().username();
    }

    public static String password() {
        return connection().password();
    }

    private static synchronized Connection connection() {
        if (connection == null) {
            container = new PostgreSQLContainer<>("postgres:16-alpine");
            container.setCommand("postgres", "-c", "max_connections=200");
            container.start();
            connection = new Connection(container.getJdbcUrl(), container.getUsername(), container.getPassword());
        }
        return connection;
    }

    /** A shared pool; tests never close it (it goes when the JVM exits). */
    public static synchronized DataSource dataSource() {
        if (dataSource == null) {
            HikariConfig cfg = new HikariConfig();
            cfg.setPoolName("it");
            cfg.setJdbcUrl(jdbcUrl());
            cfg.setUsername(username());
            cfg.setPassword(password());
            cfg.setMaximumPoolSize(60);
            dataSource = new HikariDataSource(cfg);
            JdbcSchema.migrate(dataSource);
        }
        return dataSource;
    }
}
