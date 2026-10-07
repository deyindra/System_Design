package com.salesforce.einstein.graphexecutor.age;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;

/**
 * A small pool of PostgreSQL connections, so the stores need nothing beyond the JDBC driver. At most
 * {@code max} connections are open; a caller waits for one when all are in use. Every new connection first
 * runs the {@code init} statements (for AGE: {@code LOAD 'age'} and its search path).
 *
 * <p>A connection whose call threw a {@link SQLException} is closed, not reused: after a network error it
 * may be broken, and a new one costs only a reconnect.
 */
public final class PgConnections implements AutoCloseable {

    /** A unit of work on one connection. */
    @FunctionalInterface
    public interface Work<R> {
        R apply(Connection connection) throws SQLException;
    }

    /** {@code LOAD 'age'} and a search path with {@code ag_catalog}: what every AGE session needs before Cipher. */
    public static final List<String> AGE = List.of("LOAD 'age'", "SET search_path = ag_catalog, \"$user\", public");

    private final String url;
    private final Properties properties = new Properties();
    private final List<String> init;
    private final Semaphore permits;
    private final ConcurrentLinkedQueue<Connection> idle = new ConcurrentLinkedQueue<>();

    public PgConnections(String url, String user, String password, int max, List<String> init) {
        this.url = Objects.requireNonNull(url, "url");
        properties.setProperty("user", Objects.requireNonNull(user, "user"));
        properties.setProperty("password", Objects.requireNonNull(password, "password"));
        if (max < 1) {
            throw new IllegalArgumentException("max must be >= 1");
        }
        this.permits = new Semaphore(max);
        this.init = List.copyOf(init);
    }

    /** Runs {@code work} on a pooled connection in auto-commit mode. */
    public <R> R call(Work<R> work) {
        return borrow(work, false);
    }

    /** Runs {@code work} in one transaction: committed if it returns, rolled back if it throws. */
    public <R> R transaction(Work<R> work) {
        return borrow(work, true);
    }

    private <R> R borrow(Work<R> work, boolean transaction) {
        permits.acquireUninterruptibly();
        Connection connection = null;
        try {
            connection = idle.poll();
            if (connection == null) {
                connection = open();
            }
            R result;
            if (transaction) {
                connection.setAutoCommit(false);
                try {
                    result = work.apply(connection);
                    connection.commit();
                } catch (SQLException | RuntimeException e) {
                    connection.rollback();
                    throw e;
                } finally {
                    connection.setAutoCommit(true);
                }
            } else {
                result = work.apply(connection);
            }
            idle.add(connection);
            connection = null;
            return result;
        } catch (SQLException e) {
            throw new UncheckedSqlException(e);
        } finally {
            closeQuietly(connection);   // non-null only if the work failed
            permits.release();
        }
    }

    private Connection open() throws SQLException {
        Connection connection = DriverManager.getConnection(url, properties);
        try (Statement statement = connection.createStatement()) {
            for (String sql : init) {
                statement.execute(sql);
            }
        } catch (SQLException e) {
            closeQuietly(connection);
            throw e;
        }
        return connection;
    }

    private static void closeQuietly(Connection connection) {
        if (connection == null) {
            return;
        }
        try {
            connection.close();
        } catch (SQLException ignored) {
            // it was being discarded anyway
        }
    }

    @Override
    public void close() {
        for (Connection connection = idle.poll(); connection != null; connection = idle.poll()) {
            closeQuietly(connection);
        }
    }
}
