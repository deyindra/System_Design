package com.salesforce.einstein.graphexecutor.age;

import com.salesforce.einstein.graphexecutor.spi.ClusterStore;
import org.checkerframework.checker.tainting.qual.Untainted;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

/**
 * A {@link ClusterStore} in PostgreSQL (the database AGE runs in), so the workers of a run share nothing but
 * the database. One table per kind of state, each keyed so the store's rules hold by construction:
 * <pre>cluster_run     (run_id)                          round, status, error
 * cluster_lease   (run_id, shard)                   owner, expires
 * cluster_attempt (run_id, round, shard)            starts
 * cluster_message (run_id, round, to_shard, from_shard)  nodes text[]
 * cluster_arrival (run_id, round, shard)            worker, attempt and the shard round's counts</pre>
 * <ul>
 *   <li><b>Store time:</b> every lease comparison is against the server's {@code now()}.</li>
 *   <li><b>Idempotent writes:</b> messages and arrivals are {@code INSERT … ON CONFLICT DO NOTHING}.</li>
 *   <li><b>Fenced arrivals:</b> {@link #arrive} locks the shard's lease row ({@code FOR UPDATE}). It inserts
 *       only if the worker holds that lease unexpired, so no claim can slip in between the check and the insert.</li>
 *   <li><b>Compare-and-set advance:</b> {@link #advance} locks the run's row. Of concurrent calls, exactly one
 *       finds the round complete and moves it.</li>
 * </ul>
 */
public final class PostgresClusterStore implements ClusterStore {

    private final PgConnections connections;

    public PostgresClusterStore(PgConnections connections) {
        this.connections = Objects.requireNonNull(connections, "connections");
        ddl(connections,
                """
                CREATE TABLE IF NOT EXISTS cluster_run (
                    run_id text PRIMARY KEY,
                    shards int  NOT NULL,
                    round  int  NOT NULL,
                    status text NOT NULL,
                    error  text)""",
                """
                CREATE TABLE IF NOT EXISTS cluster_lease (
                    run_id  text        NOT NULL,
                    shard   int         NOT NULL,
                    owner   text,
                    expires timestamptz NOT NULL,
                    PRIMARY KEY (run_id, shard))""",
                """
                CREATE TABLE IF NOT EXISTS cluster_attempt (
                    run_id text NOT NULL,
                    round  int  NOT NULL,
                    shard  int  NOT NULL,
                    starts int  NOT NULL,
                    PRIMARY KEY (run_id, round, shard))""",
                """
                CREATE TABLE IF NOT EXISTS cluster_message (
                    run_id     text   NOT NULL,
                    round      int    NOT NULL,
                    to_shard   int    NOT NULL,
                    from_shard int    NOT NULL,
                    nodes      text[] NOT NULL,
                    PRIMARY KEY (run_id, round, to_shard, from_shard))""",
                """
                CREATE TABLE IF NOT EXISTS cluster_arrival (
                    run_id  text   NOT NULL,
                    round   int    NOT NULL,
                    shard   int    NOT NULL,
                    worker  text   NOT NULL,
                    attempt int    NOT NULL,
                    done    bigint NOT NULL,
                    failed  bigint NOT NULL,
                    skipped bigint NOT NULL,
                    batches bigint NOT NULL,
                    edges   bigint NOT NULL,
                    PRIMARY KEY (run_id, round, shard))""");
    }

    /**
     * Runs {@code CREATE … IF NOT EXISTS} statements under an advisory lock: two workers starting at once would
     * otherwise race on the catalog and one would fail with a duplicate type.
     */
    static void ddl(PgConnections connections, @Untainted String... statements) {
        connections.transaction(connection -> {
            try (Statement statement = connection.createStatement()) {
                statement.execute("SELECT pg_advisory_xact_lock(hashtext('graphexecutor-ddl'))");
                for (String sql : statements) {
                    statement.execute(sql);
                }
            }
            return null;
        });
    }

    @Override
    public void createRun(String runId, int shards, Duration ttl) {
        connections.transaction(connection -> {
            try (PreparedStatement run = connection.prepareStatement(
                    "INSERT INTO cluster_run VALUES (?, ?, 0, 'RUNNING', NULL) ON CONFLICT DO NOTHING")) {
                run.setString(1, runId);
                run.setInt(2, shards);
                if (run.executeUpdate() == 0) {
                    return null;   // another worker created it
                }
            }
            try (PreparedStatement leases = connection.prepareStatement("""
                    INSERT INTO cluster_lease
                    SELECT ?, shard, NULL, now() + make_interval(secs => ?) FROM generate_series(0, ? - 1) shard""")) {
                leases.setString(1, runId);
                leases.setDouble(2, seconds(ttl));
                leases.setInt(3, shards);
                leases.executeUpdate();
            }
            return null;
        });
    }

    @Override
    public Optional<RunState> run(String runId) {
        return connections.call(connection -> {
            try (PreparedStatement select = connection.prepareStatement(
                    "SELECT shards, round, status, error FROM cluster_run WHERE run_id = ?")) {
                select.setString(1, runId);
                try (ResultSet rows = select.executeQuery()) {
                    return rows.next()
                            ? Optional.of(new RunState(rows.getInt(1), rows.getInt(2),
                                    RunStatus.valueOf(rows.getString(3)), rows.getString(4)))
                            : Optional.empty();
                }
            }
        });
    }

    @Override
    public List<Lease> leases(String runId) {
        return connections.call(connection -> {
            try (PreparedStatement select = connection.prepareStatement(
                    "SELECT shard, owner, expires <= now() FROM cluster_lease WHERE run_id = ? ORDER BY shard")) {
                select.setString(1, runId);
                List<Lease> leases = new ArrayList<>();
                try (ResultSet rows = select.executeQuery()) {
                    while (rows.next()) {
                        leases.add(new Lease(rows.getInt(1), rows.getString(2), rows.getBoolean(3)));
                    }
                }
                return leases;
            }
        });
    }

    @Override
    public boolean claim(String runId, int shard, String worker, Duration ttl) {
        return connections.call(connection -> {
            try (PreparedStatement update = connection.prepareStatement("""
                    UPDATE cluster_lease SET owner = ?, expires = now() + make_interval(secs => ?)
                    WHERE run_id = ? AND shard = ? AND (owner IS NULL OR owner = ? OR expires <= now())""")) {
                update.setString(1, worker);
                update.setDouble(2, seconds(ttl));
                update.setString(3, runId);
                update.setInt(4, shard);
                update.setString(5, worker);
                return update.executeUpdate() == 1;
            }
        });
    }

    @Override
    public void renew(String runId, String worker, Duration ttl) {
        connections.call(connection -> {
            try (PreparedStatement update = connection.prepareStatement("""
                    UPDATE cluster_lease SET expires = now() + make_interval(secs => ?)
                    WHERE run_id = ? AND owner = ? AND expires > now()""")) {
                update.setDouble(1, seconds(ttl));
                update.setString(2, runId);
                update.setString(3, worker);
                return update.executeUpdate();
            }
        });
    }

    @Override
    public int beginAttempt(String runId, int round, int shard) {
        return connections.call(connection -> {
            try (PreparedStatement upsert = connection.prepareStatement("""
                    INSERT INTO cluster_attempt VALUES (?, ?, ?, 1)
                    ON CONFLICT (run_id, round, shard) DO UPDATE SET starts = cluster_attempt.starts + 1
                    RETURNING starts - 1""")) {
                upsert.setString(1, runId);
                upsert.setInt(2, round);
                upsert.setInt(3, shard);
                try (ResultSet rows = upsert.executeQuery()) {
                    rows.next();
                    return rows.getInt(1);
                }
            }
        });
    }

    @Override
    public void send(String runId, int round, int from, int to, Collection<String> nodes) {
        connections.call(connection -> {
            try (PreparedStatement insert = connection.prepareStatement(
                    "INSERT INTO cluster_message VALUES (?, ?, ?, ?, ?) ON CONFLICT DO NOTHING")) {
                insert.setString(1, runId);
                insert.setInt(2, round);
                insert.setInt(3, to);
                insert.setInt(4, from);
                insert.setArray(5, connection.createArrayOf("text", nodes.toArray()));
                return insert.executeUpdate();
            }
        });
    }

    @Override
    public Set<String> inbox(String runId, int round, int to) {
        return connections.call(connection -> {
            try (PreparedStatement select = connection.prepareStatement(
                    "SELECT unnest(nodes) FROM cluster_message WHERE run_id = ? AND round = ? AND to_shard = ?")) {
                select.setString(1, runId);
                select.setInt(2, round);
                select.setInt(3, to);
                Set<String> inbox = new TreeSet<>();
                try (ResultSet rows = select.executeQuery()) {
                    while (rows.next()) {
                        inbox.add(rows.getString(1));
                    }
                }
                return inbox;
            }
        });
    }

    @Override
    public boolean arrive(String runId, Arrival arrival) {
        return connections.transaction(connection -> {
            try (PreparedStatement lease = connection.prepareStatement("""
                    SELECT 1 FROM cluster_lease
                    WHERE run_id = ? AND shard = ? AND owner = ? AND expires > now() FOR UPDATE""")) {
                lease.setString(1, runId);
                lease.setInt(2, arrival.shard());
                lease.setString(3, arrival.worker());
                try (ResultSet rows = lease.executeQuery()) {
                    if (!rows.next()) {
                        return false;   // fenced: the shard is someone else's now
                    }
                }
            }
            try (PreparedStatement insert = connection.prepareStatement(
                    "INSERT INTO cluster_arrival VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT DO NOTHING")) {
                insert.setString(1, runId);
                insert.setInt(2, arrival.round());
                insert.setInt(3, arrival.shard());
                insert.setString(4, arrival.worker());
                insert.setInt(5, arrival.attempt());
                insert.setLong(6, arrival.done());
                insert.setLong(7, arrival.failed());
                insert.setLong(8, arrival.skipped());
                insert.setLong(9, arrival.batches());
                insert.setLong(10, arrival.edges());
                insert.executeUpdate();
            }
            return true;
        });
    }

    @Override
    public Set<Integer> arrived(String runId, int round) {
        return connections.call(connection -> {
            try (PreparedStatement select = connection.prepareStatement(
                    "SELECT shard FROM cluster_arrival WHERE run_id = ? AND round = ?")) {
                select.setString(1, runId);
                select.setInt(2, round);
                Set<Integer> arrived = new HashSet<>();
                try (ResultSet rows = select.executeQuery()) {
                    while (rows.next()) {
                        arrived.add(rows.getInt(1));
                    }
                }
                return Set.copyOf(arrived);
            }
        });
    }

    @Override
    public void advance(String runId, int round) {
        connections.transaction(connection -> {
            int shards;
            try (PreparedStatement run = connection.prepareStatement(
                    "SELECT shards FROM cluster_run WHERE run_id = ? AND round = ? AND status = 'RUNNING' FOR UPDATE")) {
                run.setString(1, runId);
                run.setInt(2, round);
                try (ResultSet rows = run.executeQuery()) {
                    if (!rows.next()) {
                        return null;   // finished, or another worker moved it already
                    }
                    shards = rows.getInt(1);
                }
            }
            long arrived;
            long batches;
            try (PreparedStatement count = connection.prepareStatement(
                    "SELECT count(*), coalesce(sum(batches), 0) FROM cluster_arrival WHERE run_id = ? AND round = ?")) {
                count.setString(1, runId);
                count.setInt(2, round);
                try (ResultSet rows = count.executeQuery()) {
                    rows.next();
                    arrived = rows.getLong(1);
                    batches = rows.getLong(2);
                }
            }
            if (arrived < shards) {
                return null;
            }
            try (PreparedStatement update = connection.prepareStatement(batches == 0
                    ? "UPDATE cluster_run SET status = 'DONE' WHERE run_id = ?"
                    : "UPDATE cluster_run SET round = round + 1 WHERE run_id = ?")) {
                update.setString(1, runId);
                update.executeUpdate();
            }
            return null;
        });
    }

    @Override
    public void fail(String runId, String error) {
        connections.call(connection -> {
            try (PreparedStatement update = connection.prepareStatement(
                    "UPDATE cluster_run SET status = 'FAILED', error = ? WHERE run_id = ? AND status = 'RUNNING'")) {
                update.setString(1, error);
                update.setString(2, runId);
                return update.executeUpdate();
            }
        });
    }

    @Override
    public List<Arrival> arrivals(String runId) {
        return connections.call(connection -> {
            try (PreparedStatement select = connection.prepareStatement("""
                    SELECT round, shard, worker, attempt, done, failed, skipped, batches, edges
                    FROM cluster_arrival WHERE run_id = ? ORDER BY round, shard""")) {
                select.setString(1, runId);
                List<Arrival> arrivals = new ArrayList<>();
                try (ResultSet rows = select.executeQuery()) {
                    while (rows.next()) {
                        arrivals.add(arrival(rows));
                    }
                }
                return arrivals;
            }
        });
    }

    private static Arrival arrival(ResultSet rows) throws SQLException {
        return new Arrival(rows.getInt(1), rows.getInt(2), rows.getString(3), rows.getInt(4), rows.getLong(5),
                rows.getLong(6), rows.getLong(7), rows.getLong(8), rows.getLong(9));
    }

    private static double seconds(Duration duration) {
        return duration.toNanos() / 1e9;
    }

    /** Leaves the connections open: they are the caller's. */
    @Override
    public void close() {
    }
}
