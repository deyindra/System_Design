package com.salesforce.einstein.graphexecutor.age;

import com.salesforce.einstein.graphexecutor.spi.CompletionLog;
import com.salesforce.einstein.graphexecutor.spi.NodeCodec;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * A {@link CompletionLog} in a PostgreSQL table, one row per task of a run: {@code completion_log(run_id,
 * node_key, round, status, error)} with key {@code (run_id, node_key)}. Writes are {@code INSERT … ON CONFLICT
 * DO NOTHING}, so the first outcome wins, as the log's rules require, even when two workers race on a task.
 * Batched reads and writes are one statement each (arrays, unnested by the server).
 *
 * @param <T> node type
 */
public final class PostgresCompletionLog<T> implements CompletionLog<T> {

    private final PgConnections connections;
    private final String runId;
    private final NodeCodec<T> codec;

    public PostgresCompletionLog(PgConnections connections, String runId, NodeCodec<T> codec) {
        this.connections = Objects.requireNonNull(connections, "connections");
        this.runId = Objects.requireNonNull(runId, "runId");
        this.codec = Objects.requireNonNull(codec, "codec");
        createTable(connections);
    }

    static void createTable(PgConnections connections) {
        PostgresClusterStore.ddl(connections, """
                CREATE TABLE IF NOT EXISTS completion_log (
                    run_id   text NOT NULL,
                    node_key text NOT NULL,
                    round    int  NOT NULL,
                    status   text NOT NULL,
                    error    text,
                    PRIMARY KEY (run_id, node_key))""");
    }

    @Override
    public void record(T task, Outcome outcome) {
        recordAll(Map.of(task, outcome));
    }

    @Override
    public void recordAll(Map<? extends T, Outcome> outcomes) {
        if (outcomes.isEmpty()) {
            return;
        }
        List<String> keys = new ArrayList<>();
        List<Integer> rounds = new ArrayList<>();
        List<String> statuses = new ArrayList<>();
        List<String> errors = new ArrayList<>();
        outcomes.forEach((task, outcome) -> {
            keys.add(codec.encode(task));
            rounds.add(outcome.round());
            statuses.add(outcome.status().name());
            errors.add(outcome.error());
        });
        connections.call(connection -> {
            try (PreparedStatement insert = connection.prepareStatement("""
                    INSERT INTO completion_log (run_id, node_key, round, status, error)
                    SELECT ?, * FROM unnest(?::text[], ?::int[], ?::text[], ?::text[])
                    ON CONFLICT DO NOTHING""")) {
                insert.setString(1, runId);
                insert.setArray(2, connection.createArrayOf("text", keys.toArray()));
                insert.setArray(3, connection.createArrayOf("int4", rounds.toArray()));
                insert.setArray(4, connection.createArrayOf("text", statuses.toArray()));
                insert.setArray(5, connection.createArrayOf("text", errors.toArray()));
                insert.executeUpdate();
            }
            return null;
        });
    }

    @Override
    public Optional<Outcome> outcome(T task) {
        return Optional.ofNullable(outcomes(List.of(task)).get(task));
    }

    @Override
    public Map<T, Outcome> outcomes(Collection<? extends T> tasks) {
        if (tasks.isEmpty()) {
            return Map.of();
        }
        Map<String, T> byKey = new HashMap<>();
        for (T task : tasks) {
            byKey.put(codec.encode(task), task);
        }
        return connections.call(connection -> {
            try (PreparedStatement select = connection.prepareStatement(
                    "SELECT node_key, round, status, error FROM completion_log WHERE run_id = ? AND node_key = ANY(?)")) {
                select.setString(1, runId);
                select.setArray(2, connection.createArrayOf("text", byKey.keySet().toArray()));
                Map<T, Outcome> outcomes = new HashMap<>();
                try (ResultSet rows = select.executeQuery()) {
                    while (rows.next()) {
                        outcomes.put(byKey.get(rows.getString(1)),
                                new Outcome(Status.valueOf(rows.getString(3)), rows.getInt(2), rows.getString(4)));
                    }
                }
                return outcomes;
            }
        });
    }

    /** How many tasks of the run have an outcome. */
    public long size() {
        return connections.call(connection -> {
            try (PreparedStatement count = connection.prepareStatement(
                    "SELECT count(*) FROM completion_log WHERE run_id = ?")) {
                count.setString(1, runId);
                try (ResultSet rows = count.executeQuery()) {
                    rows.next();
                    return rows.getLong(1);
                }
            }
        });
    }

    /** Every outcome of the run, by node key. O(tasks): for tests and small runs. */
    public Map<String, Outcome> all() {
        return connections.call(connection -> {
            try (PreparedStatement select = connection.prepareStatement(
                    "SELECT node_key, round, status, error FROM completion_log WHERE run_id = ?")) {
                select.setString(1, runId);
                Map<String, Outcome> outcomes = new HashMap<>();
                try (ResultSet rows = select.executeQuery()) {
                    while (rows.next()) {
                        outcomes.put(rows.getString(1),
                                new Outcome(Status.valueOf(rows.getString(3)), rows.getInt(2), rows.getString(4)));
                    }
                }
                return outcomes;
            }
        });
    }
}
