package com.salesforce.einstein.graphexecutor.age.demo;

import com.salesforce.einstein.graphexecutor.age.PgConnections;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * The demo task: write a row to {@code task_run(run_id, node_key, worker)}, then take {@code taskTime}. The
 * rows count every run of every task, whichever container ran it, so a test can check that a task logged
 * before a crash did not run again.
 */
public final class TaskRuns {

    private final PgConnections connections;
    private final String runId;
    private final String worker;
    private final Duration taskTime;

    public TaskRuns(PgConnections connections, String runId, String worker, Duration taskTime) {
        this.connections = Objects.requireNonNull(connections, "connections");
        this.runId = Objects.requireNonNull(runId, "runId");
        this.worker = Objects.requireNonNull(worker, "worker");
        this.taskTime = Objects.requireNonNull(taskTime, "taskTime");
        connections.transaction(connection -> {
            try (Statement statement = connection.createStatement()) {
                statement.execute("SELECT pg_advisory_xact_lock(hashtext('graphexecutor-ddl'))");
                statement.execute("""
                        CREATE TABLE IF NOT EXISTS task_run (
                            run_id   text NOT NULL,
                            node_key text NOT NULL,
                            worker   text NOT NULL,
                            at       timestamptz NOT NULL DEFAULT clock_timestamp())""");
            }
            return null;
        });
    }

    /** Runs the task of {@code node}. */
    public void run(String node) throws InterruptedException {
        connections.call(connection -> {
            try (PreparedStatement insert = connection.prepareStatement(
                    "INSERT INTO task_run (run_id, node_key, worker) VALUES (?, ?, ?)")) {
                insert.setString(1, runId);
                insert.setString(2, node);
                insert.setString(3, worker);
                return insert.executeUpdate();
            }
        });
        Thread.sleep(taskTime.toMillis());
    }

    /** How many times each node's task ran in the run. */
    public static Map<String, Integer> counts(PgConnections connections, String runId) {
        return connections.call(connection -> {
            try (PreparedStatement select = connection.prepareStatement(
                    "SELECT node_key, count(*) FROM task_run WHERE run_id = ? GROUP BY node_key")) {
                select.setString(1, runId);
                Map<String, Integer> counts = new HashMap<>();
                try (ResultSet rows = select.executeQuery()) {
                    while (rows.next()) {
                        counts.put(rows.getString(1), rows.getInt(2));
                    }
                }
                return counts;
            }
        });
    }

    /** The nodes whose task {@code worker} started. */
    public static Set<String> ranBy(PgConnections connections, String runId, String worker) {
        return connections.call(connection -> {
            try (PreparedStatement select = connection.prepareStatement(
                    "SELECT node_key FROM task_run WHERE run_id = ? AND worker = ?")) {
                select.setString(1, runId);
                select.setString(2, worker);
                Set<String> nodes = new HashSet<>();
                try (ResultSet rows = select.executeQuery()) {
                    while (rows.next()) {
                        nodes.add(rows.getString(1));
                    }
                }
                return nodes;
            }
        });
    }
}
