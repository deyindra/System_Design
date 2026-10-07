package com.salesforce.einstein.graphexecutor.neo4j.demo;

import com.salesforce.einstein.graphexecutor.neo4j.Neo4jSessions;
import org.neo4j.driver.Record;

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * The demo task: create a {@code (:TaskRun {run, key, worker, at})} node, then take {@code taskTime}. The nodes
 * count every run of every task, whichever container ran it, so a test can check that a task logged before a
 * crash did not run again.
 */
public final class TaskRuns {

    private final Neo4jSessions sessions;
    private final String runId;
    private final String worker;
    private final Duration taskTime;

    public TaskRuns(Neo4jSessions sessions, String runId, String worker, Duration taskTime) {
        this.sessions = Objects.requireNonNull(sessions, "sessions");
        this.runId = Objects.requireNonNull(runId, "runId");
        this.worker = Objects.requireNonNull(worker, "worker");
        this.taskTime = Objects.requireNonNull(taskTime, "taskTime");
        sessions.schema("CREATE INDEX task_run IF NOT EXISTS FOR (n:TaskRun) ON (n.run, n.worker)");
    }

    /** Runs the task of {@code node}. */
    public void run(String node) throws InterruptedException {
        sessions.write(tx -> tx.run("CREATE (:TaskRun {run: $run, key: $key, worker: $worker, at: datetime()})",
                Map.of("run", runId, "key", node, "worker", worker)).consume());
        Thread.sleep(taskTime.toMillis());
    }

    /** How many times each node's task ran in the run. */
    public static Map<String, Integer> counts(Neo4jSessions sessions, String runId) {
        return sessions.read(tx -> {
            Map<String, Integer> counts = new HashMap<>();
            for (Record row : tx.run("MATCH (n:TaskRun {run: $run}) RETURN n.key, count(*)",
                    Map.of("run", runId)).list()) {
                counts.put(row.get(0).asString(), row.get(1).asInt());
            }
            return counts;
        });
    }

    /** The nodes whose task {@code worker} started. */
    public static Set<String> ranBy(Neo4jSessions sessions, String runId, String worker) {
        return sessions.read(tx -> {
            Set<String> nodes = new HashSet<>();
            for (Record row : tx.run("MATCH (n:TaskRun {run: $run, worker: $worker}) RETURN n.key",
                    Map.of("run", runId, "worker", worker)).list()) {
                nodes.add(row.get(0).asString());
            }
            return nodes;
        });
    }
}
