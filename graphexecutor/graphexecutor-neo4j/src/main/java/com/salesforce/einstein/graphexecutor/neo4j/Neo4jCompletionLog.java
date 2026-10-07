package com.salesforce.einstein.graphexecutor.neo4j;

import com.salesforce.einstein.graphexecutor.spi.CompletionLog;
import com.salesforce.einstein.graphexecutor.spi.NodeCodec;
import org.neo4j.driver.Record;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * A {@link CompletionLog} as Neo4j nodes, one per task of a run: {@code (:Outcome {run, key, round, status,
 * error})}, unique on {@code (run, key)}. Writes are {@code MERGE … ON CREATE SET}: with the constraint, MERGE
 * locks, so of two workers racing on a task exactly one creates it and the first outcome wins, as the log's
 * rules require. Batched reads and writes are one {@code UNWIND} query each.
 *
 * @param <T> node type
 */
public final class Neo4jCompletionLog<T> implements CompletionLog<T> {

    private final Neo4jSessions sessions;
    private final String runId;
    private final NodeCodec<T> codec;

    public Neo4jCompletionLog(Neo4jSessions sessions, String runId, NodeCodec<T> codec) {
        this.sessions = Objects.requireNonNull(sessions, "sessions");
        this.runId = Objects.requireNonNull(runId, "runId");
        this.codec = Objects.requireNonNull(codec, "codec");
        sessions.schema("CREATE CONSTRAINT outcome_key IF NOT EXISTS FOR (o:Outcome) REQUIRE (o.run, o.key) IS UNIQUE");
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
        List<Map<String, Object>> rows = new ArrayList<>();
        outcomes.forEach((task, outcome) -> {
            Map<String, Object> row = new HashMap<>();
            row.put("key", codec.encode(task));
            row.put("round", outcome.round());
            row.put("status", outcome.status().name());
            row.put("error", outcome.error());
            rows.add(row);
        });
        sessions.write(tx -> tx.run("""
                UNWIND $rows AS r
                MERGE (o:Outcome {run: $run, key: r.key})
                ON CREATE SET o.round = r.round, o.status = r.status, o.error = r.error""",
                Map.of("run", runId, "rows", rows)).consume());
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
        return sessions.read(tx -> {
            Map<T, Outcome> outcomes = new HashMap<>();
            for (Record row : tx.run("""
                    UNWIND $keys AS k MATCH (o:Outcome {run: $run, key: k})
                    RETURN o.key, o.round, o.status, o.error""",
                    Map.of("run", runId, "keys", List.copyOf(byKey.keySet()))).list()) {
                outcomes.put(byKey.get(row.get(0).asString()), outcome(row));
            }
            return outcomes;
        });
    }

    /** How many tasks of the run have an outcome. */
    public long size() {
        return sessions.read(tx -> tx.run("MATCH (o:Outcome {run: $run}) RETURN count(o)", Map.of("run", runId))
                .single().get(0).asLong());
    }

    /** Every outcome of the run, by node key. O(tasks): for tests and small runs. */
    public Map<String, Outcome> all() {
        return sessions.read(tx -> {
            Map<String, Outcome> outcomes = new HashMap<>();
            for (Record row : tx.run("MATCH (o:Outcome {run: $run}) RETURN o.key, o.round, o.status, o.error",
                    Map.of("run", runId)).list()) {
                outcomes.put(row.get(0).asString(), outcome(row));
            }
            return outcomes;
        });
    }

    private static Outcome outcome(Record row) {
        return new Outcome(Status.valueOf(row.get(2).asString()), row.get(1).asInt(),
                row.get(3).isNull() ? null : row.get(3).asString());
    }
}
