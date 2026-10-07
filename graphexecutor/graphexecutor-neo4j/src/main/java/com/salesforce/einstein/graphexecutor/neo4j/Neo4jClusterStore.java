package com.salesforce.einstein.graphexecutor.neo4j;

import com.salesforce.einstein.graphexecutor.spi.ClusterStore;
import org.neo4j.driver.Record;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

/**
 * A {@link ClusterStore} in Neo4j (the database the graph is in), so the workers of a run share nothing but
 * the database. One label per kind of state, each with a uniqueness constraint on its key:
 * <pre>ClusterRun     {run}                     shards, round, status, error
 * ClusterLease   {run, shard}              owner, expires
 * ClusterAttempt {run, round, shard}       starts
 * ClusterMessage {run, round, to, from}    nodes (a list of keys)
 * ClusterArrival {run, round, shard}       worker, attempt and the shard round's counts</pre>
 * <ul>
 *   <li><b>Store time:</b> every lease comparison is against the server's {@code datetime()};</li>
 *   <li><b>Idempotent writes:</b> messages and arrivals are {@code MERGE … ON CREATE SET}; with the
 *       constraint, of two racing MERGEs exactly one creates;</li>
 *   <li><b>Check then write, under a lock:</b> Neo4j reads without locks, so {@link #claim}, {@link #renew},
 *       {@link #arrive}, {@link #advance}, {@link #fail} and {@link #beginAttempt} first take the write lock of
 *       the node they check ({@link Neo4jSessions#lock}), and only then read and write it, in the same
 *       transaction. That makes arrivals fenced and advance a compare-and-set, as in
 *       {@code PostgresClusterStore}'s {@code FOR UPDATE}.</li>
 * </ul>
 */
public final class Neo4jClusterStore implements ClusterStore {

    private static final String LEASE = "MATCH (n:ClusterLease {run: $run, shard: $shard})";
    private static final String RUN = "MATCH (n:ClusterRun {run: $run})";

    private final Neo4jSessions sessions;

    public Neo4jClusterStore(Neo4jSessions sessions) {
        this.sessions = Objects.requireNonNull(sessions, "sessions");
        sessions.schema(
                "CREATE CONSTRAINT cluster_run IF NOT EXISTS FOR (n:ClusterRun) REQUIRE n.run IS UNIQUE",
                "CREATE CONSTRAINT cluster_lease IF NOT EXISTS FOR (n:ClusterLease) REQUIRE (n.run, n.shard) IS UNIQUE",
                "CREATE CONSTRAINT cluster_attempt IF NOT EXISTS FOR (n:ClusterAttempt) "
                        + "REQUIRE (n.run, n.round, n.shard) IS UNIQUE",
                "CREATE CONSTRAINT cluster_message IF NOT EXISTS FOR (n:ClusterMessage) "
                        + "REQUIRE (n.run, n.round, n.to, n.from) IS UNIQUE",
                "CREATE CONSTRAINT cluster_arrival IF NOT EXISTS FOR (n:ClusterArrival) "
                        + "REQUIRE (n.run, n.round, n.shard) IS UNIQUE",
                "CALL db.awaitIndexes(300)");
    }

    @Override
    public void createRun(String runId, int shards, Duration ttl) {
        sessions.write(tx -> {
            boolean created = tx.run("""
                    MERGE (r:ClusterRun {run: $run})
                    ON CREATE SET r.shards = $shards, r.round = 0, r.status = 'RUNNING', r.fresh = true
                    WITH r, coalesce(r.fresh, false) AS fresh
                    REMOVE r.fresh
                    RETURN fresh""", Map.of("run", runId, "shards", shards)).single().get(0).asBoolean();
            if (created) {   // else another worker created it, leases too
                tx.run("""
                        UNWIND range(0, $shards - 1) AS shard
                        CREATE (:ClusterLease {run: $run, shard: shard,
                                               expires: datetime() + duration({milliseconds: $ttl})})""",
                        Map.of("run", runId, "shards", shards, "ttl", ttl.toMillis())).consume();
            }
            return null;
        });
    }

    @Override
    public Optional<RunState> run(String runId) {
        return sessions.read(tx -> {
            List<Record> rows = tx.run(RUN + " RETURN n.shards, n.round, n.status, n.error",
                    Map.of("run", runId)).list();
            if (rows.isEmpty()) {
                return Optional.empty();
            }
            Record row = rows.get(0);
            return Optional.of(new RunState(row.get(0).asInt(), row.get(1).asInt(),
                    RunStatus.valueOf(row.get(2).asString()), row.get(3).isNull() ? null : row.get(3).asString()));
        });
    }

    @Override
    public List<Lease> leases(String runId) {
        return sessions.read(tx -> {
            List<Lease> leases = new ArrayList<>();
            for (Record row : tx.run("""
                    MATCH (n:ClusterLease {run: $run})
                    RETURN n.shard, n.owner, n.expires <= datetime() ORDER BY n.shard""",
                    Map.of("run", runId)).list()) {
                leases.add(new Lease(row.get(0).asInt(), row.get(1).isNull() ? null : row.get(1).asString(),
                        row.get(2).asBoolean()));
            }
            return leases;
        });
    }

    @Override
    public boolean claim(String runId, int shard, String worker, Duration ttl) {
        Map<String, Object> params = Map.of("run", runId, "shard", shard, "worker", worker, "ttl", ttl.toMillis());
        return sessions.write(tx -> {
            Neo4jSessions.lock(tx, LEASE, params);
            return tx.run(LEASE + """
                     WHERE n.owner IS NULL OR n.owner = $worker OR n.expires <= datetime()
                    SET n.owner = $worker, n.expires = datetime() + duration({milliseconds: $ttl})
                    RETURN count(n)""", params).single().get(0).asInt() == 1;
        });
    }

    @Override
    public void renew(String runId, String worker, Duration ttl) {
        Map<String, Object> params = Map.of("run", runId, "worker", worker, "ttl", ttl.toMillis());
        String mine = "MATCH (n:ClusterLease {run: $run, owner: $worker})";
        sessions.write(tx -> {
            Neo4jSessions.lock(tx, mine, params);
            return tx.run(mine + """
                     WHERE n.expires > datetime()
                    SET n.expires = datetime() + duration({milliseconds: $ttl})""", params).consume();
        });
    }

    @Override
    public int beginAttempt(String runId, int round, int shard) {
        Map<String, Object> params = Map.of("run", runId, "round", round, "shard", shard);
        String attempt = "MATCH (n:ClusterAttempt {run: $run, round: $round, shard: $shard})";
        return sessions.write(tx -> {
            tx.run("MERGE (n:ClusterAttempt {run: $run, round: $round, shard: $shard}) ON CREATE SET n.starts = 0",
                    params).consume();
            Neo4jSessions.lock(tx, attempt, params);
            return tx.run(attempt + " SET n.starts = n.starts + 1 RETURN n.starts - 1", params)
                    .single().get(0).asInt();
        });
    }

    @Override
    public void send(String runId, int round, int from, int to, Collection<String> nodes) {
        sessions.write(tx -> tx.run("""
                MERGE (n:ClusterMessage {run: $run, round: $round, to: $to, from: $from})
                ON CREATE SET n.nodes = $nodes""",
                Map.of("run", runId, "round", round, "to", to, "from", from, "nodes", List.copyOf(nodes)))
                .consume());
    }

    @Override
    public Set<String> inbox(String runId, int round, int to) {
        return sessions.read(tx -> {
            Set<String> inbox = new TreeSet<>();
            for (Record row : tx.run("""
                    MATCH (n:ClusterMessage {run: $run, round: $round, to: $to})
                    UNWIND n.nodes AS key RETURN key""", Map.of("run", runId, "round", round, "to", to)).list()) {
                inbox.add(row.get(0).asString());
            }
            return inbox;
        });
    }

    @Override
    public boolean arrive(String runId, Arrival arrival) {
        Map<String, Object> lease = Map.of("run", runId, "shard", arrival.shard(), "worker", arrival.worker());
        return sessions.write(tx -> {
            Neo4jSessions.lock(tx, LEASE, lease);
            if (tx.run(LEASE + " WHERE n.owner = $worker AND n.expires > datetime() RETURN count(n)", lease)
                    .single().get(0).asInt() == 0) {
                return false;   // fenced: the shard is someone else's now
            }
            tx.run("""
                    MERGE (n:ClusterArrival {run: $run, round: $round, shard: $shard})
                    ON CREATE SET n.worker = $worker, n.attempt = $attempt, n.done = $done, n.failed = $failed,
                                  n.skipped = $skipped, n.batches = $batches, n.edges = $edges""",
                    Map.of("run", runId, "round", arrival.round(), "shard", arrival.shard(),
                            "worker", arrival.worker(), "attempt", arrival.attempt(), "done", arrival.done(),
                            "failed", arrival.failed(), "skipped", arrival.skipped(), "batches", arrival.batches(),
                            "edges", arrival.edges())).consume();
            return true;
        });
    }

    @Override
    public Set<Integer> arrived(String runId, int round) {
        return sessions.read(tx -> {
            Set<Integer> arrived = new HashSet<>();
            for (Record row : tx.run("MATCH (n:ClusterArrival {run: $run, round: $round}) RETURN n.shard",
                    Map.of("run", runId, "round", round)).list()) {
                arrived.add(row.get(0).asInt());
            }
            return Set.copyOf(arrived);
        });
    }

    @Override
    public void advance(String runId, int round) {
        Map<String, Object> params = Map.of("run", runId, "round", round);
        sessions.write(tx -> {
            Neo4jSessions.lock(tx, RUN, params);
            List<Record> run = tx.run(RUN + " WHERE n.round = $round AND n.status = 'RUNNING' RETURN n.shards",
                    params).list();
            if (run.isEmpty()) {
                return null;   // finished, or another worker moved it already
            }
            Record counts = tx.run("""
                    OPTIONAL MATCH (a:ClusterArrival {run: $run, round: $round})
                    RETURN count(a), coalesce(sum(a.batches), 0)""", params).single();
            if (counts.get(0).asLong() < run.get(0).get(0).asLong()) {
                return null;
            }
            tx.run(RUN + (counts.get(1).asLong() == 0 ? " SET n.status = 'DONE'" : " SET n.round = n.round + 1"),
                    params).consume();
            return null;
        });
    }

    @Override
    public void fail(String runId, String error) {
        Map<String, Object> params = Map.of("run", runId, "error", error);
        sessions.write(tx -> {
            Neo4jSessions.lock(tx, RUN, params);
            return tx.run(RUN + " WHERE n.status = 'RUNNING' SET n.status = 'FAILED', n.error = $error", params)
                    .consume();
        });
    }

    @Override
    public List<Arrival> arrivals(String runId) {
        return sessions.read(tx -> {
            List<Arrival> arrivals = new ArrayList<>();
            for (Record row : tx.run("""
                    MATCH (n:ClusterArrival {run: $run})
                    RETURN n.round, n.shard, n.worker, n.attempt, n.done, n.failed, n.skipped, n.batches, n.edges
                    ORDER BY n.round, n.shard""", Map.of("run", runId)).list()) {
                arrivals.add(new Arrival(row.get(0).asInt(), row.get(1).asInt(), row.get(2).asString(),
                        row.get(3).asInt(), row.get(4).asLong(), row.get(5).asLong(), row.get(6).asLong(),
                        row.get(7).asLong(), row.get(8).asLong()));
            }
            return arrivals;
        });
    }

    /** Leaves the driver open: it is the caller's. */
    @Override
    public void close() {
    }
}
