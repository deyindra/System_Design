package com.salesforce.einstein.graphexecutor.distributed;

import com.salesforce.einstein.graphexecutor.executor.IneligibleGroupPolicy;
import com.salesforce.einstein.graphexecutor.graph.Graph;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DistributedTopologicalExecutorTest {
    private final ExecutorService pool = Executors.newFixedThreadPool(4);

    @AfterEach
    void shutdown() throws InterruptedException {
        pool.shutdownNow();
        assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS));
    }

    /** Where to crash: partition, round, and how many batches it has sent in that round (1-based). */
    @FunctionalInterface
    private interface CrashPoint {
        boolean test(int partition, int round, int sent);
    }

    /** Counts how often each task ran; fails tasks named "fail"; crashes where {@code crash} says. */
    private class Recording extends DistributedTopologicalExecutor<String> {
        final Map<String, AtomicInteger> runs = new ConcurrentHashMap<>();
        final Map<String, AtomicInteger> sends = new ConcurrentHashMap<>();
        CrashPoint crash = (partition, round, sent) -> false;   // consulted on first attempts only
        int deliveries = 1;

        Recording(int partitions, Partitioner<String> partitioner, IneligibleGroupPolicy policy) {
            super(pool, partitions, partitioner, policy);
        }

        Recording(int partitions) {
            this(partitions, Partitioner.hash(), IneligibleGroupPolicy.REJECT_GRAPH);
        }

        @Override
        protected void executeTask(String task) {
            runs.computeIfAbsent(task, t -> new AtomicInteger()).incrementAndGet();   // idempotent: just counts
            if (task.startsWith("fail")) {
                throw new IllegalStateException(task);
            }
        }

        @Override
        protected int deliveriesPerSend() {
            return deliveries;
        }

        @Override
        protected void afterSend(int partition, int round, int attempt) {
            if (attempt > 0) {
                return;
            }
            int sent = sends.computeIfAbsent(partition + "/" + round, k -> new AtomicInteger()).incrementAndGet();
            if (crash.test(partition, round, sent)) {
                throw new IllegalStateException("simulated crash of partition " + partition);
            }
        }
    }

    /** Fixed placement for a single (giant) group: partition p gets the nodes listed at index p. */
    private static Partitioner<String> fixed(List<Set<String>> layout) {
        return (nodes, neighbors, partitions) -> {
            Map<String, Integer> assignment = new LinkedHashMap<>();
            for (String node : nodes) {
                for (int p = 0; p < layout.size(); p++) {
                    if (layout.get(p).contains(node)) {
                        assignment.put(node, p);
                    }
                }
            }
            return assignment;
        };
    }

    /** build -> {unit, lint} -> package -> deploy, plus docs -> deploy. Longest chain: 3 edges, so 4 rounds. */
    private static Graph<String> pipeline() {
        Graph<String> g = Graph.directed();
        g.addEdge("build", "unit");
        g.addEdge("build", "lint");
        g.addEdge("unit", "package");
        g.addEdge("lint", "package");
        g.addEdge("package", "deploy");
        g.addEdge("docs", "deploy");
        return g;
    }

    private static Map<String, Integer> roundOf(RunReport<String> report) {
        Map<String, Integer> roundOf = new HashMap<>();
        for (int r = 0; r < report.rounds().size(); r++) {
            for (String node : report.rounds().get(r)) {
                roundOf.put(node, r);
            }
        }
        return roundOf;
    }

    private static void assertRespectsDependencies(Graph<String> g, RunReport<String> report) {
        Map<String, Integer> roundOf = roundOf(report);
        for (String from : g.nodes()) {
            for (String to : g.successors(from)) {
                assertTrue(roundOf.get(from) < roundOf.get(to), from + " must finish before " + to);
            }
        }
    }

    @Test
    void runsInRoundsAcrossPartitions() {
        Graph<String> g = pipeline();
        Recording executor = new Recording(3);
        RunReport<String> report = executor.execute(g);

        assertEquals(List.of(List.of("build", "docs"), List.of("unit", "lint"), List.of("package"), List.of("deploy")),
                report.rounds());
        assertRespectsDependencies(g, report);
        assertEquals(4, report.stats().liveRounds());   // the longest chain, not the node count
        assertTrue(executor.runs.values().stream().allMatch(n -> n.get() == 1));
    }

    @Test
    void decrementsAreCombinedPerReceivingPartition() {
        // 100 sources all feeding one sink, split over 4 partitions: 100 dependencies, at most 4 messages
        Graph<String> g = Graph.directed();
        for (int i = 0; i < 100; i++) {
            g.addEdge("s" + i, "sink");
        }
        RunReport<String> report = new Recording(4).execute(g);

        assertEquals(100, report.stats().decrementEdges());
        assertTrue(report.stats().batchesSent() <= 4, "sent " + report.stats().batchesSent());
        assertEquals(List.of("sink"), report.rounds().get(1));
    }

    @Test
    void combinedCountsAboveTheIntegerCacheStillReleaseTheNode() {
        // 300 dependencies of one sink on one partition: a single batch carries (sink, -300), and
        // pending (300) == k (300) must compare values; with boxed Integers it compared references
        Graph<String> g = Graph.directed();
        for (int i = 0; i < 300; i++) {
            g.addEdge("s" + i, "sink");
        }
        RunReport<String> report = new Recording(1).execute(g);

        assertEquals(List.of("sink"), report.rounds().get(1));
        assertEquals(List.of(), report.skipped());
    }

    @Test
    void cycleRejectsGraphAfterDryRunAndNothingRuns() {
        Graph<String> g = pipeline();             // group 0, acyclic
        g.addEdge("p", "q");
        g.addEdge("q", "p");
        g.addEdge("q", "after");                  // group 1: cycle plus a dependant
        Recording executor = new Recording(2);

        CyclicGraphException e = assertThrows(CyclicGraphException.class, () -> executor.execute(g));
        assertEquals(Map.of(1, List.of("p", "q", "after")), e.blockedByGroup());
        assertTrue(executor.runs.isEmpty());
    }

    @Test
    void isolatePolicyDropsOnlyCyclicGroups() {
        Graph<String> g = pipeline();
        g.addEdge("x", "x");                      // group 1: self-loop
        Recording executor = new Recording(2, Partitioner.hash(), IneligibleGroupPolicy.ISOLATE_GROUP);
        RunReport<String> report = executor.execute(g);

        assertEquals(Map.of(1, List.of("x")), report.rejectedGroups());
        assertEquals(6, executor.runs.size());
        assertFalse(executor.runs.containsKey("x"));
    }

    @Test
    void failedTaskSkipsOnlyItsDependants() {
        Graph<String> g = Graph.directed();
        g.addEdge("a", "fail");
        g.addEdge("fail", "b");
        g.addEdge("b", "c");
        g.addEdge("a", "d");
        RunReport<String> report = new Recording(2).execute(g);

        assertEquals(Set.of("fail"), report.failed().keySet());
        assertInstanceOf(IllegalStateException.class, report.failed().get("fail"));
        assertEquals(List.of("b", "c"), report.skipped());
        assertEquals(List.of(List.of("a"), List.of("d")), report.rounds());
    }

    /**
     * One group over 2 partitions. In round 1 each partition sends to both: partition 0 (a1) to itself
     * (a2) and to 1 (b2); partition 1 (b1) to itself (b2) and to 0 (a3).
     */
    private static Graph<String> crossing() {
        Graph<String> g = Graph.directed();
        g.addEdge("root", "a1");
        g.addEdge("root", "b1");
        g.addEdge("a1", "a2");
        g.addEdge("a1", "b2");
        g.addEdge("b1", "b2");
        g.addEdge("b1", "a3");
        return g;
    }

    private Recording crossingExecutor() {
        return new Recording(2, fixed(List.of(Set.of("root", "a1", "a2", "a3"), Set.of("b1", "b2"))),
                IneligibleGroupPolicy.REJECT_GRAPH);
    }

    @Test
    void crashedPartitionRebuildsFromTheLogAndReplayDuplicatesAreDropped() {
        Recording executor = crossingExecutor();
        executor.crash = (partition, round, sent) -> partition == 0 && round == 1 && sent == 2;   // both batches out

        RunReport<String> report = executor.execute(crossing());

        assertEquals(1, report.stats().recoveries());
        assertEquals(1, report.stats().duplicatesDropped());   // partition 1 got (b2, -1) from the crash and the replay
        assertEquals(1, executor.runs.get("a1").get());         // logged DONE before the crash: reused, not re-run
        assertEquals(1, executor.runs.get("b1").get());         // the other partition never rolled back
        assertRespectsDependencies(crossing(), report);
        assertEquals(List.of(), report.skipped());
    }

    @Test
    void messagesLostWithTheInboxAreRecoveredFromTheLog() {
        // partition 0 crashes only after partition 1's (a3, -1) is in its inbox, so the crash destroys it
        CountDownLatch delivered = new CountDownLatch(1);
        Recording executor = crossingExecutor();
        executor.crash = (partition, round, sent) -> {
            if (round != 1) {
                return false;
            }
            if (partition == 1 && sent == 2) {   // batches go out in receiver order: 1 to itself, then to 0
                delivered.countDown();
                return false;
            }
            if (partition == 0 && sent == 1) {
                try {
                    assertTrue(delivered.await(5, TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return true;
            }
            return false;
        };

        RunReport<String> report = executor.execute(crossing());

        assertEquals(1, report.stats().recoveries());
        // nobody re-sent (a3, -1): the rebuild found b1 DONE in the log, so a3 was released anyway
        assertEquals(List.of("a2", "b2", "a3"), report.rounds().get(2));   // graph insertion order
        assertEquals(List.of(), report.skipped());
        assertTrue(executor.runs.values().stream().allMatch(n -> n.get() == 1));
    }

    @Test
    void failuresSurviveACrashThroughTheLog() {
        Graph<String> g = Graph.directed();
        g.addEdge("root", "fail");
        g.addEdge("root", "ok");
        g.addEdge("fail", "after");
        g.addEdge("ok", "after");
        Recording executor = new Recording(2, fixed(List.of(Set.of("root", "fail", "ok"), Set.of("after"))),
                IneligibleGroupPolicy.REJECT_GRAPH);
        executor.crash = (partition, round, sent) -> partition == 0 && round == 1;

        RunReport<String> report = executor.execute(g);

        assertEquals(1, report.stats().recoveries());
        assertEquals(1, executor.runs.get("fail").get());   // FAILED was logged: the replay did not retry it
        assertTrue(report.failed().get("fail").getMessage().contains("recovered from the completion log"));
        assertEquals(List.of("after"), report.skipped());
    }

    @Test
    void partitionThatKeepsCrashingFailsTheRun() {
        Graph<String> g = pipeline();
        Recording executor = new Recording(1) {
            @Override
            protected void afterSend(int partition, int round, int attempt) {
                throw new IllegalStateException("always down");
            }
        };
        IllegalStateException e = assertThrows(IllegalStateException.class, () -> executor.execute(g));
        assertTrue(e.getMessage().contains("crashed " + DistributedTopologicalExecutor.MAX_ATTEMPTS + " times"));
    }

    @Test
    void atLeastOnceDeliveryIsHarmless() {
        Graph<String> g = pipeline();
        Recording once = new Recording(3);
        Recording twice = new Recording(3);
        twice.deliveries = 2;

        RunReport<String> expected = once.execute(g);
        RunReport<String> actual = twice.execute(g);

        assertEquals(expected.rounds(), actual.rounds());
        assertEquals(actual.stats().batchesSent(), actual.stats().duplicatesDropped());   // every copy dropped
    }

    @Test
    void smallGroupsArePlacedWholeAndTheGiantIsSplit() {
        // a 40-node chain (giant) plus 10 pairs; 4 partitions -> fair share 15
        Graph<String> g = Graph.directed();
        for (int i = 0; i < 39; i++) {
            g.addEdge("c" + i, "c" + (i + 1));
        }
        for (int i = 0; i < 10; i++) {
            g.addEdge("x" + i, "y" + i);
        }
        RunReport<String> report = new Recording(4, Partitioner.greedy(0.1), IneligibleGroupPolicy.REJECT_GRAPH)
                .execute(g);

        assertEquals(11, report.stats().groups());
        assertEquals(10, report.stats().groupsPlacedWhole());
        assertEquals(1, report.stats().groupsSplit());
        // greedy on a BFS-ordered chain cuts it into contiguous runs: only the 3 boundaries cross
        assertEquals(3, report.stats().edgeCut());
        assertEquals(40, report.stats().liveRounds());
    }

    @Test
    void labelPropagationFindsWeaklyConnectedComponents() {
        // a -> c <- b (joined only through c), x -> y, z alone
        List<String> nodes = List.of("a", "c", "x", "y", "b", "z");
        Map<String, List<String>> neighbors = Map.of(
                "a", List.of("c"), "c", List.of("a", "b"), "b", List.of("c"),
                "x", List.of("y"), "y", List.of("x"), "z", List.of());
        Map<String, Integer> owner = Partitioner.<String>hash().assign(nodes, neighbors::get, 3);

        LabelPropagation.Result<String> result = LabelPropagation.run(nodes, neighbors, owner, 3, pool);

        assertEquals(Map.of("a", 0, "c", 0, "b", 0, "x", 2, "y", 2, "z", 5), result.labels());
    }

    @Test
    void greedyPartitionerCutsFarFewerEdgesThanHash() {
        // 20 x 20 grid, edges right and down
        List<String> nodes = new ArrayList<>();
        Map<String, List<String>> neighbors = new HashMap<>();
        for (int r = 0; r < 20; r++) {
            for (int c = 0; c < 20; c++) {
                String node = r + "," + c;
                nodes.add(node);
                List<String> adjacent = new ArrayList<>();
                if (r > 0) adjacent.add((r - 1) + "," + c);
                if (r < 19) adjacent.add((r + 1) + "," + c);
                if (c > 0) adjacent.add(r + "," + (c - 1));
                if (c < 19) adjacent.add(r + "," + (c + 1));
                neighbors.put(node, adjacent);
            }
        }
        long hashCut = cut(Partitioner.<String>hash().assign(nodes, neighbors::get, 4), neighbors);
        Map<String, Integer> greedy = Partitioner.<String>greedy(0.05).assign(nodes, neighbors::get, 4);
        long greedyCut = cut(greedy, neighbors);

        assertTrue(greedyCut * 3 < hashCut, "greedy " + greedyCut + " vs hash " + hashCut);
        int[] load = new int[4];
        greedy.values().forEach(p -> load[p]++);
        for (int l : load) {
            assertTrue(l <= 105, "partition over capacity: " + l);   // 400 / 4 * 1.05
        }
    }

    private static long cut(Map<String, Integer> assignment, Map<String, List<String>> neighbors) {
        long crossing = 0;
        for (var entry : neighbors.entrySet()) {
            for (String other : entry.getValue()) {
                if (!assignment.get(entry.getKey()).equals(assignment.get(other))) {
                    crossing++;
                }
            }
        }
        return crossing / 2;   // each undirected adjacency was counted from both ends
    }
}
