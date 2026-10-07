package com.salesforce.einstein.graphexecutor.distributed;

import com.salesforce.einstein.ds.graph.Graph;
import com.salesforce.einstein.graphexecutor.executor.GroupResult;
import com.salesforce.einstein.graphexecutor.executor.IneligibleGroupPolicy;
import com.salesforce.einstein.graphexecutor.executor.TraversalResult;
import com.salesforce.einstein.graphexecutor.executor.TraversalTaskExecutor;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DistributedTraversalExecutorTest {
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

    /** Counts runs, fails nodes named "fail*", rejects "img*", and crashes where {@code crash} says. */
    private class Recording extends DistributedTraversalExecutor<String> {
        final Map<String, AtomicInteger> runs = new ConcurrentHashMap<>();
        final Map<String, AtomicInteger> sends = new ConcurrentHashMap<>();
        final Map<Object, AtomicInteger> laneInFlight = new ConcurrentHashMap<>();
        final Map<Object, Integer> laneMaxInFlight = new ConcurrentHashMap<>();
        CrashPoint crash = (partition, round, sent) -> false;   // consulted on first attempts only
        boolean crashEveryAttempt;
        int deliveries = 1;
        int maxDepth = Integer.MAX_VALUE;
        boolean byHost;
        Set<String> roots;   // null: the default
        boolean startRootless = true;
        Map<String, CountDownLatch> rendezvous = Map.of();

        Recording(int partitions, Partitioner<String> partitioner) {
            super(pool, partitions, partitioner);
        }

        Recording(int partitions) {
            this(partitions, Partitioner.hash());
        }

        @Override
        protected void executeTask(String node, int depth) throws Exception {
            Object lane = laneOf(node);
            int inFlight = laneInFlight.computeIfAbsent(lane, l -> new AtomicInteger()).incrementAndGet();
            laneMaxInFlight.merge(lane, inFlight, Math::max);
            try {
                runs.computeIfAbsent(node, n -> new AtomicInteger()).incrementAndGet();   // idempotent: just counts
                CountDownLatch latch = rendezvous.get(node);
                if (latch != null) {
                    latch.countDown();
                    if (!latch.await(5, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("rendezvous timed out: partitions did not run concurrently");
                    }
                }
                if (node.startsWith("fail")) {
                    throw new IllegalStateException(node);
                }
            } finally {
                laneInFlight.get(lane).decrementAndGet();
            }
        }

        @Override
        protected Set<String> roots(Graph<String> graph) {
            return roots == null ? super.roots(graph) : roots;
        }

        @Override
        protected boolean startRootlessGroups() {
            return startRootless;
        }

        @Override
        protected boolean admit(String node) {
            return !node.startsWith("img");
        }

        @Override
        protected int maxDepth() {
            return maxDepth;
        }

        @Override
        protected Object laneOf(String node) {
            return byHost ? host(node) : node;
        }

        @Override
        protected boolean placeGroupsWhole() {
            return !byHost;
        }

        @Override
        protected int deliveriesPerSend() {
            return deliveries;
        }

        @Override
        protected void afterSend(int partition, int round, int attempt) {
            if (attempt > 0 && !crashEveryAttempt) {
                return;
            }
            int sent = sends.computeIfAbsent(partition + "/" + round + "/" + attempt, k -> new AtomicInteger())
                    .incrementAndGet();
            if (crash.test(partition, round, sent)) {
                throw new IllegalStateException("simulated crash of partition " + partition);
            }
        }
    }

    private static String host(String node) {
        int slash = node.indexOf('/');
        return slash < 0 ? node : node.substring(0, slash);
    }

    private static Graph<String> graph(String... edges) {
        Graph<String> g = Graph.directed();
        for (String edge : edges) {
            String[] ends = edge.split("->");
            g.addEdge(ends[0], ends[1]);
        }
        return g;
    }

    /** Several components, cycles everywhere, a few excluded and failing nodes. */
    private static Graph<String> randomGraph(long seed) {
        Random random = new Random(seed);
        Graph<String> g = Graph.directed();
        for (int i = 0; i < 150; i++) {
            g.addNode(name(i));
        }
        for (int e = 0; e < 300; e++) {
            int from = random.nextInt(150);
            int to = (from / 50) * 50 + random.nextInt(50);   // edges stay inside blocks of 50: 3+ components
            g.addEdge(name(from), name(to));
        }
        return g;
    }

    private static String name(int i) {
        return i % 37 == 5 ? "img" + i : i % 41 == 7 ? "fail" + i : "n" + i;
    }

    @Test
    void sameAnswerAsTheLocalExecutor() {
        Graph<String> g = randomGraph(42);
        Recording distributed = new Recording(4, Partitioner.greedy(0.1));
        distributed.maxDepth = 3;
        TraversalReport<String> report = distributed.execute(g);

        ExecutorService taskPool = Executors.newFixedThreadPool(2);
        try {
            TraversalTaskExecutor<String> local = new TraversalTaskExecutor<>(pool, taskPool, IneligibleGroupPolicy.REJECT_GRAPH) {
                @Override
                protected void executeTask(String node, int depth) {
                    if (node.startsWith("fail")) {
                        throw new IllegalStateException(node);
                    }
                }

                @Override
                protected boolean admit(String node) {
                    return !node.startsWith("img");
                }

                @Override
                protected int maxDepth() {
                    return 3;
                }
            };
            Map<String, Integer> depth = new HashMap<>();
            Set<String> failed = new HashSet<>();
            Set<String> excluded = new HashSet<>();
            Set<String> tooDeep = new HashSet<>();
            Set<String> unreachable = new HashSet<>();
            Set<String> completed = new HashSet<>();
            for (GroupResult<String, TraversalResult<String>> group : local.execute(g)) {
                TraversalResult<String> r = group.value();
                depth.putAll(r.depth());
                failed.addAll(r.failed().keySet());
                excluded.addAll(r.excluded());
                tooDeep.addAll(r.tooDeep());
                unreachable.addAll(r.unreachable());
                completed.addAll(r.completed());
            }

            assertEquals(depth, report.depth());
            assertEquals(failed, report.failed().keySet());
            assertEquals(excluded, Set.copyOf(report.excluded()));
            assertEquals(tooDeep, Set.copyOf(report.tooDeep()));
            assertEquals(unreachable, Set.copyOf(report.unreachable()));
            Set<String> ran = new HashSet<>();
            report.rounds().forEach(ran::addAll);
            assertEquals(completed, ran);
            assertTrue(report.stats().groups() >= 3);
        } finally {
            taskPool.shutdownNow();
        }
    }

    @Test
    void anUndirectedGraphRunsLikeItsTwoWayDirectedTwinAndCountsEachCutEdgeOnce() {
        Graph<String> directed = randomGraph(7);
        Graph<String> undirected = Graph.undirected();
        Graph<String> twin = Graph.directed();
        for (String node : directed.nodes()) {
            undirected.addNode(node);
            twin.addNode(node);
            for (String to : directed.successors(node)) {
                undirected.addEdge(node, to);
                twin.addEdge(node, to);
                twin.addEdge(to, node);
            }
        }
        for (Set<String> roots : Arrays.asList(null, Set.of("n1", "n60"))) {
            TraversalReport<String> one = run(undirected, roots);
            TraversalReport<String> other = run(twin, roots);

            assertEquals(other.depth(), one.depth());
            assertEquals(other.failed().keySet(), one.failed().keySet());
            assertEquals(Set.copyOf(other.excluded()), Set.copyOf(one.excluded()));
            assertEquals(Set.copyOf(other.unreachable()), Set.copyOf(one.unreachable()));
            assertEquals(other.rounds().stream().map(Set::copyOf).toList(),
                    one.rounds().stream().map(Set::copyOf).toList());
            assertTrue(other.stats().edgeCut() > 0);
            assertEquals(other.stats().edgeCut() / 2, one.stats().edgeCut());   // the twin has every edge twice
        }
    }

    /** Placement by hash, so both graphs put every node on the same partition. */
    private TraversalReport<String> run(Graph<String> g, Set<String> roots) {
        Recording executor = new Recording(4);
        executor.byHost = true;
        executor.roots = roots;
        TraversalReport<String> report = executor.execute(g);
        executor.runs.values().forEach(count -> assertEquals(1, count.get()));
        return report;
    }

    @Test
    void roundsAreDepthLevelsAndCyclesAreFine() {
        Recording executor = new Recording(3);
        TraversalReport<String> report = executor.execute(graph("a->b", "a->c", "b->d", "c->d", "d->a"));

        // no node without predecessors: the group starts at its first node
        assertEquals(List.of(List.of("a"), List.of("b", "c"), List.of("d")), report.rounds());
        executor.runs.values().forEach(count -> assertEquals(1, count.get()));
        assertEquals(3, report.stats().liveRounds());
    }

    @Test
    void aFailureBlocksNothingAndTooDeepIsExact() {
        Recording executor = new Recording(2);
        executor.maxDepth = 2;
        TraversalReport<String> report = executor.execute(graph("a->fail", "fail->b", "b->c", "c->d"));

        assertEquals(Set.of("fail"), report.failed().keySet());
        assertEquals(List.of(List.of("a"), List.of(), List.of("b")), report.rounds());
        assertEquals(List.of("c", "d"), report.tooDeep());
        assertEquals(Map.of("a", 0, "fail", 1, "b", 2, "c", 3, "d", 4), report.depth());
    }

    @Test
    void excludedNodesAreNeitherRunNorTraversedThrough() {
        Recording executor = new Recording(2);
        TraversalReport<String> report = executor.execute(graph("a->img1", "img1->b", "a->c"));

        assertEquals(List.of("img1"), report.excluded());
        assertEquals(List.of("b"), report.unreachable());
        assertEquals(Set.of("a", "c"), executor.runs.keySet());
    }

    @Test
    void rootlessGroupsAreUnreachableWhenStartingThemIsTurnedOff() {
        Graph<String> g = graph("home->about", "x->y", "y->x");
        Recording starting = new Recording(2);
        starting.roots = Set.of("home");
        assertEquals(Set.of("home", "about", "x", "y"), starting.execute(g).depth().keySet());

        Recording strict = new Recording(2);
        strict.roots = Set.of("home");
        strict.startRootless = false;
        TraversalReport<String> report = strict.execute(g);
        assertEquals(Set.of("home", "about"), strict.runs.keySet());
        assertEquals(List.of("x", "y"), report.unreachable());
    }

    @Test
    void byKeyKeepsEachLaneOnOnePartition() {
        Graph<String> g = Graph.directed();
        for (int h = 0; h < 4; h++) {   // "h0".."h3": consecutive hash codes, so four different partitions
            for (int p = 0; p < 5; p++) {
                g.addEdge("root", "h" + h + "/" + p);
                g.addEdge("h" + h + "/" + p, "root");
            }
        }
        Recording executor = new Recording(4, Partitioner.byKey(DistributedTraversalExecutorTest::host));
        executor.byHost = true;
        CountDownLatch twoHosts = new CountDownLatch(2);
        executor.rendezvous = Map.of("h0/0", twoHosts, "h1/0", twoHosts);   // only passes if they overlap

        TraversalReport<String> report = executor.execute(g);

        assertTrue(report.failed().isEmpty(), () -> report.failed().toString());
        assertEquals(21, executor.runs.size());
        executor.laneMaxInFlight.forEach((lane, max) -> assertEquals(1, max, "lane " + lane));
    }

    @Test
    void aCrashedPartitionRecoversFromTheLogWithoutRerunningTasks() {
        Graph<String> g = randomGraph(7);
        TraversalReport<String> clean = new Recording(4).execute(g);

        Recording executor = new Recording(4);
        executor.crash = (partition, round, sent) -> round == 1 && sent == 1;   // every partition, mid-round
        TraversalReport<String> recovered = executor.execute(g);

        assertTrue(recovered.stats().recoveries() > 0);
        assertEquals(clean.rounds(), recovered.rounds());
        assertEquals(clean.depth(), recovered.depth());
        assertEquals(clean.failed().keySet(), recovered.failed().keySet());
        executor.runs.forEach((node, count) -> assertEquals(1, count.get(), node));   // logged before the crash
        // every failure is still reported; one logged before a crash comes back from the log
        recovered.failed().values().forEach(error -> assertInstanceOf(IllegalStateException.class, error));
    }

    @Test
    void aReceiverThatLostItsInboxRebuildsTheNextFrontier() {
        // partition 0 crashes after its last send of round 0, when others' round-0 batches are in its inbox
        Graph<String> g = randomGraph(11);
        TraversalReport<String> clean = new Recording(3).execute(g);

        Recording executor = new Recording(3);
        executor.crash = (partition, round, sent) -> partition == 0 && round == 0 && sent >= 1;
        TraversalReport<String> recovered = executor.execute(g);

        assertEquals(clean.depth(), recovered.depth());
        assertEquals(clean.rounds(), recovered.rounds());
    }

    @Test
    void duplicateDeliveriesAreDropped() {
        Graph<String> g = randomGraph(3);
        TraversalReport<String> clean = new Recording(4).execute(g);

        Recording executor = new Recording(4);
        executor.deliveries = 3;
        TraversalReport<String> report = executor.execute(g);

        assertEquals(2 * report.stats().batchesSent(), report.stats().duplicatesDropped());
        assertEquals(clean.depth(), report.depth());
        executor.runs.values().forEach(count -> assertEquals(1, count.get()));
    }

    @Test
    void aPartitionThatKeepsCrashingFailsTheRun() {
        Recording executor = new Recording(2);
        executor.crashEveryAttempt = true;
        executor.crash = (partition, round, sent) -> true;
        assertThrows(IllegalStateException.class, () -> executor.execute(graph("a->b", "b->c")));
    }
}
