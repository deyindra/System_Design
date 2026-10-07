package com.salesforce.einstein.graphexecutor.distributed;

import com.salesforce.einstein.ds.graph.Graph;
import com.salesforce.einstein.graphexecutor.executor.IneligibleGroupPolicy;
import com.salesforce.einstein.graphexecutor.spi.ClusterStore.Arrival;
import com.salesforce.einstein.graphexecutor.spi.ClusterStore.RunStatus;
import com.salesforce.einstein.graphexecutor.spi.ClusterStore;
import com.salesforce.einstein.graphexecutor.spi.CompletionLog.Outcome;
import com.salesforce.einstein.graphexecutor.spi.CompletionLog.Status;
import com.salesforce.einstein.graphexecutor.spi.CompletionLog;
import com.salesforce.einstein.graphexecutor.spi.GraphStore;
import com.salesforce.einstein.graphexecutor.spi.NodeCodec;
import com.salesforce.einstein.graphexecutor.spi.memory.InMemoryClusterStore;
import com.salesforce.einstein.graphexecutor.spi.memory.InMemoryCompletionLog;
import com.salesforce.einstein.graphexecutor.spi.memory.InMemoryGraphStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.ToIntFunction;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WorkerTest {
    private static final int SHARDS = 4;
    private static final ToIntFunction<String> BY_KEY = GraphStore.byKey(node -> node, SHARDS);

    private final ExecutorService pool = Executors.newCachedThreadPool();

    @AfterEach
    void shutdown() throws InterruptedException {
        pool.shutdownNow();
        assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS));
    }

    /** Where to crash: shard, round, attempt. */
    @FunctionalInterface
    private interface CrashPoint {
        boolean test(int shard, int round, int attempt);
    }

    private class Traversal extends DistributedTraversalExecutor<String> {
        final Map<String, AtomicInteger> runs = new ConcurrentHashMap<>();
        final Set<String> roots;
        volatile CrashPoint crash = (shard, round, attempt) -> false;

        Traversal(Set<String> roots) {
            super(pool, SHARDS, Partitioner.byKey(BY_KEY::applyAsInt));
            this.roots = roots;
        }

        @Override
        protected void executeTask(String node, int depth) {
            runs.computeIfAbsent(node, n -> new AtomicInteger()).incrementAndGet();
            if (node.startsWith("fail")) {
                throw new IllegalStateException(node);
            }
        }

        @Override
        protected Set<String> roots(Graph<String> graph) {
            return roots;
        }

        @Override
        protected boolean startRootlessGroups() {
            return false;
        }

        @Override
        protected void afterSend(int partition, int round, int attempt) {
            if (crash.test(partition, round, attempt)) {
                throw new IllegalStateException("simulated death of the worker holding shard " + partition);
            }
        }
    }

    private class Topological extends DistributedTopologicalExecutor<String> {
        final Map<String, AtomicInteger> runs = new ConcurrentHashMap<>();

        Topological() {
            super(pool, SHARDS, Partitioner.hash(), IneligibleGroupPolicy.REJECT_GRAPH);
        }

        @Override
        protected void executeTask(String node) {
            runs.computeIfAbsent(node, n -> new AtomicInteger()).incrementAndGet();
            if (node.startsWith("fail")) {
                throw new IllegalStateException(node);
            }
        }
    }

    /** n nodes, m random edges; with {@code acyclic}, edges only go from lower to higher index. */
    private static Graph<String> random(long seed, int n, int m, boolean acyclic) {
        Random random = new Random(seed);
        Graph<String> g = Graph.directed();
        for (int i = 0; i < n; i++) {
            g.addNode("n" + i);
        }
        while (g.edgeCount() < m) {
            int a = random.nextInt(n);
            int b = random.nextInt(n);
            if (a != b && (!acyclic || a < b)) {
                g.addEdge("n" + a, "n" + b);
            }
        }
        return g;
    }

    private static Worker.Config config(String run, String worker) {
        return Worker.Config.of(run, worker).withMaxShards(2).withLease(Duration.ofMillis(300), Duration.ofMillis(5))
                .withBatchSize(7);
    }

    /** Starts one worker per id on its own thread. */
    private List<CompletableFuture<ClusterReport>> start(List<Worker<String>> workers) {
        List<CompletableFuture<ClusterReport>> running = new ArrayList<>();
        for (Worker<String> worker : workers) {
            running.add(CompletableFuture.supplyAsync(worker::run, pool));
        }
        return running;
    }

    private static Map<String, Integer> rounds(Graph<String> g, CompletionLog<String> log) {
        Map<String, Integer> rounds = new HashMap<>();
        for (String node : g.nodes()) {
            log.outcome(node).ifPresent(outcome -> rounds.put(node, outcome.round()));
        }
        return rounds;
    }

    @Test
    void threeWorkersTraverseAsTheOneJvmExecutorDoes() {
        Graph<String> g = random(7, 80, 200, false);
        Set<String> roots = Set.of("n0", "n1");
        TraversalReport<String> expected = new Traversal(roots).execute(g);

        GraphStore<String> store = InMemoryGraphStore.of(g, SHARDS, BY_KEY);
        try (InMemoryClusterStore cluster = new InMemoryClusterStore()) {
            CompletionLog<String> log = new InMemoryCompletionLog<>();
            Traversal executor = new Traversal(roots);
            List<Worker<String>> workers = new ArrayList<>();
            for (int w = 0; w < 3; w++) {
                workers.add(Worker.traversal(executor, roots, store, log, NodeCodec.strings(), cluster, config("t", "w" + w)));
            }
            ClusterReport report = start(workers).get(0).join();

            assertEquals(RunStatus.DONE, report.status());
            assertEquals(expected.depth(), rounds(g, log));
            assertEquals(expected.depth().size(), report.done());
            assertTrue(executor.runs.values().stream().allMatch(n -> n.get() == 1));
            assertEquals(expected.rounds().size() + 1, report.rounds());   // plus the last round, which sends nothing
        }
    }

    @Test
    void threeWorkersRunDependenciesInTheSameRoundsAsTheOneJvmExecutor() {
        Graph<String> g = random(11, 60, 150, true);
        RunReport<String> expected = new Topological().execute(g);
        Map<String, Integer> expectedRounds = new HashMap<>();
        for (int r = 0; r < expected.rounds().size(); r++) {
            for (String node : expected.rounds().get(r)) {
                expectedRounds.put(node, r);
            }
        }

        GraphStore<String> store = InMemoryGraphStore.of(g, SHARDS, BY_KEY);
        try (InMemoryClusterStore cluster = new InMemoryClusterStore()) {
            CompletionLog<String> log = new InMemoryCompletionLog<>();
            Topological executor = new Topological();
            List<Worker<String>> workers = new ArrayList<>();
            for (int w = 0; w < 3; w++) {
                workers.add(Worker.topological(executor, store, log, NodeCodec.strings(), cluster, config("k", "w" + w)));
            }
            ClusterReport report = start(workers).get(0).join();

            assertEquals(RunStatus.DONE, report.status());
            assertEquals(expectedRounds, rounds(g, log));
            assertTrue(executor.runs.values().stream().allMatch(n -> n.get() == 1));
        }
    }

    @Test
    void aFailedDependencyBlocksOnlyItsDependantsAndACycleNeverRuns() {
        Graph<String> g = Graph.directed();
        g.addEdge("a", "fail");
        g.addEdge("fail", "b");
        g.addEdge("a", "c");
        g.addEdge("p", "q");
        g.addEdge("q", "p");                       // a cycle: never ready
        g.addEdge("a", "p");
        CompletionLog<String> log = new InMemoryCompletionLog<>();
        Topological executor = new Topological();
        try (InMemoryClusterStore cluster = new InMemoryClusterStore()) {
            Worker<String> worker = Worker.topological(executor, InMemoryGraphStore.of(g, SHARDS, BY_KEY), log,
                    NodeCodec.strings(), cluster, config("f", "w").withMaxShards(SHARDS));

            ClusterReport report = worker.run();

            assertEquals(RunStatus.DONE, report.status());
            assertEquals(Set.of("a", "fail", "c"), executor.runs.keySet());
            assertEquals(2, report.done());
            assertEquals(1, report.failed());
            assertEquals(Status.FAILED, log.outcome("fail").map(Outcome::status).orElseThrow());
        }
    }

    /** The same nodes and edges, undirected. */
    private static Graph<String> undirected(Graph<String> directed) {
        Graph<String> g = Graph.undirected();
        for (String node : directed.nodes()) {
            g.addNode(node);
            directed.successors(node).forEach(to -> g.addEdge(node, to));
        }
        return g;
    }

    @Test
    void workersTraverseAnUndirectedStoreAsTheOneJvmExecutorDoes() {
        Graph<String> g = undirected(random(5, 80, 90, false));
        Set<String> roots = Set.of("n0", "n1");
        TraversalReport<String> expected = new Traversal(roots).execute(g);

        GraphStore<String> store = InMemoryGraphStore.of(g, SHARDS, BY_KEY);
        assertFalse(store.isDirected());
        try (InMemoryClusterStore cluster = new InMemoryClusterStore()) {
            CompletionLog<String> log = new InMemoryCompletionLog<>();
            Traversal executor = new Traversal(roots);
            List<Worker<String>> workers = new ArrayList<>();
            for (int w = 0; w < 3; w++) {
                workers.add(Worker.traversal(executor, roots, store, log, NodeCodec.strings(), cluster, config("u", "w" + w)));
            }
            ClusterReport report = start(workers).get(0).join();

            assertEquals(RunStatus.DONE, report.status());
            assertEquals(expected.depth(), rounds(g, log));
            assertTrue(expected.depth().size() > roots.size());
            assertTrue(executor.runs.values().stream().allMatch(n -> n.get() == 1));

            IllegalArgumentException noRoots = assertThrows(IllegalArgumentException.class, () -> Worker.traversal(
                    executor, Set.of(), store, log, NodeCodec.strings(), cluster, config("v", "w")));
            assertTrue(noRoots.getMessage().contains("explicit roots"));
        }
    }

    @Test
    void anUndirectedStoreRunsOnlyIsolatedNodesInDependencyOrder() {
        Graph<String> g = Graph.undirected();
        g.addEdge("a", "b");
        g.addNode("solo");
        CompletionLog<String> log = new InMemoryCompletionLog<>();
        Topological executor = new Topological();
        try (InMemoryClusterStore cluster = new InMemoryClusterStore()) {
            ClusterReport report = Worker.topological(executor, InMemoryGraphStore.of(g, SHARDS, BY_KEY), log,
                    NodeCodec.strings(), cluster, config("i", "w").withMaxShards(SHARDS)).run();

            assertEquals(RunStatus.DONE, report.status());
            assertEquals(Set.of("solo"), executor.runs.keySet());
        }
    }

    @Test
    void loadingAStoreKeepsItsDirection() {
        Graph<String> g = Graph.undirected();
        g.addEdge("a", "b");
        g.addEdge("b", "c");
        g.addEdge("c", "c");
        g.addNode("solo");
        Graph<String> loaded = GraphStore.load(InMemoryGraphStore.of(g, SHARDS, BY_KEY), 2);

        assertFalse(loaded.isDirected());
        assertEquals(g.nodes(), Set.copyOf(loaded.nodes()));
        assertEquals(3, loaded.edgeCount());
        assertEquals(Set.of("a", "c"), loaded.successors("b"));
        assertTrue(GraphStore.load(InMemoryGraphStore.of(random(1, 10, 15, false), SHARDS, BY_KEY), 3).isDirected());
    }

    @Test
    void aDeadWorkersShardsAreTakenOverAndReplayedWithoutRerunningLoggedTasks() {
        Graph<String> g = random(3, 50, 120, false);
        Set<String> roots = Set.of("n0");
        TraversalReport<String> expected = new Traversal(roots).execute(g);

        GraphStore<String> store = InMemoryGraphStore.of(g, SHARDS, BY_KEY);
        try (InMemoryClusterStore cluster = new InMemoryClusterStore()) {
            CompletionLog<String> log = new InMemoryCompletionLog<>();
            Traversal dying = new Traversal(roots);
            Traversal healthy = new Traversal(roots);
            // the dying worker owns every shard; the first time it sends in round 1, after logging that
            // shard's tasks and sending one batch, it dies
            AtomicInteger deaths = new AtomicInteger();
            dying.crash = (shard, round, attempt) -> round == 1 && deaths.getAndIncrement() == 0;
            Map<String, AtomicInteger> runs = new ConcurrentHashMap<>();
            CompletableFuture<ClusterReport> died = start(List.of(Worker.traversal(dying, roots, store, log,
                    NodeCodec.strings(), cluster, config("c", "dying").withMaxShards(SHARDS)))).get(0);
            assertThrows(CompletionException.class, died::join, "the dying worker crashed");

            // its replacements start only now, so they take everything over from its expired leases
            ClusterReport report = start(List.of(
                    Worker.traversal(healthy, roots, store, log, NodeCodec.strings(), cluster, config("c", "w1")),
                    Worker.traversal(healthy, roots, store, log, NodeCodec.strings(), cluster, config("c", "w2"))))
                    .get(0).join();

            assertEquals(RunStatus.DONE, report.status());
            assertEquals(1, report.recoveries(), "only the shard round it died in was replayed");
            assertTrue(report.workers().containsAll(Set.of("w1", "w2")), "both replacements took shards: " + report.workers());
            assertEquals(expected.depth(), rounds(g, log));
            dying.runs.forEach((node, n) -> runs.computeIfAbsent(node, k -> new AtomicInteger()).addAndGet(n.get()));
            healthy.runs.forEach((node, n) -> runs.computeIfAbsent(node, k -> new AtomicInteger()).addAndGet(n.get()));
            assertTrue(runs.values().stream().allMatch(n -> n.get() == 1), "a logged task ran again: " + runs);
        }
    }

    @Test
    void aShardThatKeepsKillingItsWorkerFailsTheRun() {
        Graph<String> g = random(5, 20, 40, false);
        GraphStore<String> store = InMemoryGraphStore.of(g, SHARDS, BY_KEY);
        try (InMemoryClusterStore cluster = new InMemoryClusterStore()) {
            CompletionLog<String> log = new InMemoryCompletionLog<>();
            List<Worker<String>> workers = new ArrayList<>();
            for (int w = 0; w <= DistributedTraversalExecutor.MAX_ATTEMPTS; w++) {
                Traversal deadly = new Traversal(Set.of("n0"));
                deadly.crash = (shard, round, attempt) -> true;   // every send kills its worker
                workers.add(Worker.traversal(deadly, Set.of("n0"), store, log, NodeCodec.strings(), cluster,
                        config("x", "w" + w).withMaxShards(SHARDS)));
            }
            List<CompletableFuture<ClusterReport>> running = start(workers);
            CompletableFuture.allOf(running.toArray(CompletableFuture[]::new)).exceptionally(e -> null).join();

            ClusterStore.RunState state = cluster.run("x").orElseThrow();
            assertEquals(RunStatus.FAILED, state.status());
            assertTrue(state.error().contains("started " + (DistributedTraversalExecutor.MAX_ATTEMPTS + 1) + " times"),
                    state.error());
        }
    }

    @Test
    void aWorkerPresumedDeadCannotFinishItsOldRound() {
        MutableClock clock = new MutableClock();
        try (InMemoryClusterStore cluster = new InMemoryClusterStore(clock)) {
            Duration ttl = Duration.ofSeconds(10);
            cluster.createRun("z", 1, ttl);

            assertTrue(cluster.claim("z", 0, "old", ttl));
            assertFalse(cluster.claim("z", 0, "new", ttl), "a live lease is not taken");
            clock.advance(ttl);
            assertTrue(cluster.claim("z", 0, "new", ttl), "an expired lease is");

            assertFalse(cluster.arrive("z", new Arrival(0, 0, "old", 0, 1, 0, 0, 1, 1)));
            assertTrue(cluster.arrive("z", new Arrival(0, 0, "new", 1, 1, 0, 0, 1, 1)));
            cluster.advance("z", 0);
            assertEquals(1, cluster.run("z").orElseThrow().round());
        }
    }

    @Test
    void messagesAreKeptOncePerSenderReceiverAndRound() {
        try (InMemoryClusterStore cluster = new InMemoryClusterStore()) {
            cluster.createRun("m", 2, Duration.ofSeconds(1));
            cluster.send("m", 0, 0, 1, List.of("a", "b"));
            cluster.send("m", 0, 0, 1, List.of("a", "b"));   // a replay
            cluster.send("m", 0, 1, 1, List.of("b", "c"));

            assertEquals(Set.of("a", "b", "c"), cluster.inbox("m", 0, 1));
            assertEquals(Set.of(), cluster.inbox("m", 0, 0));
        }
    }

    @Test
    void graphStorePagesEachShardAndLoadsBackIntoAGraph() {
        Graph<String> g = random(1, 30, 60, false);
        GraphStore<String> store = InMemoryGraphStore.of(g, SHARDS, BY_KEY);
        int nodes = 0;
        for (int shard = 0; shard < SHARDS; shard++) {
            for (List<String> page = store.nodes(shard, null, 4); !page.isEmpty();
                 page = store.nodes(shard, page.get(page.size() - 1), 4)) {
                for (String node : page) {
                    assertEquals(shard, store.shardOf(node));
                    nodes++;
                }
            }
        }
        Graph<String> loaded = GraphStore.load(store, 4);

        assertEquals(g.nodeCount(), nodes);
        assertEquals(g.nodes(), loaded.nodes());
        assertEquals(g.edgeCount(), loaded.edgeCount());
        g.nodes().forEach(node -> assertEquals(g.successors(node), loaded.successors(node)));
    }

    private static final class MutableClock extends Clock {
        private Instant now = Instant.parse("2026-01-01T00:00:00Z");

        void advance(Duration by) {
            now = now.plus(by);
        }

        @Override
        public ZoneOffset getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(java.time.ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return now;
        }
    }
}
