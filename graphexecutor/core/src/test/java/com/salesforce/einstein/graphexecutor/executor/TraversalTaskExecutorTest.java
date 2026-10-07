package com.salesforce.einstein.graphexecutor.executor;

import com.salesforce.einstein.ds.graph.Graph;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TraversalTaskExecutorTest {
    private final ExecutorService groupPool = Executors.newFixedThreadPool(2);
    private final ExecutorService taskPool = Executors.newFixedThreadPool(4);

    @AfterEach
    void shutdown() throws InterruptedException {
        groupPool.shutdownNow();
        taskPool.shutdownNow();
        assertTrue(groupPool.awaitTermination(5, TimeUnit.SECONDS));
        assertTrue(taskPool.awaitTermination(5, TimeUnit.SECONDS));
    }

    /** Counts runs, fails nodes named "fail*", and tracks how many tasks of each lane run at once. */
    private static class Recording extends TraversalTaskExecutor<String> {
        final Map<String, AtomicInteger> runs = new ConcurrentHashMap<>();
        final Map<Object, AtomicInteger> laneInFlight = new ConcurrentHashMap<>();
        final Map<Object, Integer> laneMaxInFlight = new ConcurrentHashMap<>();
        final Map<Object, List<Long>> laneStarts = new ConcurrentHashMap<>();
        Set<String> rejected = Set.of();
        int maxDepth = Integer.MAX_VALUE;
        Function<String, Object> lane = node -> node;
        Duration delay = Duration.ZERO;
        Set<String> roots;   // null: the default
        boolean startRootless = true;
        Map<String, CountDownLatch> rendezvous = Map.of();   // node -> latch it counts down and awaits

        Recording(ExecutorService pool) {
            super(pool, IneligibleGroupPolicy.REJECT_GRAPH);
        }

        Recording(ExecutorService groupPool, ExecutorService taskPool) {
            super(groupPool, taskPool, IneligibleGroupPolicy.REJECT_GRAPH);
        }

        @Override
        protected void executeTask(String node, int depth) throws Exception {
            Object myLane = lane.apply(node);
            laneStarts.computeIfAbsent(myLane, l -> Collections.synchronizedList(new ArrayList<>())).add(System.nanoTime());
            int inFlight = laneInFlight.computeIfAbsent(myLane, l -> new AtomicInteger()).incrementAndGet();
            laneMaxInFlight.merge(myLane, inFlight, Math::max);
            try {
                runs.computeIfAbsent(node, n -> new AtomicInteger()).incrementAndGet();
                CountDownLatch latch = rendezvous.get(node);
                if (latch != null) {
                    latch.countDown();
                    if (!latch.await(5, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("rendezvous timed out: lanes did not run concurrently");
                    }
                }
                if (node.startsWith("fail")) {
                    throw new IllegalStateException(node);
                }
            } finally {
                laneInFlight.get(myLane).decrementAndGet();
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
            return !rejected.contains(node);
        }

        @Override
        protected int maxDepth() {
            return maxDepth;
        }

        @Override
        protected Object laneOf(String node) {
            return lane.apply(node);
        }

        @Override
        protected Duration laneDelay() {
            return delay;
        }
    }

    private static Graph<String> graph(String... edges) {
        Graph<String> g = Graph.directed();
        for (String edge : edges) {
            String[] ends = edge.split("->");
            g.addEdge(ends[0], ends[1]);
        }
        return g;
    }

    private static TraversalResult<String> only(List<GroupResult<String, TraversalResult<String>>> results) {
        assertEquals(1, results.size());
        assertTrue(results.get(0).isSuccess(), () -> String.valueOf(results.get(0).error()));
        return results.get(0).value();
    }

    @Test
    void cyclesAreFineAndEveryNodeRunsOnce() {
        Recording executor = new Recording(groupPool, taskPool);
        // no node without predecessors: the group starts at its first node
        TraversalResult<String> result = only(executor.execute(graph("a->b", "b->c", "c->a", "b->d", "d->b")));

        assertEquals(Map.of("a", 0, "b", 1, "c", 2, "d", 2), result.depth());
        assertEquals(Set.of("a", "b", "c", "d"), Set.copyOf(result.completed()));
        executor.runs.values().forEach(count -> assertEquals(1, count.get()));
    }

    @Test
    void depthIsTheShortestPathNotTheLongestChain() {
        TraversalResult<String> result = only(new Recording(groupPool).execute(graph("a->b", "b->c", "a->c")));
        assertEquals(1, result.depth().get("c"));   // Kahn's waves would put c at 2
    }

    @Test
    void sequentialModeRunsInBfsOrder() {
        TraversalResult<String> result = only(new Recording(groupPool)
                .execute(graph("a->b", "a->c", "b->d", "c->e", "d->a")));
        assertEquals(List.of("a", "b", "c", "d", "e"), result.completed());
    }

    @Test
    void nodesBeyondMaxDepthAreReachedButNotRun() {
        Recording executor = new Recording(groupPool, taskPool);
        executor.maxDepth = 1;
        TraversalResult<String> result = only(executor.execute(graph("a->b", "b->c", "c->d")));

        assertEquals(Set.of("a", "b"), Set.copyOf(result.completed()));
        assertEquals(List.of("c", "d"), result.tooDeep());
        assertEquals(Map.of("a", 0, "b", 1, "c", 2, "d", 3), result.depth());
        assertEquals(Set.of("a", "b"), executor.runs.keySet());
    }

    @Test
    void anExcludedNodeIsNeitherRunNorTraversedThrough() {
        Recording executor = new Recording(groupPool, taskPool);
        executor.rejected = Set.of("img");
        TraversalResult<String> result = only(executor.execute(graph("a->img", "img->b", "a->c")));

        assertEquals(List.of("img"), result.excluded());
        assertEquals(List.of("b"), result.unreachable());   // its only way in was through img
        assertEquals(Set.of("a", "c"), executor.runs.keySet());
    }

    @Test
    void nodesNotReachableFromTheRootsAreReported() {
        Recording executor = new Recording(groupPool, taskPool);
        executor.roots = Set.of("home");
        TraversalResult<String> result = only(executor.execute(graph("home->about", "orphan->about")));

        assertEquals(List.of("orphan"), result.unreachable());
        assertEquals(Set.of("home", "about"), executor.runs.keySet());
    }

    @Test
    void aFailureBlocksNothing() {
        TraversalResult<String> result = only(new Recording(groupPool, taskPool).execute(graph("a->fail", "fail->b")));

        assertEquals(Set.of("fail"), result.failed().keySet());
        assertEquals(Set.of("a", "b"), Set.copyOf(result.completed()));
    }

    @Test
    void oneTaskPerLaneAtATimeWhileLanesRunConcurrently() {
        Graph<String> g = Graph.directed();
        for (int i = 0; i < 6; i++) {
            g.addEdge("root", "x" + i);
            g.addEdge("root", "y" + i);
        }
        Recording executor = new Recording(groupPool, taskPool);
        executor.lane = node -> node.substring(0, 1);   // lanes r, x, y
        CountDownLatch bothLanes = new CountDownLatch(2);
        executor.rendezvous = Map.of("x0", bothLanes, "y0", bothLanes);   // succeeds only if x and y overlap

        TraversalResult<String> result = only(executor.execute(g));

        assertEquals(13, result.completed().size());
        assertTrue(result.failed().isEmpty(), () -> result.failed().toString());
        executor.laneMaxInFlight.forEach((lane, max) -> assertEquals(1, max, "lane " + lane));
    }

    @Test
    void tasksOfOneLaneAreAtLeastTheDelayApart() {
        for (Recording executor : List.of(new Recording(groupPool), new Recording(groupPool, taskPool))) {
            executor.lane = node -> "host";
            executor.delay = Duration.ofMillis(40);
            only(executor.execute(graph("a->b", "a->c")));

            List<Long> starts = executor.laneStarts.get("host");
            assertEquals(3, starts.size());
            for (int i = 1; i < starts.size(); i++) {
                long gap = starts.get(i) - starts.get(i - 1);
                assertTrue(gap >= Duration.ofMillis(40).toNanos(), "gap " + gap + "ns");
            }
        }
    }

    @Test
    void aGroupWithoutADeclaredRootStartsAtItsFirstNodeUnlessThatIsTurnedOff() {
        Graph<String> g = graph("home->about", "x->y", "y->x");   // the cycle is a separate group with no root
        Recording starting = new Recording(groupPool, taskPool);
        starting.roots = Set.of("home");
        assertEquals(Set.of("home", "about", "x", "y"), runsOf(starting, g));

        Recording strict = new Recording(groupPool, taskPool);
        strict.roots = Set.of("home");
        strict.startRootless = false;
        List<GroupResult<String, TraversalResult<String>>> results = strict.execute(g);
        assertEquals(Set.of("home", "about"), strict.runs.keySet());
        assertEquals(List.of("x", "y"), results.get(1).value().unreachable());
    }

    private static Set<String> runsOf(Recording executor, Graph<String> g) {
        executor.execute(g);
        return executor.runs.keySet();
    }

    @Test
    void independentSubgraphsAreSeparateGroups() {
        List<GroupResult<String, TraversalResult<String>>> results =
                new Recording(groupPool, taskPool).execute(graph("a->b", "b->a", "c->d"));
        assertEquals(2, results.size());
        assertEquals(Set.of("a", "b"), Set.copyOf(results.get(0).value().completed()));
        assertEquals(Set.of("c", "d"), Set.copyOf(results.get(1).value().completed()));
    }

    @Test
    void theTaskPoolMustDifferFromTheGroupPool() {
        assertThrows(IllegalArgumentException.class, () -> new Recording(groupPool, groupPool));
    }
}
