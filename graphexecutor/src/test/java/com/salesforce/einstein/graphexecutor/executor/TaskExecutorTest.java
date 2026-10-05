package com.salesforce.einstein.graphexecutor.executor;

import com.salesforce.einstein.graphexecutor.executor.TopologicalTaskExecutor.CycleDetectedException;
import com.salesforce.einstein.graphexecutor.graph.Graph;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.NotSerializableException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TaskExecutorTest {
    private final ExecutorService pool = Executors.newFixedThreadPool(4);
    private final ExecutorService taskPool = Executors.newFixedThreadPool(4);   // tasks of parallel groups

    @AfterEach
    void shutdown() throws InterruptedException {
        pool.shutdownNow();
        taskPool.shutdownNow();
        assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS));
        assertTrue(taskPool.awaitTermination(5, TimeUnit.SECONDS));
    }

    /** Collects each group's node set; the simplest possible subclass. */
    private final class NodeSetExecutor extends TaskExecutor<String, Set<String>> {
        NodeSetExecutor() {
            super(pool);
        }

        @Override
        protected Set<String> processGroup(TaskGroup<String> group) {
            return Set.copyOf(group.nodes());
        }
    }

    /**
     * Three groups: {a, b, c, d} (joined only through c: a -> c <- b, plus c -> d),
     * {x, y} and the isolated {z}.
     */
    private static Graph<String> threeGroups() {
        Graph<String> g = Graph.directed();
        g.addEdge("a", "c");
        g.addEdge("x", "y");
        g.addEdge("b", "c");
        g.addEdge("c", "d");
        g.addNode("z");
        return g;
    }

    @Test
    void groupsAreWeaklyConnectedComponents() {
        List<GroupResult<String, Set<String>>> results = new NodeSetExecutor().execute(threeGroups());

        assertEquals(List.of(Set.of("a", "b", "c", "d"), Set.of("x", "y"), Set.of("z")),
                results.stream().map(GroupResult::value).toList());
        assertEquals(List.of(0, 1, 2), results.stream().map(r -> r.group().id()).toList());
        // each group carries only its own edges
        assertEquals(3, results.get(0).group().graph().edgeCount());
        assertEquals(1, results.get(1).group().graph().edgeCount());
    }

    @Test
    void emptyGraphHasNoGroups() {
        assertEquals(List.of(), new NodeSetExecutor().execute(Graph.directed()));
    }

    @Test
    void groupsRunConcurrently() {
        // every group blocks until all three are in flight at once; serial execution would time out
        CountDownLatch allStarted = new CountDownLatch(3);
        TaskExecutor<String, Boolean> executor = new TaskExecutor<>(pool) {
            @Override
            protected Boolean processGroup(TaskGroup<String> group) throws InterruptedException {
                allStarted.countDown();
                return allStarted.await(5, TimeUnit.SECONDS);
            }
        };
        assertTrue(executor.execute(threeGroups()).stream().allMatch(GroupResult::value));
    }

    @Test
    void failingGroupDoesNotAffectOthers() {
        TaskExecutor<String, Integer> executor = new TaskExecutor<>(pool) {
            @Override
            protected Integer processGroup(TaskGroup<String> group) {
                if (group.nodes().contains("x")) {
                    throw new IllegalStateException("boom");
                }
                return group.size();
            }
        };
        List<GroupResult<String, Integer>> results = executor.execute(threeGroups());

        assertTrue(results.get(0).isSuccess());
        assertEquals(4, results.get(0).value());
        assertFalse(results.get(1).isSuccess());
        assertInstanceOf(IllegalStateException.class, results.get(1).error());
        assertEquals(1, results.get(2).value());
    }

    @Test
    void callerMayModifyGraphOnceSubmitReturns() {
        CountDownLatch release = new CountDownLatch(1);
        TaskExecutor<String, Set<String>> executor = new TaskExecutor<>(pool) {
            @Override
            protected Set<String> processGroup(TaskGroup<String> group) throws InterruptedException {
                release.await();
                return Set.copyOf(group.nodes());
            }
        };
        Graph<String> g = threeGroups();
        var future = executor.submit(g);
        g.removeNode("c");
        g.addEdge("y", "q");
        release.countDown();

        assertEquals(List.of(Set.of("a", "b", "c", "d"), Set.of("x", "y"), Set.of("z")),
                future.join().stream().map(GroupResult::value).toList());
    }

    // ---- TopologicalTaskExecutor ----

    private final class RecordingExecutor extends TopologicalTaskExecutor<String> {
        final Map<String, String> threadByTask = new ConcurrentHashMap<>();

        RecordingExecutor() {
            super(pool);
        }

        RecordingExecutor(IneligibleGroupPolicy policy) {
            super(pool, policy);
        }

        @Override
        protected void executeTask(String task) {
            if (task.equals("fail")) {
                throw new IllegalArgumentException(task);
            }
            threadByTask.put(task, Thread.currentThread().getName());
        }
    }

    @Test
    void tasksRunInDependencyOrderWithinEachGroup() {
        // build -> test -> deploy, lint -> test ; independent: fetch -> parse
        Graph<String> g = Graph.directed();
        g.addEdge("test", "deploy");
        g.addEdge("build", "test");
        g.addEdge("lint", "test");
        g.addEdge("fetch", "parse");

        RecordingExecutor executor = new RecordingExecutor();
        List<GroupResult<String, List<String>>> results = executor.execute(g);

        assertEquals(List.of("build", "lint", "test", "deploy"), results.get(0).value());
        assertEquals(List.of("fetch", "parse"), results.get(1).value());
        // one group = one thread
        assertEquals(1, Set.of("build", "lint", "test", "deploy").stream()
                .map(executor.threadByTask::get).collect(Collectors.toSet()).size());
    }

    @Test
    void isolatePolicyFailsOnlyTheCyclicGroupAndRunsTheRest() {
        Graph<String> g = Graph.directed();
        g.addEdge("start", "p");
        g.addEdge("p", "q");
        g.addEdge("q", "p");      // cycle p <-> q
        g.addEdge("q", "after");
        g.addEdge("ok1", "ok2");

        RecordingExecutor executor = new RecordingExecutor(IneligibleGroupPolicy.ISOLATE_GROUP);
        List<GroupResult<String, List<String>>> results = executor.execute(g);

        CycleDetectedException cycle = assertInstanceOf(CycleDetectedException.class, results.get(0).error());
        List<String> blocked = executor.blockedNodes(cycle);   // typed, no cast at the call site
        assertEquals(List.of("p", "q", "after"), blocked);
        assertFalse(executor.threadByTask.containsKey("start"));   // nothing from the cyclic group ran
        assertEquals(List.of("ok1", "ok2"), results.get(1).value());
    }

    @Test
    void cycleRejectsWholeGraphByDefaultAndNothingRuns() {
        Graph<String> g = Graph.directed();
        g.addEdge("a", "b");
        g.addEdge("b", "a");      // cycle in group 0
        g.addEdge("ok1", "ok2");  // group 1 is fine, but must not run either
        g.addEdge("x", "x");      // self-loop: a cycle in group 2

        RecordingExecutor executor = new RecordingExecutor();
        assertEquals(IneligibleGroupPolicy.REJECT_GRAPH, executor.policy());

        IneligibleGraphException rejected = assertThrows(IneligibleGraphException.class, () -> executor.submit(g));
        assertEquals(List.of(0, 2), rejected.rejected().stream().map(r -> r.group().id()).toList());
        CycleDetectedException cycle = assertInstanceOf(CycleDetectedException.class, rejected.rejected().get(0).error());
        assertEquals(List.of("a", "b"), cycle.blocked());
        assertEquals(2, rejected.getSuppressed().length);
        assertTrue(executor.threadByTask.isEmpty());   // not even the acyclic group ran
    }

    @Test
    void blockedNodesRefusesExceptionFromAnotherExecutor() {
        Graph<String> g = Graph.directed();
        g.addEdge("a", "a");
        RecordingExecutor raiser = new RecordingExecutor(IneligibleGroupPolicy.ISOLATE_GROUP);
        CycleDetectedException cycle = (CycleDetectedException) raiser.execute(g).get(0).error();

        assertEquals(List.of("a"), raiser.blockedNodes(cycle));
        // another executor (here even with the same T) must not vouch for nodes it never saw
        assertThrows(IllegalArgumentException.class, () -> new RecordingExecutor().blockedNodes(cycle));
    }

    @Test
    void cycleExceptionSurvivesSerializationWithSerializableNodes() throws Exception {
        Graph<String> g = Graph.directed();
        g.addEdge("a", "b");
        g.addEdge("b", "a");
        RecordingExecutor executor = new RecordingExecutor(IneligibleGroupPolicy.ISOLATE_GROUP);
        CycleDetectedException original = (CycleDetectedException) executor.execute(g).get(0).error();

        CycleDetectedException copy = (CycleDetectedException) roundTrip(original);

        assertEquals(List.of("a", "b"), copy.blocked());
        assertEquals(original.getMessage(), copy.getMessage());
        // no executor travels with it, so none can vouch for its node type
        IllegalArgumentException refused = assertThrows(IllegalArgumentException.class,
                () -> executor.blockedNodes(copy));
        assertTrue(refused.getMessage().contains("deserialized"));
    }

    @Test
    void cycleExceptionWithNonSerializableNodesFailsLoudlyWhenSerialized() {
        final class Opaque {   // deliberately not Serializable
            @Override
            public String toString() {
                return "opaque";
            }
        }
        Graph<Opaque> g = Graph.directed();
        Opaque node = new Opaque();
        g.addEdge(node, node);
        TopologicalTaskExecutor<Opaque> executor = new TopologicalTaskExecutor<>(pool, IneligibleGroupPolicy.ISOLATE_GROUP) {
            @Override
            protected void executeTask(Opaque task) {
            }
        };
        Throwable cycle = executor.execute(g).get(0).error();

        NotSerializableException e = assertThrows(NotSerializableException.class, () -> roundTrip(cycle));
        assertTrue(e.getMessage().contains("Opaque"));
    }

    private static Object roundTrip(Object value) throws IOException, ClassNotFoundException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(value);
        }
        try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            return in.readObject();
        }
    }

    @Test
    void acyclicGraphPassesStrictPolicy() {
        Graph<String> g = Graph.directed();
        g.addEdge("a", "b");
        g.addEdge("a", "c");
        g.addEdge("b", "c");   // a diamond-ish DAG: shared descendant, no cycle

        assertEquals(List.of("a", "b", "c"), new RecordingExecutor().execute(g).get(0).value());
    }

    @Test
    void baseExecutorValidationHookAndPolicies() {
        class SizeLimited extends TaskExecutor<String, Integer> {
            final Set<Integer> processed = ConcurrentHashMap.newKeySet();

            SizeLimited(IneligibleGroupPolicy policy) {
                super(pool, policy);
            }

            @Override
            protected void validateGroup(TaskGroup<String> group) {
                if (group.size() > 2) {
                    throw new IllegalStateException("too big: " + group.size());
                }
            }

            @Override
            protected Integer processGroup(TaskGroup<String> group) {
                processed.add(group.id());
                return group.size();
            }
        }
        Graph<String> g = threeGroups();   // sizes 4, 2, 1

        SizeLimited strict = new SizeLimited(IneligibleGroupPolicy.REJECT_GRAPH);
        assertThrows(IneligibleGraphException.class, () -> strict.submit(g));
        assertTrue(strict.processed.isEmpty());

        SizeLimited lenient = new SizeLimited(IneligibleGroupPolicy.ISOLATE_GROUP);
        List<GroupResult<String, Integer>> results = lenient.execute(g);
        assertInstanceOf(IllegalStateException.class, results.get(0).error());
        assertEquals(List.of(2, 1), List.of(results.get(1).value(), results.get(2).value()));
        assertEquals(Set.of(1, 2), lenient.processed);   // the rejected group never reached processGroup
    }

    @Test
    void failingTaskSkipsRestOfItsGroup() {
        Graph<String> g = Graph.directed();
        g.addEdge("first", "fail");
        g.addEdge("fail", "never");
        g.addNode("solo");

        RecordingExecutor executor = new RecordingExecutor();
        List<GroupResult<String, List<String>>> results = executor.execute(g);

        assertInstanceOf(IllegalArgumentException.class, results.get(0).error());
        assertTrue(executor.threadByTask.containsKey("first"));
        assertFalse(executor.threadByTask.containsKey("never"));
        assertEquals(List.of("solo"), results.get(1).value());
    }

    // ---- TopologicalTaskExecutor, parallel inside a group ----

    /** A task body that may throw, as {@link TopologicalTaskExecutor#executeTask} may. */
    @FunctionalInterface
    private interface Task {
        void run(String task) throws Exception;
    }

    /** Runs each group's independent tasks in parallel on {@link #taskPool}. */
    private TopologicalTaskExecutor<String> parallel(Task body) {
        return new TopologicalTaskExecutor<>(pool, taskPool, IneligibleGroupPolicy.REJECT_GRAPH) {
            @Override
            protected void executeTask(String task) throws Exception {
                body.run(task);
            }
        };
    }

    @Test
    void independentTasksInsideAGroupRunInParallel() {
        // diamond a -> {b, c} -> d: b and c each wait until both are running, so serial would time out
        Graph<String> g = Graph.directed();
        g.addEdge("a", "b");
        g.addEdge("a", "c");
        g.addEdge("b", "d");
        g.addEdge("c", "d");
        CountDownLatch bothRunning = new CountDownLatch(2);
        TopologicalTaskExecutor<String> executor = parallel(task -> {
            if (task.equals("b") || task.equals("c")) {
                bothRunning.countDown();
                assertTrue(bothRunning.await(5, TimeUnit.SECONDS));
            }
        });
        List<String> order = executor.execute(g).get(0).value();

        assertEquals("a", order.get(0));
        assertEquals(Set.of("b", "c"), Set.copyOf(order.subList(1, 3)));
        assertEquals("d", order.get(3));
    }

    @Test
    void parallelGroupStopsReleasingAfterAFailure() {
        Graph<String> g = Graph.directed();
        g.addEdge("first", "fail");
        g.addEdge("fail", "never");
        Set<String> ran = ConcurrentHashMap.newKeySet();
        TopologicalTaskExecutor<String> executor = parallel(task -> {
            ran.add(task);
            if (task.equals("fail")) {
                throw new IllegalStateException(task);
            }
        });
        GroupResult<String, List<String>> result = executor.execute(g).get(0);

        assertInstanceOf(IllegalStateException.class, result.error());
        assertEquals(Set.of("first", "fail"), ran);
    }

    @Test
    void parallelModeRefusesTheGroupPoolAsTaskPool() {
        assertThrows(IllegalArgumentException.class, () -> new TopologicalTaskExecutor<String>(pool, pool,
                IneligibleGroupPolicy.REJECT_GRAPH) {
            @Override
            protected void executeTask(String task) {
            }
        });
    }

    @Test
    void rejectsUndirectedGraph() {
        Graph<String> g = Graph.undirected();
        g.addEdge("a", "b");
        assertThrows(IllegalArgumentException.class, () -> new NodeSetExecutor().submit(g));
    }

    @Test
    void weightedGraphGroupsKeepTheirWeights() {
        Graph<String> g = Graph.weightedDirected();
        g.addEdge("a", "b", 2.5);
        g.addEdge("x", "y", 7);
        TaskExecutor<String, Optional<Double>> executor = new TaskExecutor<>(pool) {
            @Override
            protected Optional<Double> processGroup(TaskGroup<String> group) {
                assertTrue(group.graph().isWeighted());
                String from = group.nodes().iterator().next();
                return group.graph().edgeWeight(from, group.graph().successors(from).iterator().next());
            }
        };
        assertEquals(List.of(Optional.of(2.5), Optional.of(7.0)),
                executor.execute(g).stream().map(GroupResult::value).toList());
    }
}
