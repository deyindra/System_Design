package com.salesforce.einstein.graphexecutor.executor;

import com.salesforce.einstein.ds.graph.Graph;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A {@link TaskExecutor} for reachability graphs: an edge {@code a -> b} means "b is reachable from a"
 * (a links to b), <em>not</em> "b depends on a". Every node reachable from the roots runs exactly once,
 * at its shortest distance from a root; nothing waits for anything else. The fit for crawling a known
 * link graph, fanning out over a call graph, or any visit-everything-once workload.
 *
 * <p>Compared with {@link TopologicalTaskExecutor}, where an edge is a dependency:
 * <ul>
 *   <li><b>cycles are fine</b>: no group is ineligible, a node on a cycle still runs once;</li>
 *   <li><b>depth is the shortest path</b> (BFS level), not the longest dependency chain;</li>
 *   <li><b>a failure blocks nothing</b>: the failed node's successors are known from the graph and still run.</li>
 * </ul>
 *
 * <p>{@link #processGroup}:
 * <ol>
 *   <li>BFS from the group's {@link #roots} (O(V + E)), only through {@link #admit admitted} nodes, giving
 *       each reached node its depth. A group with no admitted root starts at its first admitted node.</li>
 *   <li>Runs every reached node no deeper than {@link #maxDepth}, in BFS order within each <b>lane</b>
 *       ({@link #laneOf}): one task per lane at a time, {@link #laneDelay} apart. Different lanes run
 *       concurrently on the task pool (if given). Lanes are how a crawler is polite: lane = host.</li>
 * </ol>
 *
 * <p>Every hook except {@link #executeTask} runs on the group's thread before any task starts, so hooks
 * need not be thread-safe.
 *
 * <p>The group result is a {@link TraversalResult}; the group itself fails only if the pool rejects work,
 * the coordinator is interrupted, or a task throws an {@link Error}.
 */
public abstract class TraversalTaskExecutor<T> extends TaskExecutor<T, TraversalResult<T>> {
    private final Executor taskPool;   // null: a group's tasks run one at a time on its group thread

    protected TraversalTaskExecutor(Executor executor, IneligibleGroupPolicy policy) {
        super(executor, policy);
        this.taskPool = null;
    }

    /**
     * Runs a group's lanes in parallel on {@code taskPool}.
     *
     * @param groupPool runs one coordinator per group; it blocks until its group is done
     * @param taskPool  runs the tasks. Must differ from {@code groupPool}: blocked coordinators could
     *                  otherwise occupy every thread and starve their own tasks (deadlock)
     */
    protected TraversalTaskExecutor(Executor groupPool, Executor taskPool, IneligibleGroupPolicy policy) {
        super(groupPool, policy);
        if (taskPool == groupPool) {
            throw new IllegalArgumentException("taskPool must differ from groupPool, or blocked groups starve their tasks");
        }
        this.taskPool = Objects.requireNonNull(taskPool, "taskPool");
    }

    /**
     * Runs one node. Its throwing fails only this node; its lane continues.
     *
     * @param depth the node's shortest distance from a root
     */
    protected abstract void executeTask(T node, int depth) throws Exception;

    /**
     * Where traversal starts, given a group's graph. Default: nodes with no predecessors. Nodes not in
     * the graph are ignored. For a group with no admitted root, see {@link #startRootlessGroups()}.
     */
    protected Set<T> roots(Graph<T> graph) {
        Set<T> roots = new HashSet<>();
        for (T node : graph.nodes()) {
            if (graph.inDegree(node) == 0) {
                roots.add(node);
            }
        }
        return roots;
    }

    /**
     * Whether a group with no admitted root starts at its first admitted node. True (the default) suits
     * the default {@link #roots}: a cycle nothing points into still runs. False when roots are declared
     * explicitly: such a group is then reported as unreachable.
     */
    protected boolean startRootlessGroups() {
        return true;
    }

    /** Whether a node may run. A rejected node is never run and never traversed through. Default: all. */
    protected boolean admit(T node) {
        return true;
    }

    /** Nodes deeper than this are reached but not run (reported as too deep). Default: unlimited. */
    protected int maxDepth() {
        return Integer.MAX_VALUE;
    }

    /**
     * Nodes with equal lanes run one at a time, {@link #laneDelay} apart. Default: the node itself, so
     * nothing is serialized.
     */
    protected Object laneOf(T node) {
        return node;
    }

    /** The minimum gap between the start of one task and the start of the next in the same lane. Default: 0. */
    protected Duration laneDelay() {
        return Duration.ZERO;
    }

    @Override
    protected final TraversalResult<T> processGroup(TaskGroup<T> group) throws Exception {
        Graph<T> graph = group.graph();
        Plan<T> plan = plan(graph);
        Executor pool = taskPool;   // read once, so the null check and the use cannot disagree
        Outcomes<T> outcomes = pool == null ? runSequential(plan) : runParallel(plan, pool);

        Map<T, Throwable> failed = new LinkedHashMap<>();
        for (T node : plan.depth.keySet()) {
            Throwable error = outcomes.failed.get(node);
            if (error != null) {
                failed.put(node, error);
            }
        }
        return new TraversalResult<>(List.copyOf(outcomes.completed), Collections.unmodifiableMap(plan.depth),
                Collections.unmodifiableMap(failed), plan.excluded, plan.tooDeep, plan.unreachable);
    }

    /** The BFS, lanes and every hook's answer, computed before anything runs. */
    private record Plan<T>(Map<T, Integer> depth, List<Deque<T>> lanes, Duration delay, List<T> excluded,
                           List<T> tooDeep, List<T> unreachable) {
    }

    private Plan<T> plan(Graph<T> graph) {
        Set<T> admitted = new HashSet<>();
        List<T> excluded = new ArrayList<>();
        for (T node : graph.nodes()) {
            if (admit(node)) {
                admitted.add(node);
            } else {
                excluded.add(node);
            }
        }
        Set<T> declared = roots(graph);
        List<T> roots = graph.nodes().stream().filter(n -> declared.contains(n) && admitted.contains(n)).toList();
        if (roots.isEmpty() && startRootlessGroups()) {
            roots = graph.nodes().stream().filter(admitted::contains).limit(1).toList();
        }

        // BFS through admitted nodes; a node's first discovery is its shortest distance
        Map<T, Integer> depth = new LinkedHashMap<>();
        Deque<T> queue = new ArrayDeque<>();
        for (T root : roots) {
            depth.put(root, 0);
            queue.add(root);
        }
        while (!queue.isEmpty()) {
            T node = queue.poll();
            int next = depth.get(node) + 1;
            for (T successor : graph.successors(node)) {
                if (admitted.contains(successor) && !depth.containsKey(successor)) {
                    depth.put(successor, next);
                    queue.add(successor);
                }
            }
        }

        int maxDepth = maxDepth();
        List<T> tooDeep = new ArrayList<>();
        Map<Object, Deque<T>> lanes = new LinkedHashMap<>();
        depth.forEach((node, d) -> {
            if (d > maxDepth) {
                tooDeep.add(node);
            } else {
                lanes.computeIfAbsent(laneOf(node), lane -> new ArrayDeque<>()).add(node);   // BFS order = depth order
            }
        });
        List<T> unreachable = graph.nodes().stream()
                .filter(n -> admitted.contains(n) && !depth.containsKey(n)).toList();
        Duration delay = Objects.requireNonNull(laneDelay(), "laneDelay");
        if (delay.isNegative()) {
            throw new IllegalArgumentException("laneDelay must be >= 0");
        }
        return new Plan<>(depth, new ArrayList<>(lanes.values()), delay, List.copyOf(excluded),
                List.copyOf(tooDeep), unreachable);
    }

    private record Outcomes<T>(Queue<T> completed, Map<T, Throwable> failed) {
    }

    /** One task at a time, in BFS order; a lane's next task waits out the delay since that lane's last start. */
    private Outcomes<T> runSequential(Plan<T> plan) throws InterruptedException {
        Outcomes<T> outcomes = new Outcomes<>(new ArrayDeque<>(), new HashMap<>());
        long delayNanos = plan.delay.toNanos();
        Map<T, Integer> laneIndex = new HashMap<>();
        for (int lane = 0; lane < plan.lanes.size(); lane++) {
            for (T node : plan.lanes.get(lane)) {
                laneIndex.put(node, lane);
            }
        }
        long[] lastStart = new long[plan.lanes.size()];
        boolean[] started = new boolean[plan.lanes.size()];
        for (Map.Entry<T, Integer> entry : plan.depth.entrySet()) {
            Integer lane = laneIndex.get(entry.getKey());
            if (lane == null) {
                continue;   // too deep
            }
            if (started[lane]) {
                long wait = lastStart[lane] + delayNanos - System.nanoTime();
                if (wait > 0) {
                    TimeUnit.NANOSECONDS.sleep(wait);
                }
            }
            started[lane] = true;
            lastStart[lane] = System.nanoTime();
            try {
                executeTask(entry.getKey(), entry.getValue());
                outcomes.completed.add(entry.getKey());
            } catch (InterruptedException e) {
                throw e;   // the group is being cancelled, not this task failing
            } catch (Exception e) {
                outcomes.failed.put(entry.getKey(), e);
            }
        }
        return outcomes;
    }

    /** Each lane is a chain of tasks on {@code pool}; lanes run concurrently. */
    private Outcomes<T> runParallel(Plan<T> plan, Executor pool) throws Exception {
        ParallelRun run = new ParallelRun(plan, pool);
        run.start();
        try {
            run.finished.get();
        } catch (InterruptedException e) {
            run.fatal.compareAndSet(null, e);   // stop starting new tasks
            throw e;
        } catch (ExecutionException e) {
            throw new IllegalStateException(e.getCause());   // finished is only ever completed normally
        }
        Throwable fatal = run.fatal.get();
        if (fatal instanceof Exception e) {
            throw e;
        }
        if (fatal instanceof Error e) {
            throw e;
        }
        return run.outcomes;
    }

    /** State of one parallel group run, shared by its lanes. */
    private final class ParallelRun {
        final Plan<T> plan;
        final Executor pool;
        final Executor delayed;   // the pool, after laneDelay; same as pool when there is no delay
        final Outcomes<T> outcomes = new Outcomes<>(new ConcurrentLinkedQueue<>(), new ConcurrentHashMap<>());
        final AtomicReference<Throwable> fatal = new AtomicReference<>();   // fails the whole group
        final AtomicInteger lanesLeft;
        final CompletableFuture<Void> finished = new CompletableFuture<>();

        ParallelRun(Plan<T> plan, Executor pool) {
            this.plan = plan;
            this.pool = pool;
            this.delayed = plan.delay.isZero() ? pool
                    : CompletableFuture.delayedExecutor(plan.delay.toNanos(), TimeUnit.NANOSECONDS, pool);
            this.lanesLeft = new AtomicInteger(plan.lanes.size());
        }

        void start() {
            if (plan.lanes.isEmpty()) {
                finished.complete(null);
                return;
            }
            for (Deque<T> lane : plan.lanes) {
                schedule(lane, pool);
            }
        }

        // a lane's deque is only touched by the one task of that lane in flight; the executor hand-off orders them
        private void schedule(Deque<T> lane, Executor executor) {
            try {
                executor.execute(() -> runNext(lane));
            } catch (RuntimeException rejected) {   // e.g. the pool was shut down
                fatal.compareAndSet(null, rejected);
                laneDone();
            }
        }

        private void runNext(Deque<T> lane) {
            T node = lane.poll();
            if (node == null || fatal.get() != null) {
                laneDone();
                return;
            }
            try {
                executeTask(node, plan.depth.get(node));
                outcomes.completed.add(node);
            } catch (Exception e) {
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
                outcomes.failed.put(node, e);
            } catch (Error e) {
                fatal.compareAndSet(null, e);
            }
            if (lane.isEmpty()) {
                laneDone();
            } else {
                schedule(lane, delayed);   // the delay runs from this task's end: at least laneDelay between starts
            }
        }

        private void laneDone() {
            if (lanesLeft.decrementAndGet() == 0) {
                finished.complete(null);
            }
        }
    }
}
