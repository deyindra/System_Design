package com.salesforce.einstein.graphexecutor.executor;

import com.salesforce.einstein.ds.graph.Graph;

import java.io.IOException;
import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.Serial;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A {@link TaskExecutor} for dependency graphs: an edge {@code a -> b} means "a must finish before b
 * starts". Groups run in parallel. Within a group, tasks run either one at a time in topological order
 * (the default) or, given a separate task pool, in parallel: each task is submitted the moment its last
 * dependency finishes (Kahn's ready set driven by completions).
 *
 * <p><b>Cycles make a group ineligible.</b> {@link #validateGroup} runs Kahn's algorithm (O(V + E)) on
 * every group on the calling thread, before anything is dispatched. A group with a cycle has no valid
 * order and is rejected with {@link CycleDetectedException}. The {@link IneligibleGroupPolicy} then
 * decides: {@code REJECT_GRAPH} (the default) makes {@code submit} throw {@link IneligibleGraphException}
 * with nothing run; {@code ISOLATE_GROUP} fails just that group and runs the others.
 *
 * <p><b>One pass per run.</b> Validation is a dry run of Kahn's algorithm (counters only, no tasks).
 * Processing does not compute an order first and then walk it: it runs Kahn's algorithm again with each
 * task executed as it is released. Both passes are O(V + E) and the first is what lets
 * {@code REJECT_GRAPH} promise that nothing ran.
 *
 * <p><b>Parallel mode and the quote about parallel Kahn.</b> Many threads decrementing shared in-degree
 * counters only hurts when the decrement is the whole job. Here each decrement follows a task that is
 * orders of magnitude slower, so contention on the {@link AtomicIntegerArray} is negligible.
 *
 * <p>If a task throws, nothing new is started in its group (its dependants cannot run anyway); tasks
 * already running finish. The group fails with the first error; later ones are suppressed onto it.
 *
 * <p>The group result is the order in which its tasks completed.
 */
public abstract class TopologicalTaskExecutor<T> extends TaskExecutor<T, List<T>> {
    private final Executor taskPool;   // null: a group's tasks run one at a time on its group thread

    /** With {@link IneligibleGroupPolicy#REJECT_GRAPH}: any cycle makes the whole graph ineligible. */
    protected TopologicalTaskExecutor(Executor executor) {
        this(executor, IneligibleGroupPolicy.REJECT_GRAPH);
    }

    protected TopologicalTaskExecutor(Executor executor, IneligibleGroupPolicy policy) {
        super(executor, policy);
        this.taskPool = null;
    }

    /**
     * Runs independent tasks inside a group in parallel on {@code taskPool}.
     *
     * @param groupPool runs one coordinator per group; it blocks until its group is done
     * @param taskPool  runs the tasks. Must differ from {@code groupPool}: blocked coordinators could
     *                  otherwise occupy every thread and starve their own tasks (deadlock)
     */
    protected TopologicalTaskExecutor(Executor groupPool, Executor taskPool, IneligibleGroupPolicy policy) {
        super(groupPool, policy);
        if (taskPool == groupPool) {
            throw new IllegalArgumentException("taskPool must differ from groupPool, or blocked groups starve their tasks");
        }
        this.taskPool = Objects.requireNonNull(taskPool, "taskPool");
    }

    /** Runs one task. Every task it depends on has already completed. */
    protected abstract void executeTask(T task) throws Exception;

    /**
     * Rejects a group whose dependencies contain a cycle. Subclasses adding their own checks must call
     * {@code super.validateGroup(group)}.
     */
    @Override
    protected void validateGroup(TaskGroup<T> group) throws CycleDetectedException {
        try {
            kahn(group.graph(), false);   // dry run: counters only
        } catch (CycleDetectedException e) {
            throw e;
        } catch (Exception impossible) {   // a dry run executes nothing that could throw
            throw new IllegalStateException(impossible);
        }
    }

    @Override
    protected final List<T> processGroup(TaskGroup<T> group) throws Exception {
        Executor pool = taskPool;   // read once, so the null check and the use cannot disagree
        return pool == null ? kahn(group.graph(), true) : runParallel(group.graph(), pool);
    }

    /**
     * Kahn's algorithm; if {@code live}, each node's task runs as the node is released, so an order is
     * never computed only to be walked again. An instance method so every CycleDetectedException
     * records which executor (and so which T) made it.
     */
    private List<T> kahn(Graph<T> graph, boolean live) throws Exception {
        Map<T, Integer> pendingDependencies = new HashMap<>();
        Deque<T> ready = new ArrayDeque<>();
        for (T node : graph.nodes()) {
            int inDegree = graph.inDegree(node);
            pendingDependencies.put(node, inDegree);
            if (inDegree == 0) {
                ready.add(node);
            }
        }
        List<T> order = new ArrayList<>(graph.nodeCount());
        while (!ready.isEmpty()) {
            T node = ready.poll();
            if (live) {
                executeTask(node);   // a failure propagates: the rest of the group is skipped
            }
            order.add(node);
            for (T dependant : graph.successors(node)) {
                if (pendingDependencies.merge(dependant, -1, Integer::sum) == 0) {
                    ready.add(dependant);
                }
            }
        }
        if (order.size() < graph.nodeCount()) {
            // whatever never became ready is on a cycle or depends on one
            throw new CycleDetectedException(this, graph.nodes().stream()
                    .filter(node -> pendingDependencies.get(node) > 0).toList());
        }
        return order;
    }

    /**
     * Kahn's ready set driven by completions: a task is submitted to {@code pool} the moment its
     * pending-dependency counter reaches zero. Nodes are mapped to ints so the counters are one
     * {@link AtomicIntegerArray} and the adjacency is plain arrays (no boxing, no per-node locks).
     */
    private List<T> runParallel(Graph<T> graph, Executor pool) throws Exception {
        List<T> nodes = List.copyOf(graph.nodes());
        Map<T, Integer> id = new HashMap<>();
        for (int i = 0; i < nodes.size(); i++) {
            id.put(nodes.get(i), i);
        }
        int[][] successors = new int[nodes.size()][];
        AtomicIntegerArray pending = new AtomicIntegerArray(nodes.size());
        for (int i = 0; i < nodes.size(); i++) {
            T node = nodes.get(i);
            successors[i] = graph.successors(node).stream().mapToInt(id::get).toArray();
            pending.set(i, graph.inDegree(node));
        }

        ParallelRun run = new ParallelRun(pool, nodes, successors, pending);
        run.inFlight.incrementAndGet();   // guard: the run cannot look finished while roots are being seeded
        for (int i = 0; i < nodes.size(); i++) {
            if (pending.get(i) == 0) {
                run.submit(i);
            }
        }
        run.taskDone();   // drop the guard
        try {
            run.finished.get();
        } catch (InterruptedException e) {
            run.failure.compareAndSet(null, e);   // stop releasing new tasks
            throw e;
        } catch (ExecutionException e) {
            throw new IllegalStateException(e.getCause());   // finished is only ever completed normally
        }

        Throwable failure = run.failure.get();
        if (failure instanceof Exception e) {
            throw e;
        }
        if (failure instanceof Error e) {
            throw e;
        }
        List<T> order = List.copyOf(run.completed);
        if (order.size() < nodes.size()) {   // cannot happen after validateGroup, but never return a partial run as success
            List<T> blocked = new ArrayList<>();
            for (int i = 0; i < nodes.size(); i++) {
                if (pending.get(i) > 0) {
                    blocked.add(nodes.get(i));
                }
            }
            throw new CycleDetectedException(this, blocked);
        }
        return order;
    }

    /** State of one parallel group run, shared by the tasks of that group. */
    private final class ParallelRun {
        final Executor pool;
        final List<T> nodes;
        final int[][] successors;
        final AtomicIntegerArray pending;
        final Queue<T> completed = new ConcurrentLinkedQueue<>();
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final AtomicInteger inFlight = new AtomicInteger();
        final CompletableFuture<Void> finished = new CompletableFuture<>();

        ParallelRun(Executor pool, List<T> nodes, int[][] successors, AtomicIntegerArray pending) {
            this.pool = pool;
            this.nodes = nodes;
            this.successors = successors;
            this.pending = pending;
        }

        void submit(int node) {
            inFlight.incrementAndGet();
            try {
                pool.execute(() -> run(node));
            } catch (RuntimeException rejected) {   // e.g. the pool was shut down
                fail(rejected);
                taskDone();
            }
        }

        private void run(int node) {
            try {
                if (failure.get() != null) {
                    return;   // the group has already failed: start nothing new
                }
                executeTask(nodes.get(node));
                completed.add(nodes.get(node));
                for (int successor : successors[node]) {
                    if (pending.decrementAndGet(successor) == 0) {   // this task was its last dependency
                        submit(successor);
                    }
                }
            } catch (Throwable t) {
                if (t instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
                fail(t);
            } finally {
                taskDone();
            }
        }

        private void fail(Throwable t) {
            if (!failure.compareAndSet(null, t) && failure.get() != t) {
                failure.get().addSuppressed(t);
            }
        }

        void taskDone() {
            if (inFlight.decrementAndGet() == 0) {
                finished.complete(null);
            }
        }
    }

    /**
     * The nodes of a {@link CycleDetectedException} raised by this executor, typed as {@code T}.
     *
     * @throws IllegalArgumentException if {@code e} came from a different executor, whose nodes may not
     *                                  be {@code T}s, or was deserialized (no executor can vouch for it then;
     *                                  use {@link CycleDetectedException#blocked()})
     */
    @SuppressWarnings("unchecked")   // safe: checked below that this executor built the list from its own T nodes
    public List<T> blockedNodes(CycleDetectedException e) {
        if (e.source == null) {
            throw new IllegalArgumentException("exception was deserialized; use blocked() for its nodes");
        }
        if (e.source != this) {
            throw new IllegalArgumentException("exception was raised by a different executor");
        }
        return (List<T>) e.blocked;
    }

    /**
     * A group's dependencies form a cycle, so no valid order exists.
     *
     * <p>Not generic: Java forbids generic subclasses of {@code Throwable}, because {@code catch} cannot
     * test a type argument that erasure has removed. So {@link #blocked()} is {@code List<?>}; for a
     * {@code List<T>}, ask the executor that raised it: {@link #blockedNodes}.
     *
     * <p><b>Serialization</b> works like a {@code List}'s: {@link #blocked()} survives it if the nodes are
     * {@link java.io.Serializable}; otherwise serializing throws {@link java.io.NotSerializableException}
     * naming the node type. The raising executor is not serialized (it means nothing elsewhere), so
     * {@link #blockedNodes} refuses a deserialized exception.
     */
    public static final class CycleDetectedException extends Exception {
        @Serial
        private static final long serialVersionUID = 1L;

        private final transient TopologicalTaskExecutor<?> source;   // null once deserialized
        private final List<?> blocked;   // List.copyOf is serializable; the nodes must be too (see class doc)

        CycleDetectedException(TopologicalTaskExecutor<?> source, List<?> blocked) {
            super("dependency cycle; cannot order " + blocked);
            this.source = source;
            this.blocked = List.copyOf(blocked);
        }

        /** Nodes on a cycle or downstream of one, untyped; see {@link TopologicalTaskExecutor#blockedNodes}. */
        public List<?> blocked() {
            return blocked;
        }

        @Serial
        private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
            in.defaultReadObject();
            if (blocked == null) {   // only a corrupted or hand-crafted stream gets here
                throw new InvalidObjectException("blocked nodes missing");
            }
        }
    }
}
