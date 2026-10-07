package com.salesforce.einstein.graphexecutor.executor;

import com.salesforce.einstein.ds.graph.BfsIterator;
import com.salesforce.einstein.ds.graph.Graph;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Function;

/**
 * Runs a unidirectional graph of tasks by splitting it into groups that cannot affect each other and
 * processing those groups concurrently.
 *
 * <p><b>What is a group?</b> A <em>weakly</em> connected component: nodes joined by edges followed in
 * either direction. Direction matters for ordering <em>inside</em> a group (a -> b may mean "a before
 * b"), but for independence any edge is a link: a and b in {@code a -> c <- b} both touch c, so they
 * belong together. Strongly connected components would be wrong here: they would put a and b apart.
 *
 * <p><b>Template method.</b> {@link #execute}/{@link #submit} are final and own the orchestration:
 * <ol>
 *   <li>{@link #findGroups} (overridable) splits the graph, BFS over the both-directions view, O(V + E);</li>
 *   <li>each group is snapshotted into its own subgraph on the calling thread, so the source graph
 *       (not thread-safe) is never read by a worker;</li>
 *   <li>{@link #validateGroup} (overridable) checks every group, still on the calling thread and before
 *       anything runs; the {@link IneligibleGroupPolicy} decides what a rejected group means;</li>
 *   <li>each eligible group is handed to the {@link Executor} and {@link #processGroup} (the subclass's job)
 *       runs it. One thread per group at a time; how nodes inside a group run is up to the subclass.</li>
 * </ol>
 *
 * <p><b>Failure isolation.</b> An exception from one group becomes that group's {@link GroupResult};
 * the other groups still run to completion.
 *
 * <p>The graph must be directed; weighted or unweighted are both fine (weights reach each group's copy).
 *
 * <p>The executor does not own the thread pool: callers create it, size it and shut it down.
 *
 * @param <T> node (task) type
 * @param <R> what processing one group produces
 */
public abstract class TaskExecutor<T, R> {
    private final Executor executor;
    private final IneligibleGroupPolicy policy;

    /** With {@link IneligibleGroupPolicy#REJECT_GRAPH}: one ineligible group makes the whole graph ineligible. */
    protected TaskExecutor(Executor executor) {
        this(executor, IneligibleGroupPolicy.REJECT_GRAPH);
    }

    protected TaskExecutor(Executor executor, IneligibleGroupPolicy policy) {
        this.executor = Objects.requireNonNull(executor, "executor");
        this.policy = Objects.requireNonNull(policy, "policy");
    }

    /** Processes one group. Runs on an executor thread; may block. Thrown exceptions fail only this group. */
    protected abstract R processGroup(TaskGroup<T> group) throws Exception;

    /**
     * Decides, before anything runs, whether a group may be processed: throw to reject it, with the
     * exception explaining why. Runs on the calling thread, so keep it cheap (graph checks, not work).
     * Default: every group is eligible.
     */
    protected void validateGroup(TaskGroup<T> group) throws Exception {
    }

    public final IneligibleGroupPolicy policy() {
        return policy;
    }

    /** Processes every group and waits; results are in {@link TaskGroup#id()} order. */
    public final List<GroupResult<T, R>> execute(Graph<T> graph) {
        return submit(graph).join();
    }

    /**
     * Starts processing every group and returns at once. The graph is fully read before this returns,
     * so the caller may modify it immediately afterwards.
     *
     * @throws IllegalArgumentException if the graph is undirected
     * @throws IneligibleGraphException  if a group fails {@link #validateGroup} under
     *                                   {@link IneligibleGroupPolicy#REJECT_GRAPH}; nothing has run
     */
    public final CompletableFuture<List<GroupResult<T, R>>> submit(Graph<T> graph) {
        if (!graph.isDirected()) {
            throw new IllegalArgumentException("TaskExecutor needs a directed (unidirectional) graph");
        }
        List<TaskGroup<T>> groups = findGroups(graph);

        // validate everything before dispatching anything, so REJECT_GRAPH really means "nothing ran"
        List<Exception> reasons = new ArrayList<>(groups.size());   // per group; null = eligible
        List<GroupResult<T, R>> rejected = new ArrayList<>();
        for (TaskGroup<T> group : groups) {
            Exception reason = validate(group);
            reasons.add(reason);
            if (reason != null) {
                rejected.add(GroupResult.failure(group, reason));
            }
        }
        if (!rejected.isEmpty() && policy == IneligibleGroupPolicy.REJECT_GRAPH) {
            throw new IneligibleGraphException(rejected);
        }

        List<CompletableFuture<GroupResult<T, R>>> futures = new ArrayList<>(groups.size());
        for (int i = 0; i < groups.size(); i++) {
            TaskGroup<T> group = groups.get(i);
            Exception reason = reasons.get(i);
            futures.add(reason == null
                    ? CompletableFuture.supplyAsync(() -> run(group), executor)
                    : CompletableFuture.completedFuture(GroupResult.failure(group, reason)));   // no thread used
        }
        return CompletableFuture.allOf(futures.toArray(CompletableFuture[]::new))
                .thenApply(done -> futures.stream().map(CompletableFuture::join).toList());
    }

    /**
     * Splits the graph into independent groups, each with a private copy of its subgraph. The default
     * is weakly connected components; override for another notion of independence.
     */
    protected List<TaskGroup<T>> findGroups(Graph<T> graph) {
        Function<T, Iterable<T>> eitherDirection = node -> concat(graph.successors(node), graph.predecessors(node));
        Set<T> assigned = new HashSet<>();
        List<TaskGroup<T>> groups = new ArrayList<>();
        for (T node : graph.nodes()) {
            if (assigned.contains(node)) {
                continue;
            }
            // one BFS per component: it stops at the component's edge, so the total is still O(V + E)
            List<T> members = new ArrayList<>();
            new BfsIterator<>(List.of(node), eitherDirection).forEachRemaining(members::add);
            assigned.addAll(members);
            groups.add(new TaskGroup<>(groups.size(), graph.subgraph(members)));
        }
        return groups;
    }

    /** The rejection reason, or null if the group is eligible. */
    private Exception validate(TaskGroup<T> group) {
        try {
            validateGroup(group);
            return null;
        } catch (Exception e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            return e;
        }
    }

    private GroupResult<T, R> run(TaskGroup<T> group) {
        try {
            return GroupResult.success(group, processGroup(group));
        } catch (Exception e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            return GroupResult.failure(group, e);
        }
    }

    private static <T> Iterable<T> concat(Iterable<T> first, Iterable<T> second) {
        return () -> new Iterator<>() {
            private final Iterator<T> a = first.iterator();
            private final Iterator<T> b = second.iterator();

            @Override
            public boolean hasNext() {
                return a.hasNext() || b.hasNext();
            }

            @Override
            public T next() {
                return a.hasNext() ? a.next() : b.next();
            }
        };
    }
}
