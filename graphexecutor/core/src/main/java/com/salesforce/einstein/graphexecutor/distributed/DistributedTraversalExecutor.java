package com.salesforce.einstein.graphexecutor.distributed;

import com.salesforce.einstein.ds.graph.BfsIterator;
import com.salesforce.einstein.ds.graph.Graph;
import com.salesforce.einstein.graphexecutor.spi.CompletionLog.Status;
import com.salesforce.einstein.graphexecutor.spi.CompletionLog;
import com.salesforce.einstein.graphexecutor.spi.memory.InMemoryCompletionLog;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A distributed breadth-first traversal: every node reachable from the roots runs exactly once, at its
 * shortest distance from a root, with an edge {@code a -> b} meaning "b is reachable from a", not "b
 * depends on a". The scale-out form of {@link com.salesforce.einstein.graphexecutor.executor.TraversalTaskExecutor},
 * built like {@link DistributedTopologicalExecutor}: bulk-synchronous rounds over partitions that each own
 * their nodes' state. Simulated in one JVM; mapping a partition to a machine changes the transport, not
 * the algorithm.
 *
 * <p><b>Round r visits the nodes at depth r.</b> That is BFS, and a node's first visit is its shortest
 * depth, so (unlike Kahn's waves, which follow the longest dependency chain) cycles are harmless and no
 * node waits for another.
 *
 * <p>{@link #execute}:
 * <ol>
 *   <li><b>Snapshot</b> the graph on the calling thread, and ask every hook (except
 *       {@link #executeTask}) its answer for every node once, so hooks need not be thread-safe.</li>
 *   <li><b>Find groups</b> with {@link LabelPropagation}: a group with no admitted root starts at its
 *       first admitted node.</li>
 *   <li><b>Place</b> nodes: by default with {@link GroupPlacement} (small groups whole, the giant one split
 *       by the {@link Partitioner}); with {@link #placeGroupsWhole()} false, by the partitioner alone,
 *       which is what a key partitioner needs to keep each lane (host) on one partition.</li>
 *   <li><b>Rounds</b> until no partition has a frontier:
 *       <ol>
 *         <li><em>compute</em>, in parallel: each partition runs its frontier (round-robin across lanes,
 *             {@link #laneDelay} apart within one), records each outcome in the {@link CompletionLog},
 *             then sends one combined {@link VisitBatch} per receiving partition;</li>
 *         <li>barrier;</li>
 *         <li><em>apply</em>, in parallel: each partition drops duplicate batches and makes every
 *             reached, admitted, never-visited node its next frontier;</li>
 *         <li>barrier.</li>
 *       </ol>
 *       Past {@link #maxDepth} the rounds go on without running anything (nodes are logged
 *       {@code SKIPPED}), so the report can say exactly which nodes were too deep.</li>
 * </ol>
 *
 * <p><b>Failures</b> are handled as in {@link DistributedTopologicalExecutor}. A task that throws fails only
 * itself, and blocks nothing: its successors are still visited. A partition that crashes (an unchecked
 * exception, simulated through {@link #afterSend}) loses its memory, inbox included; with no checkpoint,
 * its replacement rebuilds its frontier from the log and replays the round, up to {@value #MAX_ATTEMPTS}
 * attempts. Tasks already logged are not run again, but a crash between running and logging runs a task
 * twice, so {@link #executeTask} must be <b>idempotent</b>. Duplicate batches (replays, at-least-once
 * delivery) are dropped by id; a visit is idempotent anyway, so that only saves work.
 *
 * @param <T> node type
 */
public abstract class DistributedTraversalExecutor<T> {
    static final int MAX_ATTEMPTS = 3;

    private final Executor pool;
    private final int partitions;
    private final Partitioner<T> partitioner;

    protected DistributedTraversalExecutor(Executor pool, int partitions, Partitioner<T> partitioner) {
        if (partitions < 1) {
            throw new IllegalArgumentException("partitions must be >= 1");
        }
        this.pool = Objects.requireNonNull(pool, "pool");
        this.partitions = partitions;
        this.partitioner = Objects.requireNonNull(partitioner, "partitioner");
    }

    /**
     * Visits one node. Must be idempotent: a partition replaying a round after a crash may run it again.
     *
     * <p>Declared {@code throws Exception} on purpose: a task is user code (I/O, blocking calls) and may
     * fail with any checked exception. Whatever it throws fails only that node; it is recorded in
     * {@link TraversalReport#failed()} and the traversal goes on.
     *
     * @param depth the node's shortest distance from a root (= the round)
     */
    protected abstract void executeTask(T node, int depth) throws Exception;

    /**
     * Where traversal starts. Default: nodes with no predecessors. Nodes not in the graph are ignored. For
     * a group with no admitted root, see {@link #startRootlessGroups()}.
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
     * Nodes with equal lanes that share a partition run one at a time, {@link #laneDelay} apart. To make
     * that hold across the whole run, keep a lane on one partition: {@link Partitioner#byKey} on the
     * lane and {@link #placeGroupsWhole()} false. Default: the node itself, so nothing is serialized.
     */
    protected Object laneOf(T node) {
        return node;
    }

    /** The minimum gap between the starts of two tasks of one lane. Default: 0. */
    protected Duration laneDelay() {
        return Duration.ZERO;
    }

    /**
     * True (the default): groups small enough go whole onto one partition, and only bigger ones are split
     * by the partitioner. False: the partitioner places every node, so its rule (such as "same key, same
     * partition") holds for the whole graph.
     */
    protected boolean placeGroupsWhole() {
        return true;
    }

    /**
     * The log for one run. Override to plug in a durable store; the default survives partition crashes
     * (it lives outside every partition) but not the process.
     */
    protected CompletionLog<T> newCompletionLog() {
        return new InMemoryCompletionLog<>();
    }

    /** Copies of each batch the transport delivers; above 1 simulates an at-least-once network. */
    protected int deliveriesPerSend() {
        return 1;
    }

    /**
     * Fault-injection hook, called after each batch a partition sends. Throwing an unchecked exception
     * here is a crash of that partition mid-round: it has run some tasks and sent some batches.
     */
    protected void afterSend(int partition, int round, int attempt) {
    }

    public final TraversalReport<T> execute(Graph<T> graph) {
        GraphSnapshot<T> view = GraphSnapshot.of(graph);
        List<T> nodes = view.nodes();
        Comparator<T> graphOrder = view.graphOrder();

        // every hook asked once, here, so partitions only read immutable answers
        Set<T> admitted = new HashSet<>();
        List<T> excluded = new ArrayList<>();
        Map<T, Object> laneOf = new HashMap<>();
        for (T node : nodes) {
            if (admit(node)) {
                admitted.add(node);
            } else {
                excluded.add(node);
            }
            laneOf.put(node, Objects.requireNonNull(laneOf(node), "laneOf"));
        }
        int maxDepth = maxDepth();
        Duration delay = Objects.requireNonNull(laneDelay(), "laneDelay");
        if (delay.isNegative()) {
            throw new IllegalArgumentException("laneDelay must be >= 0");
        }
        Set<T> declaredRoots = roots(view.graph());
        boolean startRootless = startRootlessGroups();

        GraphSnapshot.Groups<T> found = view.groups(pool, partitions);
        List<List<T>> groups = found.groups();

        Set<T> roots = new HashSet<>();
        for (List<T> group : groups) {
            List<T> groupRoots = group.stream().filter(n -> declaredRoots.contains(n) && admitted.contains(n)).toList();
            roots.addAll(groupRoots.isEmpty() && startRootless
                    ? group.stream().filter(admitted::contains).limit(1).toList()
                    : groupRoots);
        }

        Map<T, Integer> ownerOf = placeGroupsWhole()
                ? GroupPlacement.place(groups, view.neighbors(), partitions, partitioner).partitionOf()
                : placeByPartitioner(groups, view.neighbors());

        Run run = new Run(view.graph(), nodes, ownerOf, admitted, roots, laneOf, graphOrder);
        run.execute(maxDepth, delay.toNanos());
        return report(run, view, groups, ownerOf, excluded, admitted, found.labelRounds());
    }

    /** Every node placed by the partitioner, fed group by group in BFS order (what LDG wants). */
    private Map<T, Integer> placeByPartitioner(List<List<T>> groups, Map<T, List<T>> neighbors) {
        List<T> order = new ArrayList<>();
        for (List<T> group : groups) {
            new BfsIterator<>(List.of(group.get(0)), neighbors::get).forEachRemaining(order::add);
        }
        Map<T, Integer> assignment = partitioner.assign(order, neighbors::get, partitions);
        Map<T, Integer> ownerOf = new HashMap<>();
        for (T node : order) {
            Integer p = assignment.get(node);
            if (p == null || p < 0 || p >= partitions) {
                throw new IllegalStateException("partitioner put " + node + " in partition " + p);
            }
            ownerOf.put(node, p);
        }
        return ownerOf;
    }

    private TraversalReport<T> report(Run run, GraphSnapshot<T> view, List<List<T>> groups, Map<T, Integer> ownerOf,
                                      List<T> excluded, Set<T> admitted, int labelRounds) {
        Map<T, Integer> depth = new LinkedHashMap<>();
        List<T> tooDeep = new ArrayList<>();
        List<T> unreachable = new ArrayList<>();
        for (T node : view.nodes()) {
            run.log.outcome(node).ifPresentOrElse(outcome -> {
                depth.put(node, outcome.round());
                if (outcome.status() == Status.SKIPPED) {
                    tooDeep.add(node);
                }
            }, () -> {
                if (admitted.contains(node)) {
                    unreachable.add(node);
                }
            });
        }
        Map<T, Throwable> failed = new TreeMap<>(view.graphOrder());
        run.partitions.forEach(p -> failed.putAll(p.failed()));

        long edgeCut = view.edgeCut(ownerOf);
        int placedWhole = 0;
        for (List<T> group : groups) {
            int first = ownerOf.get(group.get(0));
            if (group.stream().allMatch(n -> ownerOf.get(n) == first)) {
                placedWhole++;
            }
        }
        List<List<T>> rounds = new ArrayList<>(run.rounds);
        while (!rounds.isEmpty() && rounds.get(rounds.size() - 1).isEmpty()) {
            rounds.remove(rounds.size() - 1);   // the mark-only rounds past maxDepth complete nothing
        }

        TraversalReport.Stats stats = new TraversalReport.Stats(partitions, groups.size(), placedWhole,
                groups.size() - placedWhole, edgeCut, labelRounds, run.rounds.size(), run.transport.edges.get(),
                run.transport.batches.get(),
                run.partitions.stream().mapToLong(TraversalPartition::duplicatesDropped).sum(), run.recoveries.get());
        return new TraversalReport<>(List.copyOf(rounds), Collections.unmodifiableMap(depth),
                Collections.unmodifiableMap(new LinkedHashMap<>(failed)), List.copyOf(excluded),
                List.copyOf(tooDeep), List.copyOf(unreachable), stats);
    }

    /** One traversal over every node. */
    private final class Run {
        final List<TraversalPartition<T>> partitions = new ArrayList<>();
        final CompletionLog<T> log = newCompletionLog();
        final Transport<VisitBatch<T>> transport;
        final List<List<T>> rounds = new ArrayList<>();
        final AtomicInteger recoveries = new AtomicInteger();
        private final Comparator<T> graphOrder;

        Run(Graph<T> graph, List<T> nodes, Map<T, Integer> ownerOf, Set<T> admitted, Set<T> roots,
            Map<T, Object> laneOf, Comparator<T> graphOrder) {
            this.graphOrder = graphOrder;
            int count = DistributedTraversalExecutor.this.partitions;
            List<Map<T, List<T>>> successors = new ArrayList<>();
            List<Map<T, List<T>>> predecessors = new ArrayList<>();
            List<Set<T>> ownedRoots = new ArrayList<>();
            List<Map<T, Object>> ownedLanes = new ArrayList<>();
            for (int p = 0; p < count; p++) {
                successors.add(new LinkedHashMap<>());
                predecessors.add(new HashMap<>());
                ownedRoots.add(new HashSet<>());
                ownedLanes.add(new HashMap<>());
            }
            for (T node : nodes) {
                int owner = ownerOf.get(node);
                successors.get(owner).put(node, List.copyOf(graph.successors(node)));
                predecessors.get(owner).put(node, List.copyOf(graph.predecessors(node)));
                ownedLanes.get(owner).put(node, laneOf.get(node));
                if (roots.contains(node)) {
                    ownedRoots.get(owner).add(node);
                }
            }
            for (int p = 0; p < count; p++) {
                partitions.add(new TraversalPartition<>(p, successors.get(p), predecessors.get(p), ownerOf,
                        admitted, ownedRoots.get(p), ownedLanes.get(p), graphOrder, log));
            }
            transport = new Transport<>(partitions, deliveriesPerSend());
        }

        void execute(int maxDepth, long delayNanos) {
            int count = partitions.size();
            for (int round = 0; partitions.stream().anyMatch(TraversalPartition::hasFrontier); round++) {
                int r = round;
                rounds.add(Supersteps.round(pool, count, p -> computeWithRecovery(partitions.get(p), r, maxDepth, delayNanos),
                        p -> partitions.get(p).apply(r), graphOrder));
            }
        }

        private List<T> computeWithRecovery(TraversalPartition<T> partition, int round, int maxDepth, long delayNanos) {
            return Supersteps.withRecovery(partition.id(), round, MAX_ATTEMPTS, true,
                    attempt -> partition.compute(round, attempt, DistributedTraversalExecutor.this, maxDepth,
                            delayNanos, transport),
                    () -> {
                        partition.crashAndRecover(round);
                        recoveries.incrementAndGet();
                    });
        }
    }
}
