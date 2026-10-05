package com.salesforce.einstein.graphexecutor.distributed;

import com.salesforce.einstein.graphexecutor.executor.IneligibleGroupPolicy;
import com.salesforce.einstein.graphexecutor.graph.Graph;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Kahn's algorithm the way it scales out: bulk-synchronous rounds over partitions that each own their
 * nodes' counters, so no in-degree counter is ever written by two workers. Simulated in one JVM: a
 * partition is a unit of work on the pool per round, and messages go through an in-memory
 * {@link Transport}. Everything a partition touches is its own, so mapping a partition to a machine
 * changes the transport, not the algorithm.
 *
 * <p>{@link #execute}:
 * <ol>
 *   <li><b>Snapshot</b> the graph on the calling thread.</li>
 *   <li><b>Find groups</b> (weakly connected components) with {@link LabelPropagation} over hash-owned
 *       partitions: no global BFS.</li>
 *   <li><b>Place</b> them ({@link GroupPlacement}): small groups whole onto one partition, the giant one
 *       split by node with the {@link Partitioner}.</li>
 *   <li><b>Dry run</b>: the rounds below with no tasks. Whatever never reaches in-degree 0 is on a
 *       cycle or downstream of one, exactly as in single-machine Kahn. Cheap: only counters move.
 *       {@code REJECT_GRAPH} throws {@link CyclicGraphException}; {@code ISOLATE_GROUP} drops the cyclic
 *       groups.</li>
 *   <li><b>Live run</b>, round by round until no partition has a ready node:
 *       <ol>
 *         <li><em>compute</em>, in parallel: each partition runs its ready tasks, records each outcome in
 *             the {@link CompletionLog}, then sends one combined {@link DecrementBatch} per receiving
 *             partition;</li>
 *         <li>barrier;</li>
 *         <li><em>apply</em>, in parallel: each partition applies its inbox, dropping duplicates by batch
 *             id, and releases nodes whose counter reaches 0;</li>
 *         <li>barrier.</li>
 *       </ol></li>
 * </ol>
 *
 * <p><b>Failures.</b> A task that throws fails only itself: its dependants never get its decrement and
 * are reported as skipped. A partition that crashes (anything thrown out of its round other than a
 * task failure: an unchecked exception, simulated through {@link #afterSend}) loses all its memory,
 * inbox included. Nothing was checkpointed: its replacement rebuilds the counters from the
 * {@link CompletionLog} and the graph, and replays the round, up to {@value #MAX_ATTEMPTS} attempts.
 * <ul>
 *   <li>tasks the log already records are not run again, but a crash between running a task and
 *       logging it does run it twice, so {@link #executeTask} must be <b>idempotent</b>;</li>
 *   <li>the replay re-sends its batches, so receivers <b>deduplicate</b> by batch id (see
 *       {@link DecrementBatch} for why that stands in for edge ids);</li>
 *   <li>the messages lost with the inbox need no resending: senders logged before sending, so the
 *       rebuild at the barrier already counts them.</li>
 * </ul>
 * Only the crashed partition recovers; the others keep their progress.
 *
 * @param <T> node (task) type
 */
public abstract class DistributedTopologicalExecutor<T> {
    static final int MAX_ATTEMPTS = 3;

    private final Executor pool;
    private final int partitions;
    private final Partitioner<T> partitioner;
    private final IneligibleGroupPolicy policy;

    protected DistributedTopologicalExecutor(Executor pool, int partitions, Partitioner<T> partitioner,
                                             IneligibleGroupPolicy policy) {
        if (partitions < 1) {
            throw new IllegalArgumentException("partitions must be >= 1");
        }
        this.pool = Objects.requireNonNull(pool, "pool");
        this.partitions = partitions;
        this.partitioner = Objects.requireNonNull(partitioner, "partitioner");
        this.policy = Objects.requireNonNull(policy, "policy");
    }

    /**
     * Runs one task; everything it depends on has completed. Must be idempotent: a partition replaying
     * a round after a crash runs that round's tasks again.
     *
     * <p>Declared {@code throws Exception} on purpose: a task is user code (I/O, blocking calls) and may
     * fail with any checked exception. Whatever it throws fails only that task: it is caught, recorded
     * in {@link RunReport#failed()}, and the task's dependants are skipped.
     */
    protected abstract void executeTask(T task) throws Exception;

    /**
     * The log for one live run. Override to plug in a durable store; the default survives partition
     * crashes (it lives outside every partition) but not the process.
     */
    protected CompletionLog<T> newCompletionLog() {
        return new InMemoryCompletionLog<>();
    }

    /** Copies of each batch the transport delivers; above 1 simulates an at-least-once network. */
    protected int deliveriesPerSend() {
        return 1;
    }

    /**
     * Fault-injection hook, called in the live run after each batch a partition sends. Throwing an
     * unchecked exception here is a crash of that partition mid-round: it has run some tasks and sent
     * some batches. (Unchecked because a crash is not part of any contract: it is whatever goes wrong.)
     */
    protected void afterSend(int partition, int round, int attempt) {
    }

    public final RunReport<T> execute(Graph<T> graph) {
        if (!graph.isDirected()) {
            throw new IllegalArgumentException("needs a directed (unidirectional) graph");
        }
        Graph<T> snapshot = graph.subgraph(graph.nodes());
        List<T> nodes = List.copyOf(snapshot.nodes());
        Map<T, Integer> index = new HashMap<>();
        Map<T, List<T>> neighbors = new HashMap<>();   // both directions, for grouping and placement
        for (T node : nodes) {
            index.put(node, index.size());
            List<T> either = new ArrayList<>(snapshot.successors(node));
            either.addAll(snapshot.predecessors(node));
            neighbors.put(node, either);
        }
        Comparator<T> graphOrder = Comparator.comparingInt(index::get);

        // groups, found by partitions that own nodes by hash: placement is not known yet
        Map<T, Integer> hashOwner = Partitioner.<T>hash().assign(nodes, neighbors::get, partitions);
        LabelPropagation.Result<T> components = LabelPropagation.run(nodes, neighbors, hashOwner, partitions, pool);
        Map<Integer, List<T>> byLabel = new LinkedHashMap<>();   // labels are first-node indexes: group order
        for (T node : nodes) {
            byLabel.computeIfAbsent(components.labels().get(node), label -> new ArrayList<>()).add(node);
        }
        List<List<T>> groups = List.copyOf(byLabel.values());
        Map<T, Integer> groupOf = new HashMap<>();
        for (int g = 0; g < groups.size(); g++) {
            for (T node : groups.get(g)) {
                groupOf.put(node, g);
            }
        }

        GroupPlacement.Placement<T> placement = GroupPlacement.place(groups, neighbors, partitions, partitioner);
        Map<T, Integer> ownerOf = placement.partitionOf();

        // dry run: counters only
        Run dry = new Run(snapshot, nodes, ownerOf, graphOrder, false);
        dry.execute();
        Map<Integer, List<T>> blockedByGroup = new TreeMap<>();
        for (T node : dry.stuck()) {
            blockedByGroup.computeIfAbsent(groupOf.get(node), g -> new ArrayList<>()).add(node);
        }
        if (!blockedByGroup.isEmpty() && policy == IneligibleGroupPolicy.REJECT_GRAPH) {
            throw new CyclicGraphException(blockedByGroup);
        }
        Set<Integer> rejected = blockedByGroup.keySet();
        List<T> eligible = nodes.stream().filter(node -> !rejected.contains(groupOf.get(node))).toList();

        Run live = new Run(snapshot, eligible, ownerOf, graphOrder, true);
        live.execute();

        long edgeCut = 0;
        for (T from : nodes) {
            for (T to : snapshot.successors(from)) {
                if (!ownerOf.get(from).equals(ownerOf.get(to))) {
                    edgeCut++;
                }
            }
        }
        Map<T, Throwable> failed = new TreeMap<>(graphOrder);
        live.partitions.forEach(p -> failed.putAll(p.failed()));
        Map<Integer, List<T>> rejectedGroups = new TreeMap<>();
        blockedByGroup.forEach((g, blocked) -> rejectedGroups.put(g, List.copyOf(blocked)));

        RunReport.Stats stats = new RunReport.Stats(partitions, groups.size(), placement.placedWhole(),
                placement.split(), edgeCut, components.rounds(), dry.rounds.size(), live.rounds.size(),
                live.transport.edges.get(), live.transport.batches.get(),
                live.partitions.stream().mapToLong(Partition::duplicatesDropped).sum(), live.recoveries.get());
        return new RunReport<>(List.copyOf(live.rounds), new LinkedHashMap<>(failed), live.stuck(),
                Collections.unmodifiableMap(rejectedGroups), stats);
    }

    /** One run of rounds (dry or live) over the given nodes, which must be whole groups. */
    private final class Run {
        final List<Partition<T>> partitions = new ArrayList<>();
        final Transport<T> transport;
        final List<List<T>> rounds = new ArrayList<>();
        final AtomicInteger recoveries = new AtomicInteger();
        private final Comparator<T> graphOrder;
        private final boolean live;

        Run(Graph<T> graph, List<T> nodes, Map<T, Integer> ownerOf, Comparator<T> graphOrder, boolean live) {
            this.graphOrder = graphOrder;
            this.live = live;
            int count = DistributedTopologicalExecutor.this.partitions;
            List<Map<T, List<T>>> successors = new ArrayList<>();
            List<Map<T, List<T>>> predecessors = new ArrayList<>();
            for (int p = 0; p < count; p++) {
                successors.add(new LinkedHashMap<>());
                predecessors.add(new HashMap<>());
            }
            for (T node : nodes) {
                // groups are whole, so every neighbor of an included node is included too
                int owner = ownerOf.get(node);
                successors.get(owner).put(node, List.copyOf(graph.successors(node)));
                predecessors.get(owner).put(node, List.copyOf(graph.predecessors(node)));
            }
            // the dry run logs nothing; its empty log just makes every partition start from the roots
            CompletionLog<T> log = live ? newCompletionLog() : new InMemoryCompletionLog<>();
            for (int p = 0; p < count; p++) {
                partitions.add(new Partition<>(p, successors.get(p), predecessors.get(p), ownerOf, graphOrder, log));
            }
            transport = new Transport<>(partitions, live ? deliveriesPerSend() : 1);
        }

        void execute() {
            int count = partitions.size();
            for (int round = 0; partitions.stream().anyMatch(Partition::hasReady); round++) {
                int r = round;
                List<List<T>> done = Supersteps.onEveryPartition(pool, count,
                        p -> computeWithRecovery(partitions.get(p), r));
                Supersteps.onEveryPartition(pool, count, p -> {
                    partitions.get(p).apply(r);
                    return null;
                });
                List<T> completed = new ArrayList<>();
                done.forEach(completed::addAll);
                completed.sort(graphOrder);
                rounds.add(List.copyOf(completed));
            }
        }

        private List<T> computeWithRecovery(Partition<T> partition, int round) {
            for (int attempt = 0; ; attempt++) {
                try {
                    return partition.compute(round, attempt, DistributedTopologicalExecutor.this, live, transport);
                } catch (RuntimeException crash) {   // task failures never get here: compute records them
                    if (!live || attempt + 1 >= MAX_ATTEMPTS) {   // the dry run has no log to recover from
                        throw new IllegalStateException("partition " + partition.id() + " crashed "
                                + MAX_ATTEMPTS + " times in round " + round, crash);
                    }
                    partition.crashAndRecover(round);   // its own state only; the others keep their progress
                    recoveries.incrementAndGet();
                }
            }
        }

        List<T> stuck() {
            List<T> stuck = new ArrayList<>();
            partitions.forEach(p -> stuck.addAll(p.stuck()));
            stuck.sort(graphOrder);
            return List.copyOf(stuck);
        }
    }
}
