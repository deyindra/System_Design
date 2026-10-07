package com.salesforce.einstein.graphexecutor.distributed;

import com.salesforce.einstein.graphexecutor.spi.CompletionLog.Outcome;
import com.salesforce.einstein.graphexecutor.spi.CompletionLog.Status;
import com.salesforce.einstein.graphexecutor.spi.CompletionLog;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * One worker's share of a {@link DistributedTraversalExecutor} run. It owns some nodes and is the only
 * writer of their state: this round's frontier, failures and the ids of batches already applied. Other
 * partitions reach it only by sending {@link VisitBatch}es, which it applies itself between rounds.
 *
 * <p><b>All of that state is disposable</b>, exactly as in {@link Partition}: in memory only, never
 * checkpointed. After a crash, {@link #rebuild} derives it from the {@link CompletionLog} and the
 * partition's own edges. BFS round r visits the nodes at depth r, so:
 * <pre>frontier(r) = { v owned, admitted : v has no outcome before r, and
 *                  (r = 0 and v is a root) or some predecessor of v has an outcome at round r-1 }</pre>
 *
 * <p>Only {@link #deliver} is called concurrently (by senders); everything else runs on whichever pool
 * thread holds this partition for the current superstep, and the barrier orders those steps.
 */
final class TraversalPartition<T> implements Transport.Inbox<VisitBatch<T>> {
    private final int id;
    private final Map<T, List<T>> successors;     // owned node -> successors, in any partition
    private final Map<T, List<T>> predecessors;   // owned node -> predecessors, in any partition
    private final Map<T, Integer> ownerOf;        // read-only directory: which partition owns a node
    private final Set<T> admitted;                // read-only: nodes the executor admits
    private final Set<T> roots;                   // read-only: owned roots
    private final Map<T, Object> laneOf;          // read-only: owned node -> lane
    private final Comparator<T> graphOrder;
    private final CompletionLog<T> log;
    private final ConcurrentLinkedQueue<VisitBatch<T>> inbox = new ConcurrentLinkedQueue<>();

    // in memory only; lost in a crash and rebuilt from the log
    private List<T> frontier;                     // owned nodes to visit this round
    private Set<VisitBatch.Id> applied;           // dedupe of redelivered or replayed batches
    private Map<T, Throwable> failed;
    private Lanes lanes;                          // politeness: when each lane last started a task
    private boolean rebuildAtApply;               // crashed this round: its inbox is gone

    private long duplicatesDropped;

    TraversalPartition(int id, Map<T, List<T>> successors, Map<T, List<T>> predecessors, Map<T, Integer> ownerOf,
                       Set<T> admitted, Set<T> roots, Map<T, Object> laneOf, Comparator<T> graphOrder,
                       CompletionLog<T> log) {
        this.id = id;
        this.successors = successors;
        this.predecessors = predecessors;
        this.ownerOf = ownerOf;
        this.admitted = admitted;
        this.roots = roots;
        this.laneOf = laneOf;
        this.graphOrder = graphOrder;
        this.log = log;
        rebuild(0);   // with an empty log: frontier = the roots
    }

    int id() {
        return id;
    }

    boolean hasFrontier() {
        return !frontier.isEmpty();
    }

    /**
     * Visits this round's frontier and sends one combined batch per receiving partition. Each outcome is
     * logged before anything is sent. Task failures are recorded, not thrown, and do not stop the
     * traversal: the node's successors are still sent. An unchecked exception escaping this method is a
     * crash of the partition itself.
     *
     * <p>Nodes are taken round-robin across lanes, and a lane's next task waits until {@code delayNanos}
     * after that lane's previous start, so one slow lane does not hold back the others' first tasks.
     *
     * <p>On a replay, nodes that already have an outcome are not run again; the outcome is reused, so the
     * replay sends exactly the batches the crashed attempt did (same content, same ids).
     *
     * @return the nodes whose task completed this round
     */
    List<T> compute(int round, int attempt, DistributedTraversalExecutor<T> executor, int maxDepth,
                    long delayNanos, Transport<VisitBatch<T>> transport) {
        List<T> done = new ArrayList<>();
        Map<Integer, Set<T>> outgoing = new TreeMap<>();   // receiving partition -> reached nodes
        Map<Integer, Integer> edges = new HashMap<>();
        for (T node : Lanes.roundRobin(frontier, laneOf::get)) {
            if (visit(node, round, executor, maxDepth, delayNanos)) {
                done.add(node);
            }
            for (T successor : successors.get(node)) {   // every visited node expands, failed or not
                int to = ownerOf.get(successor);
                outgoing.computeIfAbsent(to, p -> new LinkedHashSet<>()).add(successor);   // the combiner
                edges.merge(to, 1, Integer::sum);
            }
        }
        frontier = new ArrayList<>();
        for (Map.Entry<Integer, Set<T>> batch : outgoing.entrySet()) {
            // local visits go through the inbox too, so the round is applied in one place
            transport.send(new VisitBatch<>(id, batch.getKey(), round, batch.getValue(), edges.get(batch.getKey())));
            executor.afterSend(id, round, attempt);
        }
        done.sort(graphOrder);
        return done;
    }

    /** True if the node's task completed: run now, or found DONE in the log by a replay. */
    private boolean visit(T node, int round, DistributedTraversalExecutor<T> executor, int maxDepth, long delayNanos) {
        Optional<Outcome> logged = log.outcome(node);
        if (logged.isPresent()) {
            if (logged.get().status() == Status.FAILED) {
                failed.putIfAbsent(node, recoveredFailure(node, logged.get()));
            }
            return logged.get().status() == Status.DONE;
        }
        if (round > maxDepth) {
            log.record(node, new Outcome(Status.SKIPPED, round, "deeper than maxDepth " + maxDepth));
            return false;
        }
        lanes.awaitTurn(laneOf.get(node), delayNanos);
        Exception error = Tasks.runAndLog(log, node, round, () -> executor.executeTask(node, round));
        if (error != null) {
            failed.put(node, error);
        }
        return error == null;
    }

    @Override
    public void deliver(VisitBatch<T> batch) {
        inbox.add(batch);
    }

    /**
     * Applies this round's inbox, dropping duplicate batches by id, and makes every reached, admitted,
     * not yet visited node the next frontier. A partition that crashed this round lost part of its inbox,
     * so it rebuilds instead: every sender logged before sending, so the log already says who was visited.
     */
    void apply(int round) {
        if (rebuildAtApply) {
            inbox.clear();
            rebuild(round + 1);
            rebuildAtApply = false;
            return;
        }
        // a replay re-sends only the current round, before the barrier, so older ids can never come back
        applied.removeIf(batchId -> batchId.round() < round);
        Set<T> next = new HashSet<>();
        for (VisitBatch<T> batch; (batch = inbox.poll()) != null; ) {
            if (!applied.add(batch.id())) {
                duplicatesDropped++;
                continue;
            }
            for (T node : batch.nodes()) {
                if (admitted.contains(node) && log.outcome(node).isEmpty()) {   // first reach = shortest depth
                    next.add(node);
                }
            }
        }
        frontier = new ArrayList<>(next);
        frontier.sort(graphOrder);
    }

    /**
     * Simulates losing the machine: every in-memory field and the inbox are gone. The replacement
     * rebuilds the state as of the start of {@code round} and replays it; at the barrier it rebuilds
     * again rather than applying its (partial) inbox.
     */
    void crashAndRecover(int round) {
        frontier = null;
        applied = null;
        failed = null;
        lanes = null;
        inbox.clear();
        rebuild(round);
        rebuildAtApply = true;
    }

    /**
     * Derives this partition's state as of the start of {@code round} from the log (see the class
     * comment). Outcomes from {@code round} itself or later are ignored, which makes the read consistent
     * while other partitions are still logging. O(edges into owned nodes) log reads.
     */
    private void rebuild(int round) {
        frontier = new ArrayList<>();
        applied = new HashSet<>();
        failed = new LinkedHashMap<>();
        lanes = new Lanes();
        for (T node : successors.keySet()) {
            Optional<Outcome> own = log.outcome(node).filter(outcome -> outcome.round() < round);
            if (own.isPresent()) {
                if (own.get().status() == Status.FAILED) {
                    failed.put(node, recoveredFailure(node, own.get()));
                }
                continue;
            }
            if (!admitted.contains(node)) {
                continue;
            }
            boolean reached = round == 0 ? roots.contains(node) : predecessors.get(node).stream()
                    .anyMatch(p -> log.outcome(p).filter(o -> o.round() == round - 1).isPresent());
            if (reached) {
                frontier.add(node);
            }
        }
        frontier.sort(graphOrder);
    }

    // the original exception died with the partition's memory; the log kept its description
    private IllegalStateException recoveredFailure(T node, Outcome outcome) {
        return new IllegalStateException("task " + node + " failed in round " + outcome.round()
                + " (recovered from the completion log): " + outcome.error());
    }

    Map<T, Throwable> failed() {
        return failed;
    }

    long duplicatesDropped() {
        return duplicatesDropped;
    }
}
