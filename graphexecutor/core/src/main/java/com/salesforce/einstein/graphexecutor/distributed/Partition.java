package com.salesforce.einstein.graphexecutor.distributed;

import com.salesforce.einstein.graphexecutor.spi.CompletionLog.Outcome;
import com.salesforce.einstein.graphexecutor.spi.CompletionLog.Status;
import com.salesforce.einstein.graphexecutor.spi.CompletionLog;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * One worker's share of the graph. It owns some nodes and is the only writer of their state: pending
 * dependency counters, the ready list, failures and the ids of batches already applied. Other partitions
 * reach that state only by sending it {@link DecrementBatch}es, which it applies itself between rounds.
 *
 * <p><b>All of that state is disposable.</b> It lives in memory only and is never checkpointed. After a
 * crash, {@link #rebuild} derives it from the {@link CompletionLog} and the partition's own edges:
 * <pre>pending(v) = |{ u in predecessors(v) : u has no DONE outcome }|</pre>
 *
 * <p>Only {@link #deliver} is called concurrently (by senders); everything else runs on whichever pool
 * thread holds this partition for the current superstep, and the barrier orders those steps.
 */
final class Partition<T> implements Transport.Inbox<DecrementBatch<T>> {
    private final int id;
    private final Map<T, List<T>> successors;     // owned node -> successors, in any partition
    private final Map<T, List<T>> predecessors;   // owned node -> predecessors, in any partition
    private final Map<T, Integer> ownerOf;        // read-only directory: which partition owns a node
    private final Comparator<T> graphOrder;
    private final CompletionLog<T> log;
    private final ConcurrentLinkedQueue<DecrementBatch<T>> inbox = new ConcurrentLinkedQueue<>();

    // in memory only; lost in a crash and rebuilt from the log
    private Map<T, Integer> pending;              // owned nodes still waiting on dependencies (count > 0)
    private List<T> ready;                        // released, to run next round
    private Set<DecrementBatch.Id> applied;       // dedupe of redelivered or replayed batches
    private Map<T, Throwable> failed;
    private boolean rebuildAtApply;               // crashed this round: its inbox is gone

    private long duplicatesDropped;

    Partition(int id, Map<T, List<T>> successors, Map<T, List<T>> predecessors, Map<T, Integer> ownerOf,
              Comparator<T> graphOrder, CompletionLog<T> log) {
        this.id = id;
        this.successors = successors;
        this.predecessors = predecessors;
        this.ownerOf = ownerOf;
        this.graphOrder = graphOrder;
        this.log = log;
        rebuild(0);   // with an empty log: pending = in-degree, ready = the roots
    }

    int id() {
        return id;
    }

    boolean hasReady() {
        return !ready.isEmpty();
    }

    /**
     * Runs (or, in a dry run, just releases) this round's ready nodes and sends one combined batch per
     * receiving partition. Each outcome is logged before anything is sent for it. Task failures are
     * recorded, not thrown: the failed node's dependants never get its decrement. An unchecked
     * exception escaping this method is a crash of the partition itself.
     *
     * <p>On a replay, tasks that already have an outcome are not run again; their outcome is reused, so
     * the replay sends exactly the batches the crashed attempt did (same content, same ids).
     */
    List<T> compute(int round, int attempt, DistributedTopologicalExecutor<T> executor, boolean live,
                    Transport<DecrementBatch<T>> transport) {
        List<T> done = new ArrayList<>();
        Map<Integer, Map<T, Integer>> outgoing = new TreeMap<>();   // receiving partition -> combined decrements
        for (T node : ready) {
            if (live && !runOrReuse(node, round, executor)) {
                continue;
            }
            done.add(node);
            for (T successor : successors.get(node)) {
                outgoing.computeIfAbsent(ownerOf.get(successor), p -> new LinkedHashMap<>())
                        .merge(successor, 1, Integer::sum);   // the combiner: (v, -k), not k messages
            }
        }
        ready = new ArrayList<>();
        for (Map.Entry<Integer, Map<T, Integer>> batch : outgoing.entrySet()) {
            // local decrements go through the inbox too, so the round is applied in one place
            transport.send(new DecrementBatch<>(id, batch.getKey(), round, batch.getValue()));
            if (live) {
                executor.afterSend(id, round, attempt);
            }
        }
        return done;
    }

    /** True if the node is done: run now, or found DONE in the log by a replay. */
    private boolean runOrReuse(T node, int round, DistributedTopologicalExecutor<T> executor) {
        Optional<Outcome> logged = log.outcome(node);
        if (logged.isPresent()) {
            if (logged.get().status() == Status.FAILED) {
                failed.putIfAbsent(node, recoveredFailure(node, logged.get()));
                return false;
            }
            return true;
        }
        Exception error = Tasks.runAndLog(log, node, round, () -> executor.executeTask(node));
        if (error != null) {
            failed.put(node, error);
        }
        return error == null;
    }

    @Override
    public void deliver(DecrementBatch<T> batch) {
        inbox.add(batch);
    }

    /**
     * Applies this round's inbox, dropping duplicates by batch id, and releases nodes that reach zero.
     * A partition that crashed this round lost part of its inbox, so it rebuilds instead: every sender
     * logged before sending, so the log already holds everything the lost messages said.
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
        List<T> released = new ArrayList<>();
        for (DecrementBatch<T> batch; (batch = inbox.poll()) != null; ) {
            if (!applied.add(batch.id())) {
                duplicatesDropped++;
                continue;
            }
            batch.decrements().forEach((node, count) -> {
                int k = count;   // unboxed: Integer == compares references, wrong above the cache (127)
                int left = pending.getOrDefault(node, 0);   // absent: already released
                if (left < k) {   // what a missing dedupe would cause: release too early
                    throw new IllegalStateException("over-decremented " + node + " in partition " + id);
                }
                if (left == k) {
                    pending.remove(node);
                    released.add(node);
                } else {
                    pending.put(node, left - k);
                }
            });
        }
        released.sort(graphOrder);
        ready = released;
    }

    /**
     * Simulates losing the machine: every in-memory field and the inbox are gone. The replacement
     * rebuilds the state as of the start of {@code round} and replays it; at the barrier it rebuilds
     * again rather than applying its (partial) inbox.
     */
    void crashAndRecover(int round) {
        pending = null;
        ready = null;
        applied = null;
        failed = null;
        inbox.clear();
        rebuild(round);
        rebuildAtApply = true;
    }

    /**
     * Derives this partition's state as of the start of {@code round} from the log: a node with an
     * outcome from an earlier round is finished; any other node is ready if every predecessor is DONE
     * before {@code round}, else pending on the ones that are not. Outcomes from {@code round} itself or
     * later are ignored, which makes the read consistent while other partitions are still logging.
     * O(edges into owned nodes) log reads.
     */
    private void rebuild(int round) {
        pending = new HashMap<>();
        ready = new ArrayList<>();
        applied = new HashSet<>();
        failed = new LinkedHashMap<>();
        for (T node : successors.keySet()) {
            Optional<Outcome> own = before(node, round);
            if (own.isPresent()) {
                if (own.get().status() == Status.FAILED) {
                    failed.put(node, recoveredFailure(node, own.get()));
                }
                continue;
            }
            int waiting = 0;
            for (T predecessor : predecessors.get(node)) {
                if (before(predecessor, round).filter(o -> o.status() == Status.DONE).isEmpty()) {
                    waiting++;
                }
            }
            if (waiting == 0) {
                ready.add(node);
            } else {
                pending.put(node, waiting);
            }
        }
        ready.sort(graphOrder);
    }

    private Optional<Outcome> before(T node, int round) {
        return log.outcome(node).filter(outcome -> outcome.round() < round);
    }

    // the original exception died with the partition's memory; the log kept its description
    private IllegalStateException recoveredFailure(T node, Outcome outcome) {
        return new IllegalStateException("task " + node + " failed in round " + outcome.round()
                + " (recovered from the completion log): " + outcome.error());
    }

    /** Owned nodes that never became ready: on a cycle, or downstream of a failure or a cycle. */
    List<T> stuck() {
        return pending.keySet().stream().sorted(graphOrder).toList();
    }

    Map<T, Throwable> failed() {
        return failed;
    }

    long duplicatesDropped() {
        return duplicatesDropped;
    }
}
