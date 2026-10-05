package com.salesforce.einstein.graphexecutor.distributed;

import java.util.Optional;

/**
 * The durable record of which tasks finished, and in which round. It is the only state a run must
 * keep to survive a crash: a node's pending count is just its predecessors with no {@code DONE}
 * outcome, so a partition that lost its memory rebuilds its counters from this log and the graph,
 * instead of restoring a checkpoint.
 *
 * <p>Rules the executor follows:
 * <ul>
 *   <li><b>Log before send:</b> a task's outcome is recorded before any decrement for it is sent, so
 *       the log never knows less than the messages say;</li>
 *   <li><b>Rounds make reads consistent:</b> while round r runs, partitions record round-r outcomes
 *       concurrently, so a rebuild "as of round r" reads only outcomes from earlier rounds;</li>
 *   <li><b>Outcomes are final:</b> recording a task twice keeps the first outcome. A replay finds the
 *       outcome and does not run the task again.</li>
 * </ul>
 *
 * <p>Implementations must be thread-safe, and durable before {@link #record} returns. A real one is a
 * table keyed by task id (a KV store, or a compacted log topic).
 */
public interface CompletionLog<T> {

    enum Status { DONE, FAILED }

    /**
     * @param error for {@code FAILED}, a description of the failure (an exception object does not
     *              survive a crash); null for {@code DONE}
     */
    record Outcome(Status status, int round, String error) {
    }

    /** Records a task's outcome. Idempotent: if the task already has one, it is kept. */
    void record(T task, Outcome outcome);

    Optional<Outcome> outcome(T task);
}
