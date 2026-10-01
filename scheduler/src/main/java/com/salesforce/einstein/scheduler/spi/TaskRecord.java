package com.salesforce.einstein.scheduler.spi;

import com.salesforce.einstein.scheduler.Recurrence;

import java.time.ZoneId;
import java.util.Comparator;

/**
 * Immutable snapshot of one task as held by a {@link TaskStore}.
 *
 * <p>{@code version} increases on every change except {@link #withRemovalRequested()}. The version
 * a node receives when it claims a task is its <b>fencing token</b>: {@link TaskStore#finish} only
 * applies if the version still matches, so a node whose lease was taken over can't overwrite the result.
 *
 * @param ownerNode        node running the task ({@code null} unless {@code RUNNING})
 * @param leaseUntilMillis when the claim expires; {@code Long.MAX_VALUE} for stores that never reap
 */
public record TaskRecord(long taskId, String jobType, String payload, String pinnedNode, String submitterNode,
                         int priority, long timeoutMillis, Recurrence recurrence, ZoneId zone,
                         int maxConsecutiveTimeouts, long nextRunMillis, TaskState state,
                         int consecutiveTimeouts, boolean removalRequested,
                         String ownerNode, long leaseUntilMillis, long version) {

    /** Earliest first → higher priority → FIFO (ids are assigned in submission order). */
    public static final Comparator<TaskRecord> SCHEDULE_ORDER =
        Comparator.comparingLong(TaskRecord::nextRunMillis)
                  .thenComparingInt(r -> -r.priority())
                  .thenComparingLong(TaskRecord::taskId);

    public static TaskRecord newTask(long taskId, TaskSpec s) {
        return new TaskRecord(taskId, s.jobType(), s.payload(), s.pinnedNode(), s.submitterNode(),
                s.priority(), s.timeoutMillis(), s.recurrence(), s.zone(), s.maxConsecutiveTimeouts(),
                s.firstRunMillis(), TaskState.SCHEDULED, 0, false, null, 0L, 0L);
    }

    public boolean isRecurring() { return recurrence != null; }

    /** SCHEDULED → RUNNING on {@code node}. */
    public TaskRecord claimed(String node, long leaseUntil) {
        return new TaskRecord(taskId, jobType, payload, pinnedNode, submitterNode, priority, timeoutMillis,
                recurrence, zone, maxConsecutiveTimeouts, nextRunMillis, TaskState.RUNNING,
                consecutiveTimeouts, removalRequested, node, leaseUntil, version + 1);
    }

    /** New run time for a SCHEDULED task. */
    public TaskRecord rescheduled(long newRunMillis) {
        return new TaskRecord(taskId, jobType, payload, pinnedNode, submitterNode, priority, timeoutMillis,
                recurrence, zone, maxConsecutiveTimeouts, newRunMillis, state,
                consecutiveTimeouts, removalRequested, ownerNode, leaseUntilMillis, version + 1);
    }

    /** Deferred removal of a RUNNING task. Keeps the version so the running node's fencing token stays valid. */
    public TaskRecord withRemovalRequested() {
        return new TaskRecord(taskId, jobType, payload, pinnedNode, submitterNode, priority, timeoutMillis,
                recurrence, zone, maxConsecutiveTimeouts, nextRunMillis, state,
                consecutiveTimeouts, true, ownerNode, leaseUntilMillis, version);
    }

    /** Leaves RUNNING (or is removed): releases the claim. */
    TaskRecord settled(TaskState newState, int timeouts, long nextRun) {
        return new TaskRecord(taskId, jobType, payload, pinnedNode, submitterNode, priority, timeoutMillis,
                recurrence, zone, maxConsecutiveTimeouts, nextRun, newState,
                timeouts, removalRequested, null, 0L, version + 1);
    }
}
