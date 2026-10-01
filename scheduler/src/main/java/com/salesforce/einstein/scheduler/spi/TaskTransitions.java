package com.salesforce.einstein.scheduler.spi;

import java.util.Objects;

/**
 * The scheduler's state-machine rules as pure functions, shared by every {@link TaskStore} so all
 * backends behave identically. A store only has to apply the returned record atomically.
 */
public final class TaskTransitions {
    private TaskTransitions() {}

    /** @param outcome final result for the submitter, or {@code null} if the task lives on */
    public record Transition(TaskRecord after, Outcome outcome) {}

    /**
     * A run ended (or its lease expired).
     *
     * <ul>
     *   <li><b>One-time:</b> COMPLETED with SUCCEEDED / FAILED / TIMED_OUT.</li>
     *   <li><b>Recurring:</b> a timeout increments {@code consecutiveTimeouts}, anything else resets it.
     *       Then: removal requested or dropped → CANCELLED; timeouts reached the limit → BLACKLISTED;
     *       otherwise → SCHEDULED at {@code recurrence.nextAfter(now)}.</li>
     * </ul>
     *
     * A recurring task is dropped when its node stops and no other node can take it over: the store
     * isn't shared, or the task is pinned to that node. A pinned task whose lease expired is dropped
     * too (its in-process job died with the node).
     */
    public static Transition finish(TaskRecord r, RunOutcome result, String detail, long nowMillis,
                                    boolean nodeStopping, boolean sharedStore) {
        Objects.requireNonNull(result, "result");
        boolean timedOut = result.isTimeout();
        if (!r.isRecurring()) {
            Outcome.Kind kind = timedOut ? Outcome.Kind.TIMED_OUT
                              : result == RunOutcome.FAILED ? Outcome.Kind.FAILED
                              : Outcome.Kind.SUCCEEDED;
            return new Transition(r.settled(TaskState.COMPLETED, r.consecutiveTimeouts(), r.nextRunMillis()),
                    outcome(r, kind, detail));
        }

        int timeouts = timedOut ? r.consecutiveTimeouts() + 1 : 0;
        boolean pinned = r.pinnedNode() != null;
        boolean drop = r.removalRequested()
                || (nodeStopping && (!sharedStore || pinned))
                || (result == RunOutcome.LEASE_EXPIRED && pinned);
        if (drop) {
            return new Transition(r.settled(TaskState.CANCELLED, timeouts, r.nextRunMillis()),
                    outcome(r, Outcome.Kind.CANCELLED, "removed/shutdown"));
        }
        if (timedOut && timeouts >= r.maxConsecutiveTimeouts()) {
            return new Transition(r.settled(TaskState.BLACKLISTED, timeouts, r.nextRunMillis()),
                    outcome(r, Outcome.Kind.BLACKLISTED, "blacklisted after " + timeouts + " consecutive timeouts"));
        }
        long next = r.recurrence().nextAfter(nowMillis, r.zone());
        return new Transition(r.settled(TaskState.SCHEDULED, timeouts, next), null);
    }

    /** Remove: SCHEDULED → canceled now; RUNNING → deferred; BLACKLISTED → discarded. */
    public static RemoveResult remove(TaskRecord r) {
        return switch (r.state()) {
            case SCHEDULED -> new RemoveResult(RemoveResult.Kind.CANCELLED,
                    r.settled(TaskState.CANCELLED, r.consecutiveTimeouts(), r.nextRunMillis()),
                    outcome(r, Outcome.Kind.CANCELLED, "removed"));
            case RUNNING -> new RemoveResult(RemoveResult.Kind.DEFERRED, r.withRemovalRequested(), null);
            case BLACKLISTED -> new RemoveResult(RemoveResult.Kind.BLACKLIST_REMOVED,
                    r.settled(TaskState.CANCELLED, r.consecutiveTimeouts(), r.nextRunMillis()), null);
            case CANCELLED, COMPLETED -> new RemoveResult(RemoveResult.Kind.NOT_FOUND, null, null);
        };
    }

    private static Outcome outcome(TaskRecord r, Outcome.Kind kind, String detail) {
        return new Outcome(r.taskId(), r.submitterNode(), kind, detail);
    }
}
