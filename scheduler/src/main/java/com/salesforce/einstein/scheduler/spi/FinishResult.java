package com.salesforce.einstein.scheduler.spi;

/**
 * Result of {@link TaskStore#finish}.
 *
 * @param applied {@code false} if the fencing token was stale (the run was already settled elsewhere)
 * @param after   the task after the transition; {@code null} if not applied
 * @param outcome final result for the submitter, or {@code null} if the task lives on (rescheduled)
 */
public record FinishResult(boolean applied, TaskRecord after, Outcome outcome) {
    private static final FinishResult STALE = new FinishResult(false, null, null);

    public static FinishResult stale() { return STALE; }
}
