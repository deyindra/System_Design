package com.salesforce.einstein.scheduler.spi;

/**
 * Final result of a task, addressed to the node that submitted it. Stores keep outcomes in a
 * per-submitter outbox ({@link TaskStore#takeOutcomes}) so a handle completes even when another
 * node ran or removed the task.
 *
 * @param detail human-readable reason (exception text, "removed", ...); may be {@code null}
 */
public record Outcome(long taskId, String submitterNode, Kind kind, String detail) {
    public enum Kind { SUCCEEDED, FAILED, TIMED_OUT, BLACKLISTED, CANCELLED }
}
