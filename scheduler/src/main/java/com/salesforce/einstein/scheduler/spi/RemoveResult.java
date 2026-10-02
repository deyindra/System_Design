package com.salesforce.einstein.scheduler.spi;

/**
 * Result of {@link TaskStore#remove}.
 *
 * @param after   the task after removal ({@code null} if not found)
 * @param outcome final result for the submitter, or {@code null} if none is due
 */
public record RemoveResult(Kind kind, TaskRecord after, Outcome outcome) {
    public enum Kind {
        NOT_FOUND,
        /** Was SCHEDULED: canceled immediately. */
        CANCELLED,
        /** Was RUNNING: flagged, dropped when the run ends. */
        DEFERRED,
        /** Was BLACKLISTED: discarded (its handle already completed at blacklisting). */
        BLACKLIST_REMOVED
    }

    public boolean removed() { return kind != Kind.NOT_FOUND; }
}
