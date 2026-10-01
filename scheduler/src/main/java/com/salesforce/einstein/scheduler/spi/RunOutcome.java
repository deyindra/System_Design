package com.salesforce.einstein.scheduler.spi;

/** How one execution of a task ended, as reported to {@link TaskStore#finish}. */
public enum RunOutcome {
    SUCCEEDED,
    /** The job threw. */
    FAILED,
    /** The local watchdog fired before the job returned. */
    TIMED_OUT,
    /** The owning node stopped renewing (crashed, paused, partitioned); reported by a reaper. */
    LEASE_EXPIRED;

    public boolean isTimeout() { return this == TIMED_OUT || this == LEASE_EXPIRED; }
}
