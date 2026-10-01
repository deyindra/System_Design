package com.salesforce.einstein.scheduler.spi;

/** Lifecycle of a task inside a {@link TaskStore}. */
public enum TaskState { SCHEDULED, RUNNING, CANCELLED, BLACKLISTED, COMPLETED;

    /** Terminal states are deleted from the store (and stop counting toward {@code maxTasks}). */
    public boolean isTerminal() { return this == CANCELLED || this == COMPLETED; }
}
