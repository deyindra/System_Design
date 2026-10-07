package com.salesforce.einstein.graphexecutor.executor;

/**
 * What {@link TaskExecutor#submit} does when {@link TaskExecutor#validateGroup} rejects one or more groups
 * (for {@link TopologicalTaskExecutor}: a group whose dependencies contain a cycle).
 *
 * <p>Either way, the decision is made on the calling thread before any group is dispatched.
 */
public enum IneligibleGroupPolicy {
    /** The whole graph is ineligible: {@code submit} throws {@link IneligibleGraphException} and nothing runs. */
    REJECT_GRAPH,

    /**
     * Only the bad groups are ineligible: each gets a failed {@link GroupResult} straight away, without
     * using a thread, and every other group runs normally.
     */
    ISOLATE_GROUP
}
