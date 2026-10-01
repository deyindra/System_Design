package com.salesforce.einstein.scheduler.spi;

import com.salesforce.einstein.scheduler.Recurrence;

import java.time.ZoneId;
import java.util.Objects;

/**
 * Everything needed to create a task. Per-task policy ({@code zone}, {@code maxConsecutiveTimeouts})
 * travels with the task so every node applies the same rules to it.
 *
 * @param jobType      handler name, resolved on the node that runs the task
 * @param payload      opaque argument for the handler; may be {@code null}
 * @param pinnedNode   if non-null, only this node may claim the task (used for in-process {@code Runnable}s)
 * @param recurrence   {@code null} for a one-time task
 */
public record TaskSpec(String jobType, String payload, String pinnedNode, String submitterNode,
                       int priority, long timeoutMillis, Recurrence recurrence, ZoneId zone,
                       int maxConsecutiveTimeouts, long firstRunMillis) {
    public TaskSpec {
        Objects.requireNonNull(jobType, "jobType");
        Objects.requireNonNull(submitterNode, "submitterNode");
        Objects.requireNonNull(zone, "zone");
    }
}
