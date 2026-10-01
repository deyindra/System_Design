package com.salesforce.einstein.scheduler;

/**
 * A named job that any node registering the same type can run (see
 * {@link JobScheduler.Builder#registerJob}). Unlike a {@link Runnable} submission, the work is described
 * by data ({@code payload}), so it survives the submitting node and can fail over in a cluster.
 *
 * <p>In multi-VM mode a run that outlives its lease (owner paused, partitioned, or ignoring its
 * interrupt for longer than {@code leaseGrace}) is settled by another node, and a recurring task's next
 * run may start elsewhere while the old run is still going. Handlers should therefore be idempotent.
 */
@FunctionalInterface
public interface JobHandler {
    void run(String payload) throws Exception;
}
