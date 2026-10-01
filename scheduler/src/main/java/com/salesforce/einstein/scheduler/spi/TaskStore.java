package com.salesforce.einstein.scheduler.spi;

import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Where tasks live. This is the one extension point that separates single-VM from multi-VM:
 * the {@code JobScheduler} engine (dispatcher, pool, watchdog) is the same on top of every store.
 *
 * <ul>
 *   <li>{@code InMemoryTaskStore} — single VM, {@link #isShared()} {@code false}. Built on
 *       {@code IndexedPriorityQueue}; the original behavior.</li>
 *   <li>{@code JdbcTaskStore} — multi-VM over any SQL database, {@link #isShared()} {@code true}.</li>
 *   <li>Future: ZooKeeper, Quartz, Redis… — implement this interface and pass {@code TaskStoreContractTest}.</li>
 * </ul>
 *
 * <h2>Contract</h2>
 * <ol>
 *   <li><b>Atomic:</b> every method is atomic with respect to every other caller on every node.</li>
 *   <li><b>Exclusive claim:</b> a SCHEDULED task is handed to at most one {@link #claimDue} caller;
 *       claiming increments {@link TaskRecord#version()}, which becomes the run's fencing token.</li>
 *   <li><b>Fencing:</b> {@link #finish} applies only if the task is RUNNING with the same version;
 *       otherwise it returns {@link FinishResult#stale()} and changes nothing.</li>
 *   <li><b>Same rules everywhere:</b> state changes are computed with {@link TaskTransitions}.</li>
 *   <li><b>Capacity:</b> {@link #insert} rejects once {@code maxTasks} non-terminal tasks exist
 *       (across all nodes for a shared store).</li>
 *   <li><b>Outcomes:</b> every final {@link Outcome} is queued for its submitter until
 *       {@link #takeOutcomes} returns it, exactly once.</li>
 *   <li><b>Order:</b> {@link #claimDue} returns tasks in {@link TaskRecord#SCHEDULE_ORDER}.</li>
 * </ol>
 */
public interface TaskStore {

    /** {@code true} if several scheduler nodes (VMs) may use this store concurrently. */
    boolean isShared();

    /**
     * Adds a SCHEDULED task.
     *
     * @throws java.util.concurrent.RejectedExecutionException at capacity
     */
    TaskRecord insert(TaskSpec spec, int maxTasks);

    /**
     * Atomically claims up to {@code limit} tasks due at {@code nowMillis} that {@code nodeId} can run
     * (pinned to it, or unpinned with a {@code jobType} in {@code jobTypes}), moving them to RUNNING
     * with a lease until {@code nowMillis + timeout + leaseGraceMillis}.
     */
    List<TaskRecord> claimDue(String nodeId, Set<String> jobTypes, long nowMillis, int limit, long leaseGraceMillis);

    /** Earliest {@code nextRunMillis} this node could claim, or {@code Long.MAX_VALUE} if none. */
    long nextDueMillis(String nodeId, Set<String> jobTypes);

    /** Settles a run; see {@link TaskTransitions#finish}. {@code version} is the fencing token from the claim. */
    FinishResult finish(long taskId, long version, RunOutcome result, String detail, long nowMillis, boolean nodeStopping);

    /** Settles every RUNNING task whose lease expired before {@code nowMillis} as {@link RunOutcome#LEASE_EXPIRED}. */
    List<FinishResult> reapExpiredLeases(long nowMillis);

    /** See {@link TaskTransitions#remove}. */
    RemoveResult remove(long taskId);

    /** Moves a SCHEDULED task to a new time. @return {@code false} if it isn't SCHEDULED. */
    boolean reschedule(long taskId, long newRunMillis);

    /** Removes and returns the outcomes queued for {@code submitterNode}. */
    List<Outcome> takeOutcomes(String submitterNode);

    Optional<TaskRecord> find(long taskId);

    StoreCounts counts();

    /**
     * Registers a best-effort hint fired after a change made <em>through this store instance</em>
     * (never under the store's own lock). Changes made by other VMs are discovered by polling.
     */
    void addChangeListener(Runnable listener);
}
