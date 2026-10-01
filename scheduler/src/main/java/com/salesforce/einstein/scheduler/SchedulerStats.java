package com.salesforce.einstein.scheduler;

/**
 * Snapshot of scheduler state.
 *
 * <p>Task gauges ({@code pending}, {@code clusterRunning}, {@code blacklisted}, {@code used}) come from the
 * {@link com.salesforce.einstein.scheduler.spi.TaskStore} in one consistent read, so in multi-VM mode they
 * are cluster-wide. Worker gauges ({@code running}, {@code idleWorkers}) and all counters are this node's,
 * captured under the node lock. In single-VM mode {@code clusterRunning == running} once the dispatcher
 * has handed claimed tasks to workers.
 */
public final class SchedulerStats {
    public final String nodeId;     // this scheduler's node id ("local" in single-VM mode)
    // Gauges (point-in-time)
    public final int pending;       // scheduled, not yet running (store-wide)
    public final int running;       // executing on this node (== poolSize - idleWorkers)
    public final int clusterRunning;// claimed / executing on any node (store-wide)
    public final int blacklisted;   // parked in the blacklist (store-wide)
    public final int poolSize;      // total worker threads on this node
    public final int idleWorkers;   // free worker slots on this node
    public final int capacity;      // maxTasks
    public final int used;          // live tasks in the store (== pending + clusterRunning + blacklisted)
    public final int remaining;     // capacity - used
    // Counters (cumulative since start, this node)
    public final long submitted;    // accepted by submit()
    public final long completed;    // executions that finished normally (one-time + recurring runs)
    public final long timedOut;     // executions that exceeded their timeout (incl. reclaimed leases)
    public final long rescheduled;  // recurring re-enqueues after a run
    public final long cancelled;    // tasks removed (remove()/shutdown) or blacklist-removed
    public final long failed;       // one-time executions that threw (non-timeout)
    public final long reclaimed;    // expired leases of other (dead) runs this node settled — multi-VM only

    SchedulerStats(String nodeId, int pending, int running, int clusterRunning, int blacklisted,
                   int poolSize, int idleWorkers, int capacity, int used, int remaining,
                   long submitted, long completed, long timedOut,
                   long rescheduled, long cancelled, long failed, long reclaimed) {
        this.nodeId = nodeId;
        this.pending = pending;
        this.running = running;
        this.clusterRunning = clusterRunning;
        this.blacklisted = blacklisted;
        this.poolSize = poolSize;
        this.idleWorkers = idleWorkers;
        this.capacity = capacity;
        this.used = used;
        this.remaining = remaining;
        this.submitted = submitted;
        this.completed = completed;
        this.timedOut = timedOut;
        this.rescheduled = rescheduled;
        this.cancelled = cancelled;
        this.failed = failed;
        this.reclaimed = reclaimed;
    }

    @Override public String toString() {
        return String.format(
            "Stats{node=%s pending=%d running=%d/%d(idle=%d) clusterRunning=%d blacklisted=%d cap=%d/%d | "
          + "submitted=%d completed=%d timedOut=%d rescheduled=%d cancelled=%d failed=%d reclaimed=%d}",
            nodeId, pending, running, poolSize, idleWorkers, clusterRunning, blacklisted, used, capacity,
            submitted, completed, timedOut, rescheduled, cancelled, failed, reclaimed);
    }
}
