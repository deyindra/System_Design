package com.salesforce.einstein.scheduler;

import com.salesforce.einstein.scheduler.spi.FinishResult;
import com.salesforce.einstein.scheduler.spi.Outcome;
import com.salesforce.einstein.scheduler.spi.RemoveResult;
import com.salesforce.einstein.scheduler.spi.RunOutcome;
import com.salesforce.einstein.scheduler.spi.StoreCounts;
import com.salesforce.einstein.scheduler.spi.TaskRecord;
import com.salesforce.einstein.scheduler.spi.TaskSpec;
import com.salesforce.einstein.scheduler.spi.TaskState;
import com.salesforce.einstein.scheduler.spi.TaskStore;
import com.salesforce.einstein.scheduler.store.InMemoryTaskStore;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A pure-Java job scheduler that runs in one VM or across many.
 *
 * <p>Design: the engine (one dispatcher thread, a hand-rolled {@link TaskPool} of raw worker threads,
 * and a {@link TimeoutWatchdog}) is the same in both modes. Where tasks live is a pluggable
 * {@link TaskStore}:
 * <ul>
 *   <li>{@link InMemoryTaskStore} (default) — single VM, {@link com.salesforce.einstein.ds.IndexedPriorityQueue}
 *       based, nothing leaves the process;</li>
 *   <li>{@link com.salesforce.einstein.scheduler.store.JdbcTaskStore} — multi-VM: every node points at
 *       the same database and claims due tasks with a lease and a fencing token;</li>
 *   <li>anything else (ZooKeeper, Quartz job store, …) that implements {@link TaskStore} and passes the
 *       store contract tests.</li>
 * </ul>
 * No {@code Timer}, {@code ScheduledExecutorService}, {@code ExecutorService} or {@code Future} is used —
 * only raw threads and lock/condition primitives.
 *
 * <p>Two independent bounds apply over one mixed (one-time + recurring) queue: {@code maxTasks} caps
 * live tasks in the store (cluster-wide when shared); {@code poolSize} caps concurrent executions per
 * node. Lock ordering is always scheduler → pool / store, store change listeners run outside the store
 * lock, and workers never hold a lock while running a job, so there is no deadlock.
 *
 * <p>Jobs are either a {@link Runnable} (runs only on the submitting node, exactly as in single-VM
 * mode) or a named {@link JobHandler} type plus a String payload (any node that registered the type can
 * run it, so it survives the submitting node).
 */
public final class JobScheduler {
    /** Job type of {@link Runnable} submissions; such tasks are pinned to the submitting node. */
    public static final String LOCAL_JOB_TYPE = "__local_runnable__";

    private static final System.Logger LOG = System.getLogger(JobScheduler.class.getName());

    private final int poolSize;
    private final int maxTasks;
    private final int maxConsecutiveTimeouts;
    private final SchedulerClock clock;
    private final TaskStore store;
    private final boolean shared;
    private final String nodeId;
    private final long pollMillis;
    private final long leaseGraceMillis;
    private final Map<String, JobHandler> handlers;                    // immutable after construction
    private final Set<String> jobTypes;                                // == handlers.keySet()

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition wake = lock.newCondition();
    private boolean changed;                                           // a signal arrived since the last pass
    private int freeSlots;                                             // worker capacity, guarded by lock
    private final Map<Long, TaskHandle> handles = new HashMap<>();     // tasks this node submitted, until their outcome
    private final Map<String, Runnable> localJobs = new ConcurrentHashMap<>(); // payload key → Runnable (read by workers)
    private final Map<Long, String> localJobKeys = new HashMap<>();    // taskId → localJobs key
    private final Set<Long> localFinishing = new HashSet<>();          // ids a worker of this node is finishing
    private final Map<Long, Outcome> deferredOutcomes = new HashMap<>(); // outbox outcomes that raced a local finish
    private final AtomicLong runIds = new AtomicLong();                // watchdog keys: unique per run
    private final TaskPool pool;
    private final TimeoutWatchdog watchdog;
    private final Thread dispatcher;

    private volatile boolean running = true;
    private volatile boolean terminated = false;
    // stats counters (guarded by lock)
    private long stSubmitted, stCompleted, stTimedOut, stRescheduled, stCancelled, stFailed, stReclaimed;

    /** Single-VM scheduler over an {@link InMemoryTaskStore}. */
    public JobScheduler(int poolSize, int maxTasks, int maxConsecutiveTimeouts, SchedulerClock clock) {
        this(builder().poolSize(poolSize).maxTasks(maxTasks)
                .maxConsecutiveTimeouts(maxConsecutiveTimeouts).clock(clock));
    }

    private JobScheduler(Builder b) {
        if (b.poolSize <= 0 || b.maxTasks <= 0 || b.maxConsecutiveTimeouts <= 0)
            throw new IllegalArgumentException("poolSize, maxTasks, maxConsecutiveTimeouts must be > 0");
        this.poolSize = b.poolSize;
        this.maxTasks = b.maxTasks;
        this.maxConsecutiveTimeouts = b.maxConsecutiveTimeouts;
        this.clock = Objects.requireNonNull(b.clock, "clock");
        this.store = b.store != null ? b.store : new InMemoryTaskStore();
        this.shared = store.isShared();
        this.nodeId = b.nodeId != null ? b.nodeId
                : shared ? "node-" + UUID.randomUUID().toString().substring(0, 8) : "local";
        this.pollMillis = b.pollInterval.toMillis();
        this.leaseGraceMillis = b.leaseGrace.toMillis();
        if (pollMillis <= 0 || leaseGraceMillis < 0)
            throw new IllegalArgumentException("pollInterval must be > 0 and leaseGrace >= 0");
        this.handlers = Map.copyOf(b.handlers);
        this.jobTypes = Set.copyOf(handlers.keySet());
        this.freeSlots = poolSize;

        String prefix = "local".equals(nodeId) ? "scheduler" : "scheduler-" + nodeId;
        this.pool = new TaskPool(poolSize, prefix + "-pool");
        this.watchdog = new TimeoutWatchdog(clock, prefix + "-watchdog");
        store.addChangeListener(this::signalChange);
        this.dispatcher = new Thread(this::dispatchLoop, prefix + "-dispatcher");
        this.dispatcher.setDaemon(true);
        this.dispatcher.start();
    }

    public static Builder builder() { return new Builder(); }

    /** This scheduler's identity in the store: the claim owner and the address for outcomes. */
    public String nodeId() { return nodeId; }

    // ---- submission -------------------------------------------------------

    /** Runs {@code job} once on this node. */
    public TaskHandle submitOnce(Runnable job, int priority, Duration timeout, Instant runAt) {
        return submitLocal(job, priority, timeout, null, runAt);
    }

    /** Runs {@code job} on this node every {@code rec}, starting at {@code firstRunAt}. */
    public TaskHandle submitRecurring(Runnable job, int priority, Duration timeout,
                                      Recurrence rec, Instant firstRunAt) {
        checkRecurring(rec, timeout);
        return submitLocal(job, priority, timeout, rec, firstRunAt);
    }

    /** Runs the registered {@code jobType} once, on any node that has it registered. */
    public TaskHandle submitOnce(String jobType, String payload, int priority, Duration timeout, Instant runAt) {
        return submitNamed(jobType, payload, priority, timeout, null, runAt);
    }

    /** Runs the registered {@code jobType} every {@code rec}, on any node that has it registered. */
    public TaskHandle submitRecurring(String jobType, String payload, int priority, Duration timeout,
                                      Recurrence rec, Instant firstRunAt) {
        checkRecurring(rec, timeout);
        return submitNamed(jobType, payload, priority, timeout, rec, firstRunAt);
    }

    private static void checkRecurring(Recurrence rec, Duration timeout) {
        Objects.requireNonNull(rec, "recurrence");
        Objects.requireNonNull(timeout, "timeout");
        if (timeout.toMillis() >= rec.minIntervalMillis())
            throw new IllegalArgumentException("recurring timeout must be < run interval");
    }

    private TaskHandle submitLocal(Runnable job, int priority, Duration timeout, Recurrence rec, Instant runAt) {
        Objects.requireNonNull(job, "job");
        checkCommon(timeout, runAt);
        String key = UUID.randomUUID().toString();
        return insert(LOCAL_JOB_TYPE, key, nodeId, job, priority, timeout, rec, runAt);
    }

    private TaskHandle submitNamed(String jobType, String payload, int priority, Duration timeout,
                                   Recurrence rec, Instant runAt) {
        Objects.requireNonNull(jobType, "jobType");
        if (LOCAL_JOB_TYPE.equals(jobType)) throw new IllegalArgumentException("reserved job type");
        checkCommon(timeout, runAt);
        // A private store can only ever be served by this node, so the type must be runnable here.
        if (!shared && !handlers.containsKey(jobType))
            throw new IllegalArgumentException("no handler registered for job type '" + jobType + "'");
        return insert(jobType, payload, null, null, priority, timeout, rec, runAt);
    }

    private static void checkCommon(Duration timeout, Instant runAt) {
        Objects.requireNonNull(runAt, "runAt");
        if (timeout == null || timeout.isNegative() || timeout.isZero())
            throw new IllegalArgumentException("timeout must be > 0");
    }

    private TaskHandle insert(String jobType, String payload, String pinnedNode, Runnable localJob,
                              int priority, Duration timeout, Recurrence rec, Instant runAt) {
        TaskSpec spec = new TaskSpec(jobType, payload, pinnedNode, nodeId, priority, timeout.toMillis(),
                rec, clock.zone(), maxConsecutiveTimeouts, runAt.toEpochMilli());
        lock.lock();
        try {
            if (!running) throw new RejectedExecutionException("scheduler shut down");
            // Register the Runnable first: a worker may claim the task the moment it is inserted.
            if (localJob != null) localJobs.put(payload, localJob);
            TaskRecord r;
            try {
                r = store.insert(spec, maxTasks);                 // capacity enforced by the store
            } catch (RuntimeException e) {
                if (localJob != null) localJobs.remove(payload);
                throw e;
            }
            // Still under the lock: an outcome for this id can't be resolved before its handle exists.
            TaskHandle h = new TaskHandle(r.taskId());
            handles.put(r.taskId(), h);
            if (localJob != null) localJobKeys.put(r.taskId(), payload);
            stSubmitted++;
            return h;
        } finally { lock.unlock(); }
    }

    // ---- removal / re-key -------------------------------------------------

    /**
     * Removes a task wherever it is: pending → canceled now; running → dropped when the run ends;
     * blacklisted → discarded. In multi-VM mode any node can remove any task.
     */
    public boolean remove(long taskId) {
        RemoveResult rr = removeFromStore(taskId);
        if (rr.outcome() != null) resolve(taskId, errorFor(rr.outcome(), null));
        return rr.removed();
    }

    /** Removes {@code taskId} from the store and counts it if it was discarded right away. */
    private RemoveResult removeFromStore(long taskId) {
        RemoveResult rr = store.remove(taskId);
        if (rr.kind() == RemoveResult.Kind.CANCELLED || rr.kind() == RemoveResult.Kind.BLACKLIST_REMOVED) {
            lock.lock();
            try { stCancelled++; } finally { lock.unlock(); }
        }
        return rr;
    }

    /** Moves a still-pending task to a new time (the in-memory store re-keys its IPQ in O(log N)). */
    public boolean reschedule(long taskId, Instant newTime) {
        Objects.requireNonNull(newTime, "newTime");
        return store.reschedule(taskId, newTime.toEpochMilli());
    }

    // ---- dispatcher thread ------------------------------------------------

    /** Store change listener: something may be due sooner. */
    private void signalChange() {
        lock.lock();
        try {
            changed = true;
            wake.signal();
        } finally { lock.unlock(); }
    }

    /**
     * One pass: deliver outcomes addressed to this node, settle expired leases (shared stores), claim
     * as many due tasks as there are free workers, then sleep until the next due time, a change signal,
     * or — for shared stores, whose changes on other nodes can't signal us — the poll interval.
     * Store calls run outside the node lock.
     */
    private void dispatchLoop() {
        while (true) {
            int slots;
            lock.lock();
            try {
                if (!running) return;
                changed = false;
                slots = freeSlots;
            } finally { lock.unlock(); }

            long waitMillis;
            try {
                drainOutcomes();
                long now = clock.nowMillis();
                if (shared) reap(now);
                if (slots > 0) {
                    List<TaskRecord> claimed = store.claimDue(nodeId, jobTypes, now, slots, leaseGraceMillis);
                    if (!claimed.isEmpty()) {
                        lock.lock();
                        try { freeSlots -= claimed.size(); } finally { lock.unlock(); }   // reserve workers
                        for (TaskRecord r : claimed) pool.execute(() -> runTask(r));
                        continue;                                         // more may be due right now
                    }
                    long next = store.nextDueMillis(nodeId, jobTypes);
                    waitMillis = next == Long.MAX_VALUE ? Long.MAX_VALUE : next - clock.nowMillis();
                } else {
                    waitMillis = Long.MAX_VALUE;                          // POOL FULL → wait for a free slot
                }
                if (shared) waitMillis = Math.min(waitMillis, pollMillis);
            } catch (RuntimeException e) {
                if (!running) return;
                LOG.log(System.Logger.Level.WARNING, "scheduler " + nodeId + ": store call failed, retrying", e);
                waitMillis = pollMillis;
            }
            if (waitMillis <= 0) continue;

            lock.lock();
            try {
                // Remaining time is irrelevant: whether woken early or on time, the next pass re-reads
                // the store and the clock.
                if (running && !changed) {
                    if (waitMillis == Long.MAX_VALUE) {
                        wake.await();
                    } else {
                        //noinspection ResultOfMethodCallIgnored
                        wake.awaitNanos(capNanos(waitMillis));                // capped: no overflow
                    }
                }
            } catch (InterruptedException e) {
                return;                       // shutdown() sets running=false and signals; nothing else interrupts us
            } finally { lock.unlock(); }
        }
    }

    /** Settles runs whose owner stopped renewing (crashed, paused, partitioned). */
    private void reap(long now) {
        for (FinishResult fr : store.reapExpiredLeases(now)) {
            lock.lock();
            try {
                stReclaimed++;
                countFinish(fr.after().isRecurring(), RunOutcome.LEASE_EXPIRED, fr.after().state());
            } finally { lock.unlock(); }
        }
    }

    /** Completes handles of this node's tasks that were finished or removed anywhere (incl. by other nodes). */
    private void drainOutcomes() {
        for (Outcome o : store.takeOutcomes(nodeId)) {
            boolean defer;
            lock.lock();
            try {
                // A local worker is finishing this task and holds the real Throwable: let it resolve.
                defer = localFinishing.contains(o.taskId());
                if (defer) deferredOutcomes.put(o.taskId(), o);
            } finally { lock.unlock(); }
            if (!defer) resolve(o.taskId(), errorFor(o, null));
        }
    }

    // ---- worker body ------------------------------------------------------

    private void runTask(TaskRecord r) {
        long runId = runIds.incrementAndGet();
        watchdog.arm(runId, clock.nowMillis() + r.timeoutMillis(), Thread.currentThread());
        Throwable failure = null;
        try {
            if (LOCAL_JOB_TYPE.equals(r.jobType())) {
                Runnable job = localJobs.get(r.payload());
                if (job == null) throw new IllegalStateException("local job of task " + r.taskId() + " is gone");
                job.run();
            } else {
                JobHandler h = handlers.get(r.jobType());
                if (h == null) throw new IllegalStateException("no handler for job type '" + r.jobType() + "'");
                h.run(r.payload());
            }
        } catch (Throwable ex) {
            failure = ex;
        }
        boolean timedOut = watchdog.disarm(runId);      // authoritative timeout verdict
        clearInterrupt();                               // a late watchdog interrupt must not leak into the next job
        onTaskFinished(r, timedOut, failure);
    }

    private void onTaskFinished(TaskRecord r, boolean timedOut, Throwable failure) {
        long id = r.taskId();
        RunOutcome ro = timedOut ? RunOutcome.TIMED_OUT
                      : failure != null ? RunOutcome.FAILED : RunOutcome.SUCCEEDED;
        String detail = timedOut ? "task timed out" : failure != null ? failure.toString() : null;

        lock.lock();
        try { localFinishing.add(id); } finally { lock.unlock(); }
        FinishResult fr;
        try {
            // The claim's version is the fencing token: a stale run (lease taken over) is ignored.
            fr = store.finish(id, r.version(), ro, detail, clock.nowMillis(), !running);
        } catch (RuntimeException e) {
            LOG.log(System.Logger.Level.WARNING, "scheduler " + nodeId + ": finish of task " + id
                    + " failed; its lease will expire and another node will settle it", e);
            fr = FinishResult.stale();
        }

        Outcome deferred;
        lock.lock();
        try {
            if (fr.applied()) countFinish(r.isRecurring(), ro, fr.after().state());
            freeSlots++;         // free the reserved worker (mutated under lock → visible to dispatcher)
            changed = true;
            wake.signal();       // wake dispatcher: pool no longer full (and a rescheduled run may be due)
            localFinishing.remove(id);
            deferred = deferredOutcomes.remove(id);
        } finally { lock.unlock(); }

        if (fr.applied() && fr.outcome() != null) resolve(id, errorFor(fr.outcome(), failure));
        else if (deferred != null) resolve(id, errorFor(deferred, null));
    }

    /** Stats for one settled run; caller holds the lock. */
    private void countFinish(boolean recurring, RunOutcome ro, TaskState after) {
        if (recurring) {
            if (ro.isTimeout()) stTimedOut++; else stCompleted++;
            if (after == TaskState.CANCELLED) stCancelled++;
            else if (after == TaskState.SCHEDULED) stRescheduled++;
        } else if (ro.isTimeout()) {
            stTimedOut++;
        } else if (ro == RunOutcome.FAILED) {
            stFailed++;
        } else {
            stCompleted++;
        }
    }

    /** Completes (and forgets) this node's handle for {@code taskId}, if it has one. */
    private void resolve(long taskId, Throwable err) {
        TaskHandle h;
        lock.lock();
        try {
            h = handles.remove(taskId);
            String key = localJobKeys.remove(taskId);
            if (key != null) localJobs.remove(key);
        } finally { lock.unlock(); }
        if (h != null) h.complete(err);                 // listeners run outside the scheduler lock
    }

    private static Throwable errorFor(Outcome o, Throwable localFailure) {
        return switch (o.kind()) {
            case SUCCEEDED -> null;
            case FAILED -> localFailure != null ? localFailure : new JobFailedException(o.detail());
            case TIMED_OUT, BLACKLISTED -> new TimeoutException(o.detail());
            case CANCELLED -> new CancellationException(o.detail());
        };
    }

    // ---- stats / lifecycle ------------------------------------------------

    /** Task gauges from one consistent store read; worker gauges and counters from this node. */
    public SchedulerStats getStats() {
        StoreCounts c = store.counts();
        lock.lock();
        try {
            return new SchedulerStats(nodeId,
                /* gauges   */ c.pending(), poolSize - freeSlots, c.running(), c.blacklisted(),
                               poolSize, freeSlots, maxTasks, c.live(), Math.max(0, maxTasks - c.live()),
                /* counters */ stSubmitted, stCompleted, stTimedOut, stRescheduled, stCancelled, stFailed,
                               stReclaimed);
        } finally { lock.unlock(); }
    }

    public void shutdown() { shutdown(Duration.ofSeconds(30), Duration.ofSeconds(5)); }

    /**
     * Bounded, escalating shutdown — always returns within ({@code graceful + afterInterrupt}) plus
     * store round-trips:
     * <ol>
     *   <li>stop the dispatcher (it runs no user code, so this is immediate) — no new claims;</li>
     *   <li>drain in-flight tasks cooperatively for {@code graceful} (watchdog still enforcing
     *       timeouts);</li>
     *   <li>if any remain, interrupt them and wait {@code afterInterrupt} more;</li>
     *   <li>abandon whatever still ignores interrupts (daemon threads — the JVM cannot force-kill
     *       a thread, and {@code Thread.stop} is unsafe), stop the watchdog;</li>
     *   <li>shared store: remove this node's {@link Runnable} tasks (no other node can run them);
     *       named-type tasks stay and other nodes carry on with them;</li>
     *   <li>complete every handle still open with {@code CancellationException("scheduler shutdown")}.</li>
     * </ol>
     * Recurring runs that end during the drain are dropped unless another node can take them over
     * (shared store, named type).
     * @return true if every task finished; false if any tasks were abandoned (still running).
     */
    public boolean shutdown(Duration graceful, Duration afterInterrupt) {
        lock.lock();
        try {
            if (!running) return terminated;
            running = false;
            wake.signalAll();
        } finally { lock.unlock(); }

        join(dispatcher);                                          // returns promptly (no user code)
        pool.initiateStop();                                       // idle workers exit

        if (!pool.awaitDrain(graceful.toMillis())) {               // watchdog STILL ALIVE here
            pool.interruptActive();                                // escalate
            pool.awaitDrain(afterInterrupt.toMillis());
        }
        int abandoned = pool.busyCount();                          // authoritative: tasks that ignored interrupt

        watchdog.stop();                                           // watchdog last

        try {
            if (shared) {
                List<Long> pinned;
                lock.lock();
                try { pinned = new ArrayList<>(localJobKeys.keySet()); } finally { lock.unlock(); }
                for (long id : pinned) {
                    RemoveResult rr = removeFromStore(id);
                    if (rr.outcome() != null) resolve(id, new CancellationException("scheduler shutdown"));
                }
            }
            drainOutcomes();
        } catch (RuntimeException e) {
            LOG.log(System.Logger.Level.WARNING, "scheduler " + nodeId + ": store unavailable during shutdown", e);
        }

        Map<Long, TaskHandle> open;
        lock.lock();
        try {
            open = new LinkedHashMap<>(handles);
            handles.clear();
        } finally { lock.unlock(); }
        for (TaskHandle h : open.values()) h.complete(new CancellationException("scheduler shutdown"));
        terminated = true;
        return abandoned == 0;
    }

    public boolean isTerminated() { return terminated; }

    /** Clears this thread's interrupt status; the old value is deliberately discarded. */
    private static void clearInterrupt() {
        //noinspection ResultOfMethodCallIgnored
        Thread.interrupted();
    }

    private static void join(Thread t) {
        try { t.join(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }

    /** millis→nanos, capped at 1h so a far-future task can't overflow awaitNanos (the loop re-checks). */
    static long capNanos(long delayMillis) {
        return Math.min(delayMillis, 3_600_000L) * 1_000_000L;
    }

    // ---- configuration ----------------------------------------------------

    /**
     * Configures a scheduler. Defaults: single VM ({@link InMemoryTaskStore}), {@code poolSize} = CPU
     * count, {@code maxTasks} = 10 000, {@code maxConsecutiveTimeouts} = 3, system clock in the
     * default zone, poll every 500 ms and 30 s lease grace (both multi-VM only).
     */
    public static final class Builder {
        private int poolSize = Runtime.getRuntime().availableProcessors();
        private int maxTasks = 10_000;
        private int maxConsecutiveTimeouts = 3;
        private SchedulerClock clock = new SchedulerClock.SystemClock(ZoneId.systemDefault());
        private TaskStore store;
        private String nodeId;
        private Duration pollInterval = Duration.ofMillis(500);
        private Duration leaseGrace = Duration.ofSeconds(30);
        private final Map<String, JobHandler> handlers = new HashMap<>();

        private Builder() {}

        public Builder poolSize(int n) { this.poolSize = n; return this; }

        /** Live-task bound; cluster-wide with a shared store (configure every node the same). */
        public Builder maxTasks(int n) { this.maxTasks = n; return this; }

        public Builder maxConsecutiveTimeouts(int n) { this.maxConsecutiveTimeouts = n; return this; }

        public Builder clock(SchedulerClock clock) { this.clock = clock; return this; }

        /** Where tasks live. A shared store (e.g. {@code JdbcTaskStore}) makes this node part of a cluster. */
        public Builder store(TaskStore store) { this.store = Objects.requireNonNull(store, "store"); return this; }

        /** Unique, stable per node; default {@code "local"} (single VM) or a random id (shared store). */
        public Builder nodeId(String nodeId) { this.nodeId = Objects.requireNonNull(nodeId, "nodeId"); return this; }

        /** Multi-VM: how often to look for work submitted by, or freed on, other nodes. */
        public Builder pollInterval(Duration d) { this.pollInterval = Objects.requireNonNull(d, "pollInterval"); return this; }

        /**
         * Multi-VM: a claim's lease lasts {@code timeout + leaseGrace}. Must cover a worker's time to
         * react to its interrupt plus clock skew between nodes.
         */
        public Builder leaseGrace(Duration d) { this.leaseGrace = Objects.requireNonNull(d, "leaseGrace"); return this; }

        /** Lets this node run tasks of {@code jobType}. */
        public Builder registerJob(String jobType, JobHandler handler) {
            Objects.requireNonNull(jobType, "jobType");
            Objects.requireNonNull(handler, "handler");
            if (LOCAL_JOB_TYPE.equals(jobType)) throw new IllegalArgumentException("reserved job type");
            if (handlers.putIfAbsent(jobType, handler) != null)
                throw new IllegalArgumentException("job type '" + jobType + "' already registered");
            return this;
        }

        public JobScheduler build() { return new JobScheduler(this); }
    }
}
