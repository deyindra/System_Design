package com.salesforce.einstein.scheduler;

import com.salesforce.einstein.ds.queue.IndexedPriorityQueue;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Hand-rolled per-task timeout enforcer — a single daemon {@link Thread} backed by a third
 * {@link IndexedPriorityQueue}, ordered by deadline. No {@code ScheduledExecutorService}.
 *
 * <p>When a worker starts a task it {@link #arm}s a deadline; on normal completion it
 * {@link #disarm}s. If a deadline elapses first, the watchdog interrupts the worker thread.
 *
 * <p><b>Verdict handshake (race-free):</b> when the watchdog fires it drops the entry from the
 * heap but KEEPS it in {@link #byId} and only sets {@code fired=true}. {@link #disarm} is the
 * sole remover of a fired entry from {@code byId}, so it always reads the correct verdict —
 * both paths serialize on this object's {@code lock}, so the timeout result is never lost.
 */
public final class TimeoutWatchdog {

    static final class Deadline {
        final long taskId;
        final long deadlineMillis;
        final Thread worker;
        volatile boolean fired;
        volatile boolean cancelled;

        Deadline(long taskId, long deadlineMillis, Thread worker) {
            this.taskId = taskId;
            this.deadlineMillis = deadlineMillis;
            this.worker = worker;
        }

        @Override public boolean equals(Object o) {
            return o instanceof Deadline other && other.taskId == taskId;
        }
        @Override public int hashCode() { return Long.hashCode(taskId); }
    }

    private final SchedulerClock clock;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition changed = lock.newCondition();
    private final IndexedPriorityQueue<Deadline> deadlines =
        new IndexedPriorityQueue<>(Comparator.comparingLong((Deadline d) -> d.deadlineMillis));
    private final Map<Long, Deadline> byId = new HashMap<>();
    private final Thread thread;
    private volatile boolean running = true;

    public TimeoutWatchdog(SchedulerClock clock) { this(clock, "scheduler-watchdog"); }

    public TimeoutWatchdog(SchedulerClock clock, String threadName) {
        this.clock = clock;
        thread = new Thread(this::loop, threadName);
        thread.setDaemon(true);
        thread.start();
    }

    /** @param taskId any id unique among armed deadlines (the scheduler passes a per-run id) */
    void arm(long taskId, long deadlineMillis, Thread worker) {
        lock.lock();
        try {
            Deadline d = new Deadline(taskId, deadlineMillis, worker);
            deadlines.offer(d);          // O(log N)
            byId.put(taskId, d);
            changed.signal();
        } finally { lock.unlock(); }
    }

    /** Called when the job finished on its own. @return true if the watchdog had already fired. */
    boolean disarm(long taskId) {
        lock.lock();
        try {
            Deadline d = byId.remove(taskId);
            if (d == null) return false;
            d.cancelled = true;
            deadlines.remove(d);         // O(log N); no-op if the watchdog already polled it
            changed.signal();
            return d.fired;              // authoritative timeout verdict
        } finally { lock.unlock(); }
    }

    private void loop() {
        lock.lock();
        try {
            while (running) {
                if (deadlines.isEmpty()) { changed.await(); continue; }
                Deadline d = deadlines.peek();
                long wait = d.deadlineMillis - clock.nowMillis();
                if (wait > 0) {
                    // Remaining time is irrelevant: arm()/disarm() signal 'changed' when the earliest
                    // deadline may have moved, so after any wake-up the loop re-reads head and clock.
                    //noinspection ResultOfMethodCallIgnored
                    changed.awaitNanos(JobScheduler.capNanos(wait));             // capped: no overflow
                    continue;
                }
                // Fire: drop from the heap but KEEP the byId entry so disarm() can still read
                // d.fired. disarm() (under this same lock) is the only place that removes a
                // fired entry from byId — so there is no lost verdict and no race.
                deadlines.poll();
                d.fired = true;
                d.worker.interrupt();    // best-effort abort
            }
        } catch (InterruptedException ignore) {
            // stop() interrupts us to break out of the wait.
        } finally { lock.unlock(); }
    }

    void stop() {
        lock.lock();
        try {
            running = false;
            changed.signal();
        } finally { lock.unlock(); }
        thread.interrupt();
    }
}
