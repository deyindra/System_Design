package com.salesforce.einstein.scheduler;

import java.util.ArrayDeque;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Hand-rolled fixed worker pool built from raw {@link Thread}s and a
 * {@link ReentrantLock}/{@link Condition} — no {@code ExecutorService}/{@code Executors}.
 *
 * <p>The handoff deque is slot-gated by the scheduler: the dispatcher reserves a slot
 * (decrements its {@code freeSlots}) before calling {@link #execute}, and there are exactly
 * {@code poolSize} slots, so {@code handoff.size() + busyCount <= poolSize} always holds.
 * A {@code busy[]} flag per worker lets shutdown drain and interrupt precisely.
 */
public final class TaskPool {
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition workAvailable = lock.newCondition();
    private final Condition idle = lock.newCondition();             // signaled when a worker goes idle
    private final ArrayDeque<Runnable> handoff = new ArrayDeque<>(); // <= poolSize (slot-gated)
    private final Thread[] workers;
    private final boolean[] busy;                                   // busy[idx] == true while worker #idx runs a task
    private int busyCount;
    private volatile boolean running = true;

    public TaskPool(int poolSize, String namePrefix) {
        workers = new Thread[poolSize];
        busy = new boolean[poolSize];
        for (int i = 0; i < poolSize; i++) {
            final int idx = i;
            Thread w = new Thread(() -> workerLoop(idx), namePrefix + "-worker-" + i);
            w.setDaemon(true);
            workers[i] = w;
            w.start();
        }
    }

    /** Enqueue a task for an idle worker. Never blocks (deque is slot-gated by the caller). */
    public void execute(Runnable command) {
        lock.lock();
        try {
            if (!running) throw new RejectedExecutionException("pool stopped");
            handoff.addLast(command);
            workAvailable.signal();
        } finally { lock.unlock(); }
    }

    private void workerLoop(int idx) {
        while (true) {
            Runnable task;
            lock.lock();
            try {
                while (running && handoff.isEmpty()) workAvailable.await();
                if (handoff.isEmpty()) return;          // stopped and drained → worker exits
                task = handoff.pollFirst();
                busy[idx] = true;
                busyCount++;
            } catch (InterruptedException e) {
                continue;                               // interrupted while idle → re-check running
            } finally { lock.unlock(); }

            try {
                task.run();
            } catch (Throwable ignore) {
                // The runTask wrapper already records failures; nothing to do here.
            } finally {
                lock.lock();
                try {
                    busy[idx] = false;
                    busyCount--;
                    idle.signalAll();
                } finally { lock.unlock(); }
            }
        }
    }

    /** Stop accepting new work and wake idle workers so they exit. Does not block. */
    public void initiateStop() {
        lock.lock();
        try {
            running = false;
            workAvailable.signalAll();
        } finally { lock.unlock(); }
    }

    /** Wait up to {@code millis} for all in-flight tasks to finish. @return true if fully drained. */
    public boolean awaitDrain(long millis) {
        long left = TimeUnit.MILLISECONDS.toNanos(millis);    // saturates instead of overflowing
        lock.lock();
        try {
            while (busyCount > 0) {
                if (left <= 0) return false;
                left = idle.awaitNanos(left);                  // returns the remaining wait time
            }
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return busyCount == 0;
        } finally { lock.unlock(); }
    }

    /** Escalation: interrupt every worker currently executing a task. */
    public void interruptActive() {
        lock.lock();
        try {
            for (int i = 0; i < workers.length; i++) {
                if (busy[i]) workers[i].interrupt();
            }
        } finally { lock.unlock(); }
    }

    /** Count of workers still executing (tasks that ignored interrupts → abandoned at shutdown). */
    public int busyCount() {
        lock.lock();
        try { return busyCount; } finally { lock.unlock(); }
    }
}
