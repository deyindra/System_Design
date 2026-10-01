package com.salesforce.einstein.scheduler;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

/**
 * Caller-facing handle for a submitted task. Custom completion holder built on a
 * {@link ReentrantLock}/{@link Condition} (no {@code CompletableFuture}/{@code Future}).
 *
 * <p>Jobs are {@link Runnable} (fire-and-forget), so there is no result value — only a
 * completion signal and an optional error ({@code TimeoutException}, the job's thrown
 * exception, or a {@code CancellationException}). A one-time task's handle completes on
 * its single run; a recurring task's handle completes once, on removal or blacklisting.
 */
public final class TaskHandle {
    private final long taskId;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition done = lock.newCondition();
    private boolean completed;
    private Throwable error;                       // null == success
    private volatile Consumer<Throwable> listener;

    TaskHandle(long taskId) { this.taskId = taskId; }

    public long taskId() { return taskId; }

    /** Register a completion callback; invoked once, outside the lock. If already done, fires immediately. */
    public void onComplete(Consumer<Throwable> l) {
        boolean fireNow = false;
        Throwable err = null;
        lock.lock();
        try {
            this.listener = l;
            if (completed) { fireNow = true; err = error; }
        } finally { lock.unlock(); }
        if (fireNow && l != null) l.accept(err);
    }

    /** @param err {@code null} means success. Idempotent — the first call wins. */
    void complete(Throwable err) {
        Consumer<Throwable> l;
        lock.lock();
        try {
            if (completed) return;
            completed = true;
            error = err;
            done.signalAll();
            l = listener;
        } finally { lock.unlock(); }
        if (l != null) l.accept(err);              // fired outside the lock
    }

    public boolean isDone()     { lock.lock(); try { return completed; } finally { lock.unlock(); } }
    public Throwable getError() { lock.lock(); try { return error; }     finally { lock.unlock(); } }

    public void awaitDone() throws InterruptedException {
        lock.lock();
        try { while (!completed) done.await(); } finally { lock.unlock(); }
    }

    /** @return true if the task completed, false if the wait timed out first. */
    public boolean awaitDone(long timeout, TimeUnit unit) throws InterruptedException {
        long nanos = unit.toNanos(timeout);
        lock.lock();
        try {
            while (!completed) {
                if (nanos <= 0) return false;
                nanos = done.awaitNanos(nanos);
            }
            return true;
        } finally { lock.unlock(); }
    }
}
