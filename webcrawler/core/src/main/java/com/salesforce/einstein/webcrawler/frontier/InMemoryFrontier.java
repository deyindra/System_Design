package com.salesforce.einstein.webcrawler.frontier;

import com.salesforce.einstein.webcrawler.model.CrawlTask;

import java.time.Clock;
import java.time.Instant;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Two-level frontier in one process.
 * <ul>
 *   <li><b>Back queues</b>: one priority queue per host (depth, then arrival order).</li>
 *   <li><b>Ready heap</b>: idle hosts that have work, ordered by the time they may next be fetched.</li>
 * </ul>
 * {@code poll} is O(log H), {@code push} is O(log T). A host being fetched is in neither the heap nor available.
 */
public final class InMemoryFrontier implements Frontier {

    private record Item(CrawlTask task, long seq) { }

    private static final Comparator<Item> BFS =
            Comparator.<Item>comparingInt(i -> i.task.depth()).thenComparingLong(i -> i.seq);

    private static final class HostQueue {
        final String host;
        final PriorityQueue<Item> tasks = new PriorityQueue<>(BFS);
        Instant notBefore = Instant.EPOCH;
        boolean busy;
        boolean queued;   // currently in the ready heap
        HostQueue(String host) { this.host = host; }
    }

    private final Clock clock;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition changed = lock.newCondition();
    private final Map<String, HostQueue> hosts = new HashMap<>();
    private final PriorityQueue<HostQueue> ready = new PriorityQueue<>(Comparator.comparing(h -> h.notBefore));
    private long seq;
    private long size;

    public InMemoryFrontier(Clock clock) { this.clock = clock; }

    @Override public void push(CrawlTask task) {
        lock.lock();
        try {
            HostQueue h = hosts.computeIfAbsent(task.host(), HostQueue::new);
            h.tasks.add(new Item(task, seq++));
            size++;
            if (!h.busy && !h.queued) {
                h.queued = true;
                ready.add(h);
            }
            changed.signalAll();
        } finally {
            lock.unlock();
        }
    }

    @Override public CrawlTask poll(long timeoutMillis) throws InterruptedException {
        long remaining = TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        lock.lock();
        try {
            while (true) {
                HostQueue head = ready.peek();
                long waitNanos = remaining;
                if (head != null) {
                    long untilReady = head.notBefore.toEpochMilli() - clock.millis();
                    if (untilReady <= 0) {
                        ready.poll();
                        head.queued = false;
                        head.busy = true;
                        size--;
                        return head.tasks.remove().task;   // a host is in the ready heap only while it has tasks
                    }
                    waitNanos = Math.min(waitNanos, TimeUnit.MILLISECONDS.toNanos(untilReady));
                }
                if (remaining <= 0) return null;
                // awaitNanos returns what is left of the wait it was given, so the difference is the time spent
                remaining -= waitNanos - changed.awaitNanos(waitNanos);
            }
        } finally {
            lock.unlock();
        }
    }

    @Override public void release(String host, Instant notBefore) {
        lock.lock();
        try {
            HostQueue h = hosts.get(host);
            if (h == null) {
                if (!notBefore.isAfter(clock.instant())) return;
                h = new HostQueue(host);   // a host handed over from another node arrives with its delay still running
                hosts.put(host, h);
            }
            h.busy = false;
            h.notBefore = notBefore;
            if (!h.tasks.isEmpty()) {
                h.queued = true;
                ready.add(h);
                changed.signalAll();
            } else if (!notBefore.isAfter(clock.instant())) {
                hosts.remove(host);   // idle and past its delay: nothing to remember
            }
        } finally {
            lock.unlock();
        }
    }

    @Override public long size() {
        lock.lock();
        try { return size; } finally { lock.unlock(); }
    }
}
