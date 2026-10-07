package com.salesforce.einstein.scheduler.store;

import com.salesforce.einstein.ds.queue.IndexedPriorityQueue;
import com.salesforce.einstein.scheduler.spi.FinishResult;
import com.salesforce.einstein.scheduler.spi.Outcome;
import com.salesforce.einstein.scheduler.spi.RemoveResult;
import com.salesforce.einstein.scheduler.spi.RunOutcome;
import com.salesforce.einstein.scheduler.spi.StoreCounts;
import com.salesforce.einstein.scheduler.spi.TaskRecord;
import com.salesforce.einstein.scheduler.spi.TaskSpec;
import com.salesforce.einstein.scheduler.spi.TaskState;
import com.salesforce.einstein.scheduler.spi.TaskStore;
import com.salesforce.einstein.scheduler.spi.TaskTransitions;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Single-VM store: two {@link IndexedPriorityQueue}s (ready, blacklist) and an id map under one lock —
 * the original scheduler's data structures, moved behind {@link TaskStore}.
 *
 * <p>Not shared: exactly one {@code JobScheduler} may use an instance. Leases never expire (the local
 * watchdog is the only timeout authority), so a task can never be re-run while its first run is alive.
 */
public final class InMemoryTaskStore implements TaskStore {

    /** Mutable IPQ entry; equals/hashCode on the immutable id (IPQ key-stability contract). */
    private static final class Entry {
        final long taskId;
        TaskRecord rec;
        Entry(TaskRecord rec) { this.taskId = rec.taskId(); this.rec = rec; }
        @Override public boolean equals(Object o) { return o instanceof Entry e && e.taskId == taskId; }
        @Override public int hashCode() { return Long.hashCode(taskId); }
    }

    private static final Comparator<Entry> ORDER = Comparator.comparing(e -> e.rec, TaskRecord.SCHEDULE_ORDER);

    private final ReentrantLock lock = new ReentrantLock();
    private final IndexedPriorityQueue<Entry> ready = new IndexedPriorityQueue<>(ORDER);
    private final IndexedPriorityQueue<Entry> blacklist = new IndexedPriorityQueue<>(ORDER);
    private final Map<Long, Entry> byId = new HashMap<>();              // every live task
    private final Map<String, ArrayDeque<Outcome>> outbox = new HashMap<>();
    private long idGen;
    private volatile Runnable listener;

    @Override public boolean isShared() { return false; }

    @Override public TaskRecord insert(TaskSpec spec, int maxTasks) {
        TaskRecord rec;
        lock.lock();
        try {
            if (byId.size() >= maxTasks)
                throw new RejectedExecutionException("capacity " + maxTasks + " reached");
            Entry e = new Entry(TaskRecord.newTask(++idGen, spec));
            byId.put(e.taskId, e);
            ready.offer(e);                                  // O(log N)
            rec = e.rec;
        } finally { lock.unlock(); }
        fireChanged();
        return rec;
    }

    @Override public List<TaskRecord> claimDue(String nodeId, Set<String> jobTypes, long nowMillis,
                                               int limit, long leaseGraceMillis) {
        List<TaskRecord> claimed = new ArrayList<>();
        lock.lock();
        try {
            while (claimed.size() < limit && !ready.isEmpty() && ready.peek().rec.nextRunMillis() <= nowMillis) {
                Entry e = ready.remove();                    // O(log N): earliest, then highest priority
                e.rec = e.rec.claimed(nodeId, Long.MAX_VALUE);
                claimed.add(e.rec);
            }
        } finally { lock.unlock(); }
        return claimed;
    }

    @Override public long nextDueMillis(String nodeId, Set<String> jobTypes) {
        lock.lock();
        try { return ready.isEmpty() ? Long.MAX_VALUE : ready.peek().rec.nextRunMillis(); }
        finally { lock.unlock(); }
    }

    @Override public FinishResult finish(long taskId, long version, RunOutcome result, String detail,
                                         long nowMillis, boolean nodeStopping) {
        FinishResult fr;
        lock.lock();
        try {
            Entry e = byId.get(taskId);
            if (e == null || e.rec.state() != TaskState.RUNNING || e.rec.version() != version)
                return FinishResult.stale();
            TaskTransitions.Transition t =
                TaskTransitions.finish(e.rec, result, detail, nowMillis, nodeStopping, false);
            place(e, t.after());
            publish(t.outcome());
            fr = new FinishResult(true, t.after(), t.outcome());
        } finally { lock.unlock(); }
        fireChanged();
        return fr;
    }

    /** Leases never expire in a non-shared store. */
    @Override public List<FinishResult> reapExpiredLeases(long nowMillis) { return List.of(); }

    @Override public RemoveResult remove(long taskId) {
        RemoveResult rr;
        lock.lock();
        try {
            Entry e = byId.get(taskId);
            if (e == null) return new RemoveResult(RemoveResult.Kind.NOT_FOUND, null, null);
            rr = TaskTransitions.remove(e.rec);
            if (!rr.removed()) return rr;
            switch (e.rec.state()) {                         // leave the queue it was in: O(log N)
                case SCHEDULED -> ready.remove(e);
                case BLACKLISTED -> blacklist.remove(e);
                default -> { }
            }
            place(e, rr.after());
            publish(rr.outcome());
        } finally { lock.unlock(); }
        fireChanged();
        return rr;
    }

    @Override public boolean reschedule(long taskId, long newRunMillis) {
        lock.lock();
        try {
            Entry e = byId.get(taskId);
            if (e == null || e.rec.state() != TaskState.SCHEDULED) return false;
            e.rec = e.rec.rescheduled(newRunMillis);         // mutate key (id/equals unchanged)
            ready.update(e);                                 // O(log N) re-key
        } finally { lock.unlock(); }
        fireChanged();
        return true;
    }

    @Override public List<Outcome> takeOutcomes(String submitterNode) {
        lock.lock();
        try {
            ArrayDeque<Outcome> q = outbox.remove(submitterNode);
            return q == null ? List.of() : new ArrayList<>(q);
        } finally { lock.unlock(); }
    }

    @Override public Optional<TaskRecord> find(long taskId) {
        lock.lock();
        try { return Optional.ofNullable(byId.get(taskId)).map(e -> e.rec); } finally { lock.unlock(); }
    }

    @Override public StoreCounts counts() {
        lock.lock();
        try {
            int live = byId.size();
            return new StoreCounts(ready.size(), live - ready.size() - blacklist.size(), blacklist.size(), live);
        } finally { lock.unlock(); }
    }

    /** @throws IllegalStateException on a second listener: an in-memory store serves exactly one scheduler */
    @Override public void addChangeListener(Runnable l) {
        lock.lock();
        try {
            if (listener != null)
                throw new IllegalStateException("InMemoryTaskStore is single-VM: it serves exactly one JobScheduler");
            listener = l;
        } finally { lock.unlock(); }
    }

    // ---- helpers (caller holds lock) ----------------------------------------

    /** Puts an entry that is in no queue where its new state belongs. */
    private void place(Entry e, TaskRecord after) {
        e.rec = after;
        switch (after.state()) {
            case SCHEDULED -> ready.offer(e);
            case BLACKLISTED -> blacklist.offer(e);
            case CANCELLED, COMPLETED -> byId.remove(e.taskId);
            case RUNNING -> { }                              // deferred removal: stays out of the queues
        }
    }

    private void publish(Outcome o) {
        if (o != null) outbox.computeIfAbsent(o.submitterNode(), k -> new ArrayDeque<>()).add(o);
    }

    private void fireChanged() {
        Runnable l = listener;
        if (l != null) l.run();
    }
}
