package com.salesforce.einstein.scheduler.spi;

import com.salesforce.einstein.scheduler.Recurrence;
import com.salesforce.einstein.scheduler.RecurrenceUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.ZoneOffset;
import java.util.List;
import java.util.Set;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The {@link TaskStore} contract. Every backend — in-memory, JDBC, and any future ZooKeeper / Quartz /
 * other adapter — gets a subclass that only implements {@link #newStore()}. Shared (multi-VM) stores
 * extend {@link SharedTaskStoreContractTest} instead, which adds the cross-node cases.
 */
public abstract class TaskStoreContractTest {
    protected static final String NODE = "n1";
    protected static final Set<String> TYPES = Set.of("t");
    private static final Recurrence EVERY_SECOND = new Recurrence(RecurrenceUnit.SECOND, 1);
    protected static final long NOW = 1_000_000L;

    protected TaskStore store;

    /** A fresh, empty store. */
    protected abstract TaskStore newStore();

    @BeforeEach
    void setUp() { store = newStore(); }

    protected static TaskSpec once(long runAt, int priority) {
        return new TaskSpec("t", "p", null, NODE, priority, 100, null, ZoneOffset.UTC, 2, runAt);
    }

    /** A recurring task first due at {@code NOW}. */
    protected static TaskSpec recurringDueNow() {
        return new TaskSpec("t", "p", null, NODE, 0, 100, EVERY_SECOND, ZoneOffset.UTC, 2, NOW);
    }

    private TaskRecord claimOne() {
        List<TaskRecord> c = store.claimDue(NODE, TYPES, NOW, 1, 1000);
        assertEquals(1, c.size());
        return c.get(0);
    }

    @Test
    @DisplayName("insert assigns distinct ids and enforces capacity")
    void insertAndCapacity() {
        long a = store.insert(once(NOW, 0), 2).taskId();
        long b = store.insert(once(NOW, 0), 2).taskId();
        assertTrue(a != b);
        assertThrows(RejectedExecutionException.class, () -> store.insert(once(NOW, 0), 2));
        assertEquals(new StoreCounts(2, 0, 0, 2), store.counts());
    }

    @Test
    @DisplayName("claimDue: only due tasks, earliest → highest priority → FIFO, up to the limit")
    void claimOrderAndLimit() {
        long late = store.insert(once(NOW + 5000, 9), 10).taskId();
        long low = store.insert(once(NOW, 1), 10).taskId();
        long high = store.insert(once(NOW, 5), 10).taskId();
        long earliest = store.insert(once(NOW - 10, 0), 10).taskId();
        long high2 = store.insert(once(NOW, 5), 10).taskId();

        List<TaskRecord> c = store.claimDue(NODE, TYPES, NOW, 3, 1000);
        assertEquals(List.of(earliest, high, high2), c.stream().map(TaskRecord::taskId).toList());
        assertTrue(c.stream().allMatch(r -> r.state() == TaskState.RUNNING && NODE.equals(r.ownerNode())));

        assertEquals(List.of(low), store.claimDue(NODE, TYPES, NOW, 5, 1000).stream().map(TaskRecord::taskId).toList());
        assertEquals(NOW + 5000, store.nextDueMillis(NODE, TYPES));
        assertTrue(store.claimDue(NODE, TYPES, NOW, 5, 1000).isEmpty(), "claimed tasks aren't claimed again");
        assertEquals(late, store.find(late).orElseThrow().taskId());
    }

    @Test
    @DisplayName("finish is fenced by the claim version; one-time outcome goes to the submitter exactly once")
    void finishFencedAndOutcomeOnce() {
        long id = store.insert(once(NOW, 0), 10).taskId();
        TaskRecord r = claimOne();

        assertFalse(store.finish(id, r.version() + 1, RunOutcome.SUCCEEDED, null, NOW, false).applied(), "wrong token");
        FinishResult fr = store.finish(id, r.version(), RunOutcome.SUCCEEDED, null, NOW, false);
        assertTrue(fr.applied());
        assertEquals(TaskState.COMPLETED, fr.after().state());
        assertFalse(store.finish(id, r.version(), RunOutcome.SUCCEEDED, null, NOW, false).applied(), "already settled");

        List<Outcome> out = store.takeOutcomes(NODE);
        assertEquals(1, out.size());
        assertEquals(Outcome.Kind.SUCCEEDED, out.get(0).kind());
        assertTrue(store.takeOutcomes(NODE).isEmpty());
        assertTrue(store.find(id).isEmpty(), "terminal tasks are gone");
        assertEquals(new StoreCounts(0, 0, 0, 0), store.counts(), "capacity freed");
    }

    @Test
    @DisplayName("recurring finish reschedules; consecutive timeouts blacklist; removal of each state")
    void recurringLifecycle() {
        long id = store.insert(recurringDueNow(), 10).taskId();
        TaskRecord r = claimOne();
        FinishResult fr = store.finish(id, r.version(), RunOutcome.TIMED_OUT, null, NOW, false);
        assertEquals(TaskState.SCHEDULED, fr.after().state());
        assertEquals(NOW + 1000, store.find(id).orElseThrow().nextRunMillis());

        r = store.claimDue(NODE, TYPES, NOW + 1000, 1, 1000).get(0);
        fr = store.finish(id, r.version(), RunOutcome.TIMED_OUT, null, NOW + 1000, false);
        assertEquals(TaskState.BLACKLISTED, fr.after().state());
        assertEquals(new StoreCounts(0, 0, 1, 1), store.counts());
        assertEquals(Outcome.Kind.BLACKLISTED, store.takeOutcomes(NODE).get(0).kind());

        assertEquals(RemoveResult.Kind.BLACKLIST_REMOVED, store.remove(id).kind());
        assertEquals(RemoveResult.Kind.NOT_FOUND, store.remove(id).kind());
        assertEquals(new StoreCounts(0, 0, 0, 0), store.counts());
    }

    @Test
    @DisplayName("removing a running task defers it; the run's finish then cancels it")
    void deferredRemove() {
        long id = store.insert(recurringDueNow(), 10).taskId();
        TaskRecord r = claimOne();
        assertEquals(RemoveResult.Kind.DEFERRED, store.remove(id).kind());
        assertTrue(store.takeOutcomes(NODE).isEmpty());

        FinishResult fr = store.finish(id, r.version(), RunOutcome.SUCCEEDED, null, NOW, false);
        assertTrue(fr.applied(), "deferred removal keeps the fencing token valid");
        assertEquals(TaskState.CANCELLED, fr.after().state());
        assertEquals(Outcome.Kind.CANCELLED, store.takeOutcomes(NODE).get(0).kind());
    }

    @Test
    @DisplayName("removing a scheduled task cancels it; reschedule only moves scheduled tasks")
    void removeAndReschedule() {
        long a = store.insert(once(NOW + 5000, 0), 10).taskId();
        assertTrue(store.reschedule(a, NOW));
        assertEquals(NOW, store.find(a).orElseThrow().nextRunMillis());
        claimOne();
        assertFalse(store.reschedule(a, NOW + 1), "running");

        long b = store.insert(once(NOW + 5000, 0), 10).taskId();
        RemoveResult rr = store.remove(b);
        assertEquals(RemoveResult.Kind.CANCELLED, rr.kind());
        assertEquals(Outcome.Kind.CANCELLED, rr.outcome().kind());
        assertEquals(Outcome.Kind.CANCELLED, store.takeOutcomes(NODE).get(0).kind());
        assertFalse(store.reschedule(b, NOW));
    }

    @Test
    @DisplayName("change listener fires on local changes")
    void changeListener() {
        AtomicInteger fired = new AtomicInteger();
        store.addChangeListener(fired::incrementAndGet);
        store.insert(once(NOW, 0), 10);
        assertTrue(fired.get() >= 1);
    }
}
