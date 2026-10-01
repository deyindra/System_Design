package com.salesforce.einstein.scheduler.spi;

import com.salesforce.einstein.scheduler.Recurrence;
import com.salesforce.einstein.scheduler.RecurrenceUnit;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The state machine shared by every store, tested as pure functions (no threads, no store). */
class TaskTransitionsTest {
    private static final Recurrence EVERY_SECOND = new Recurrence(RecurrenceUnit.SECOND, 1);
    private static final long NOW = 1_000_000L;

    private static TaskRecord running(Recurrence rec, String pinned, int priorTimeouts) {
        TaskSpec spec = new TaskSpec("t", "p", pinned, "submitter", 0, 100, rec, ZoneOffset.UTC, 2, NOW);
        TaskRecord r = TaskRecord.newTask(7, spec).claimed("owner", NOW + 1000);
        for (int i = 0; i < priorTimeouts; i++) {
            r = TaskTransitions.finish(r, RunOutcome.TIMED_OUT, null, NOW, false, true).after().claimed("owner", 0);
        }
        return r;
    }

    @Test
    @DisplayName("one-time: completes with the run's outcome and releases the claim")
    void oneTime() {
        for (RunOutcome ro : RunOutcome.values()) {
            TaskTransitions.Transition t = TaskTransitions.finish(running(null, null, 0), ro, "d", NOW, false, true);
            assertEquals(TaskState.COMPLETED, t.after().state());
            assertNull(t.after().ownerNode());
            Outcome.Kind expected = ro.isTimeout() ? Outcome.Kind.TIMED_OUT
                                  : ro == RunOutcome.FAILED ? Outcome.Kind.FAILED : Outcome.Kind.SUCCEEDED;
            assertEquals(expected, t.outcome().kind());
            assertEquals("submitter", t.outcome().submitterNode());
        }
    }

    @Test
    @DisplayName("recurring: rescheduled from now, version bumped, no outcome")
    void recurringReschedules() {
        TaskRecord r = running(EVERY_SECOND, null, 0);
        TaskTransitions.Transition t = TaskTransitions.finish(r, RunOutcome.SUCCEEDED, null, NOW, false, true);
        assertEquals(TaskState.SCHEDULED, t.after().state());
        assertEquals(NOW + 1000, t.after().nextRunMillis());
        assertEquals(r.version() + 1, t.after().version());
        assertNull(t.outcome());
    }

    @Test
    @DisplayName("recurring: only consecutive timeouts count; a failure resets the streak")
    void onlyConsecutiveTimeoutsBlacklist() {
        TaskRecord once = running(EVERY_SECOND, null, 1);
        assertEquals(1, once.consecutiveTimeouts());
        TaskTransitions.Transition failed = TaskTransitions.finish(once, RunOutcome.FAILED, "x", NOW, false, true);
        assertEquals(0, failed.after().consecutiveTimeouts(), "failure resets");

        TaskTransitions.Transition second = TaskTransitions.finish(once, RunOutcome.TIMED_OUT, null, NOW, false, true);
        assertEquals(TaskState.BLACKLISTED, second.after().state());
        assertEquals(Outcome.Kind.BLACKLISTED, second.outcome().kind());

        TaskTransitions.Transition reaped = TaskTransitions.finish(once, RunOutcome.LEASE_EXPIRED, null, NOW, false, true);
        assertEquals(TaskState.BLACKLISTED, reaped.after().state(), "an expired lease counts as a timeout");
    }

    @Test
    @DisplayName("recurring: dropped on removal, on shutdown unless another node can take over, and when a pinned owner died")
    void recurringDrop() {
        assertDropped(TaskTransitions.finish(running(EVERY_SECOND, null, 0).withRemovalRequested(),
                RunOutcome.SUCCEEDED, null, NOW, false, true), true);
        assertDropped(TaskTransitions.finish(running(EVERY_SECOND, null, 0), RunOutcome.SUCCEEDED, null, NOW, true, false), true);
        assertDropped(TaskTransitions.finish(running(EVERY_SECOND, "n1", 0), RunOutcome.SUCCEEDED, null, NOW, true, true), true);
        assertDropped(TaskTransitions.finish(running(EVERY_SECOND, null, 0), RunOutcome.SUCCEEDED, null, NOW, true, true), false);
        assertDropped(TaskTransitions.finish(running(EVERY_SECOND, "n1", 0), RunOutcome.LEASE_EXPIRED, null, NOW, false, true), true);
        assertDropped(TaskTransitions.finish(running(EVERY_SECOND, null, 0), RunOutcome.LEASE_EXPIRED, null, NOW, false, true), false);
    }

    private static void assertDropped(TaskTransitions.Transition t, boolean dropped) {
        assertEquals(dropped ? TaskState.CANCELLED : TaskState.SCHEDULED, t.after().state());
        if (dropped) assertEquals(Outcome.Kind.CANCELLED, t.outcome().kind());
    }

    @Test
    @DisplayName("remove: scheduled → cancelled, running → deferred (fencing token kept), blacklisted → discarded silently")
    void remove() {
        TaskRecord sched = TaskRecord.newTask(1, new TaskSpec("t", null, null, "s", 0, 100, null, ZoneOffset.UTC, 2, NOW));
        RemoveResult a = TaskTransitions.remove(sched);
        assertEquals(RemoveResult.Kind.CANCELLED, a.kind());
        assertEquals(Outcome.Kind.CANCELLED, a.outcome().kind());

        TaskRecord run = running(EVERY_SECOND, null, 0);
        RemoveResult b = TaskTransitions.remove(run);
        assertEquals(RemoveResult.Kind.DEFERRED, b.kind());
        assertTrue(b.after().removalRequested());
        assertEquals(run.version(), b.after().version());
        assertNull(b.outcome());

        TaskRecord bl = TaskTransitions.finish(running(EVERY_SECOND, null, 1), RunOutcome.TIMED_OUT, null, NOW, false, true).after();
        RemoveResult c = TaskTransitions.remove(bl);
        assertEquals(RemoveResult.Kind.BLACKLIST_REMOVED, c.kind());
        assertNull(c.outcome(), "handle already completed at blacklisting");

        assertEquals(RemoveResult.Kind.NOT_FOUND, TaskTransitions.remove(a.after()).kind());
    }
}
