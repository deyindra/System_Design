package com.salesforce.einstein.scheduler.spi;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.ZoneOffset;
import java.util.List;
import java.util.Set;
import java.util.concurrent.RejectedExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Extra contract for shared (multi-VM) stores: several nodes, each with its own store instance over
 * the same data. Subclasses also implement {@link #peerOf}.
 */
public abstract class SharedTaskStoreContractTest extends TaskStoreContractTest {

    /** Another node's store instance over the same data. */
    protected abstract TaskStore peerOf(TaskStore store);

    @Test
    @DisplayName("a shared store says so")
    void isShared() {
        assertTrue(store.isShared());
    }

    @Test
    @DisplayName("a node claims tasks pinned to it, or unpinned of a type it can run")
    void sharedClaimFilter() {
        long pinnedOther = store.insert(new TaskSpec("x", null, "n2", NODE, 0, 100, null, ZoneOffset.UTC, 2, NOW), 10).taskId();
        long pinnedMine = store.insert(new TaskSpec("x", null, NODE, NODE, 0, 100, null, ZoneOffset.UTC, 2, NOW), 10).taskId();
        long otherType = store.insert(new TaskSpec("u", null, null, NODE, 0, 100, null, ZoneOffset.UTC, 2, NOW), 10).taskId();
        long mine = store.insert(once(NOW, 0), 10).taskId();

        List<Long> got = store.claimDue(NODE, TYPES, NOW, 10, 1000).stream().map(TaskRecord::taskId).toList();
        assertEquals(Set.of(pinnedMine, mine), Set.copyOf(got));
        assertEquals(List.of(otherType), store.claimDue("n3", Set.of("u"), NOW, 10, 1000).stream().map(TaskRecord::taskId).toList());
        assertEquals(List.of(pinnedOther), store.claimDue("n2", Set.of(), NOW, 10, 1000).stream().map(TaskRecord::taskId).toList());
    }

    @Test
    @DisplayName("each task is claimed by exactly one node")
    void sharedExclusiveClaim() {
        TaskStore peer = peerOf(store);
        for (int i = 0; i < 10; i++) store.insert(once(NOW, 0), 100);
        List<TaskRecord> a = store.claimDue(NODE, TYPES, NOW, 6, 1000);
        List<TaskRecord> b = peer.claimDue("n2", TYPES, NOW, 6, 1000);
        assertEquals(10, a.size() + b.size());
        assertTrue(a.stream().noneMatch(x -> b.stream().anyMatch(y -> y.taskId() == x.taskId())));
    }

    @Test
    @DisplayName("an expired lease is reaped once (as a timeout) and the old owner's finish is stale")
    void sharedLeaseReap() {
        TaskStore peer = peerOf(store);
        long id = store.insert(recurringDueNow(), 10).taskId();
        TaskRecord r = store.claimDue("dead", TYPES, NOW, 1, 50).get(0);
        assertEquals(NOW + 150, r.leaseUntilMillis(), "lease = now + timeout + grace");

        assertTrue(peer.reapExpiredLeases(NOW + 150).isEmpty(), "not yet expired");
        List<FinishResult> reaped = peer.reapExpiredLeases(NOW + 151);
        assertEquals(1, reaped.size());
        assertEquals(TaskState.SCHEDULED, reaped.get(0).after().state(), "unpinned: rescheduled for another node");
        assertEquals(1, reaped.get(0).after().consecutiveTimeouts());
        assertTrue(store.reapExpiredLeases(NOW + 151).isEmpty(), "reaped once");

        assertFalse(store.finish(id, r.version(), RunOutcome.SUCCEEDED, null, NOW + 200, false).applied(),
                "stale owner is fenced off");
    }

    @Test
    @DisplayName("outcomes are addressed to the submitting node; capacity is store-wide")
    void sharedOutboxAndCapacity() {
        TaskStore peer = peerOf(store);
        long id = store.insert(once(NOW, 0), 2).taskId();
        peer.insert(new TaskSpec("t", null, null, "n2", 0, 100, null, ZoneOffset.UTC, 2, NOW + 9999), 2);
        assertThrows(RejectedExecutionException.class, () -> peer.insert(once(NOW, 0), 2));

        TaskRecord r = peer.claimDue("n2", TYPES, NOW, 1, 1000).get(0);
        assertEquals(id, r.taskId());
        peer.finish(id, r.version(), RunOutcome.FAILED, "java.lang.IllegalStateException: boom", NOW, false);
        assertTrue(peer.takeOutcomes("n2").isEmpty());
        Outcome o = store.takeOutcomes(NODE).get(0);
        assertEquals(Outcome.Kind.FAILED, o.kind());
        assertEquals("java.lang.IllegalStateException: boom", o.detail());
        assertEquals(1, store.counts().live());
    }
}
