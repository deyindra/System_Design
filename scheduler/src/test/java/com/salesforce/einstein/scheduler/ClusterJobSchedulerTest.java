package com.salesforce.einstein.scheduler;

import com.salesforce.einstein.scheduler.spi.RunOutcome;
import com.salesforce.einstein.scheduler.spi.TaskRecord;
import com.salesforce.einstein.scheduler.spi.TaskSpec;
import com.salesforce.einstein.scheduler.store.JdbcTaskStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import javax.sql.DataSource;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.salesforce.einstein.scheduler.AbstractJobSchedulerTest.awaitTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Multi-VM behavior: several scheduler nodes, each with its own {@link JdbcTaskStore} instance, over one
 * shared H2 database — the same topology as separate JVMs sharing a database server.
 */
@Timeout(value = 20, unit = TimeUnit.SECONDS)
class ClusterJobSchedulerTest {
    private static final Duration ONE_SECOND = Duration.ofSeconds(1);
    private static final Recurrence EVERY_SECOND = new Recurrence(RecurrenceUnit.SECOND, 1);

    private DataSource db;
    private final List<JobScheduler> nodes = new ArrayList<>();

    @BeforeEach
    void freshDatabase() { db = H2.freshDatabase(); }

    @AfterEach
    void shutdownNodes() {
        for (JobScheduler n : nodes) n.shutdown(Duration.ofMillis(300), Duration.ofMillis(200));
    }

    private JobScheduler.Builder nodeBuilder(String nodeId) {
        return JobScheduler.builder()
                .nodeId(nodeId).poolSize(2).maxTasks(100).maxConsecutiveTimeouts(3)
                .clock(new SchedulerClock.SystemClock(ZoneOffset.UTC))
                .store(H2.store(db))
                .pollInterval(Duration.ofMillis(25))
                .leaseGrace(Duration.ofMillis(200));
    }

    private JobScheduler start(JobScheduler.Builder b) {
        JobScheduler s = b.build();
        nodes.add(s);
        return s;
    }

    @Test
    @DisplayName("every one-time task runs exactly once across three nodes; handles complete on the submitter")
    void exactlyOnceAcrossNodes() throws InterruptedException {
        Map<String, AtomicInteger> runs = new ConcurrentHashMap<>();
        Set<String> ranOn = ConcurrentHashMap.newKeySet();
        JobHandler count = p -> {
            runs.computeIfAbsent(p, k -> new AtomicInteger()).incrementAndGet();
            ranOn.add(Thread.currentThread().getName().split("-")[1]);
            Thread.sleep(5);
        };
        JobScheduler a = start(nodeBuilder("A").registerJob("count", count));
        start(nodeBuilder("B").registerJob("count", count));
        start(nodeBuilder("C").registerJob("count", count));

        List<TaskHandle> handles = new ArrayList<>();
        Instant due = Instant.now().plusMillis(100);
        for (int i = 0; i < 60; i++) handles.add(a.submitOnce("count", "job-" + i, 0, ONE_SECOND, due));

        for (TaskHandle h : handles) {
            assertTrue(h.awaitDone(10, TimeUnit.SECONDS), "handle " + h.taskId());
            assertNull(h.getError());
        }
        assertEquals(60, runs.size());
        runs.forEach((k, v) -> assertEquals(1, v.get(), k + " ran " + v.get() + " times"));
        assertTrue(ranOn.size() >= 2, "work spread over nodes: " + ranOn);
        assertEquals(0, a.getStats().used);
    }

    @Test
    @DisplayName("a failure on another node completes the submitter's handle with JobFailedException")
    void remoteFailure() throws InterruptedException {
        JobScheduler a = start(nodeBuilder("A"));                       // can't run "boom" itself
        start(nodeBuilder("B").registerJob("boom", p -> { throw new IllegalStateException(p); }));

        TaskHandle h = a.submitOnce("boom", "bad input", 0, ONE_SECOND, Instant.now());
        assertTrue(h.awaitDone(5, TimeUnit.SECONDS));
        assertInstanceOf(JobFailedException.class, h.getError());
        assertEquals("java.lang.IllegalStateException: bad input", h.getError().getMessage());
    }

    @Test
    @DisplayName("Runnable tasks are pinned to the submitting node")
    void runnablesArePinned() throws InterruptedException {
        start(nodeBuilder("A"));
        JobScheduler b = start(nodeBuilder("B"));
        List<String> threads = new ArrayList<>();
        List<TaskHandle> hs = new ArrayList<>();
        for (int i = 0; i < 5; i++)
            hs.add(b.submitOnce(() -> { synchronized (threads) { threads.add(Thread.currentThread().getName()); } },
                    0, ONE_SECOND, Instant.now()));
        for (TaskHandle h : hs) assertTrue(h.awaitDone(5, TimeUnit.SECONDS));
        synchronized (threads) {
            assertEquals(5, threads.size());
            assertTrue(threads.stream().allMatch(t -> t.startsWith("scheduler-B-")), threads.toString());
        }
    }

    @Test
    @DisplayName("nodes only claim job types they registered")
    void jobTypeRouting() throws InterruptedException {
        Map<String, String> ranOn = new ConcurrentHashMap<>();
        JobScheduler a = start(nodeBuilder("A").registerJob("a", p -> ranOn.put(p, "A")));
        start(nodeBuilder("B").registerJob("b", p -> ranOn.put(p, "B")));

        List<TaskHandle> hs = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            hs.add(a.submitOnce("a", "a" + i, 0, ONE_SECOND, Instant.now()));
            hs.add(a.submitOnce("b", "b" + i, 0, ONE_SECOND, Instant.now()));
        }
        for (TaskHandle h : hs) assertTrue(h.awaitDone(5, TimeUnit.SECONDS));
        ranOn.forEach((p, node) -> assertEquals(p.substring(0, 1).toUpperCase(), node, p));
        assertEquals(8, ranOn.size());
    }

    @Test
    @DisplayName("remove from another node cancels the task and completes the submitter's handle")
    void removeFromAnotherNode() throws InterruptedException {
        JobScheduler a = start(nodeBuilder("A").registerJob("t", p -> {}));
        JobScheduler b = start(nodeBuilder("B").registerJob("t", p -> {}));

        TaskHandle h = a.submitOnce("t", null, 0, ONE_SECOND, Instant.now().plusSeconds(3600));
        assertTrue(b.remove(h.taskId()));
        assertTrue(h.awaitDone(5, TimeUnit.SECONDS));
        assertInstanceOf(CancellationException.class, h.getError());
        assertFalse(b.reschedule(h.taskId(), Instant.now()));
    }

    @Test
    @DisplayName("maxTasks is enforced cluster-wide")
    void clusterCapacity() {
        JobScheduler a = start(nodeBuilder("A").maxTasks(3).registerJob("t", p -> {}));
        JobScheduler b = start(nodeBuilder("B").maxTasks(3).registerJob("t", p -> {}));
        assertEquals("A", a.nodeId());
        assertEquals("B", b.getStats().nodeId);
        Instant far = Instant.now().plusSeconds(3600);
        a.submitOnce("t", null, 0, ONE_SECOND, far);
        a.submitOnce(() -> {}, 0, ONE_SECOND, far);
        b.submitOnce("t", null, 0, ONE_SECOND, far);

        assertThrows(RejectedExecutionException.class, () -> b.submitOnce("t", null, 0, ONE_SECOND, far));
        assertThrows(RejectedExecutionException.class, () -> a.submitOnce(() -> {}, 0, ONE_SECOND, far));
        SchedulerStats st = b.getStats();
        assertEquals(3, st.used);
        assertEquals(st.used, st.pending + st.clusterRunning + st.blacklisted);
    }

    @Test
    @DisplayName("a crashed node's lease expires; a live node reclaims the task and the dead run is fenced off")
    void failoverAfterCrash() {
        // The "dead" node: a bare store that submits and claims a recurring task, then never finishes it.
        JdbcTaskStore deadStore = H2.store(db);
        long now = System.currentTimeMillis();
        long id = deadStore.insert(new TaskSpec("t", null, null, "dead", 0, 100,
                EVERY_SECOND, ZoneOffset.UTC, 3, now), 100).taskId();
        TaskRecord stale = deadStore.claimDue("dead", Set.of("t"), now, 1, 0).get(0);   // lease: now + 100ms

        AtomicInteger runs = new AtomicInteger();
        JobScheduler live = start(nodeBuilder("live").registerJob("t", p -> runs.incrementAndGet()));

        awaitTrue(() -> live.getStats().reclaimed == 1, Duration.ofSeconds(5), () -> "lease never reaped");
        awaitTrue(() -> runs.get() >= 1, Duration.ofSeconds(5), () -> "never re-run on the live node");
        assertFalse(deadStore.finish(id, stale.version(), RunOutcome.SUCCEEDED, null,
                System.currentTimeMillis(), false).applied(), "the dead node's late finish must be ignored");
        assertTrue(live.getStats().timedOut >= 1, "an expired lease counts as a timeout");
    }

    @Test
    @DisplayName("a node shutting down hands its named recurring tasks to the rest of the cluster")
    void shutdownHandsOver() {
        Map<String, AtomicInteger> runsBy = new ConcurrentHashMap<>();
        JobHandler tick = p -> runsBy.computeIfAbsent(Thread.currentThread().getName().split("-")[1],
                k -> new AtomicInteger()).incrementAndGet();
        JobScheduler a = start(nodeBuilder("A").registerJob("tick", tick));
        JobScheduler b = start(nodeBuilder("B").registerJob("tick", tick));
        a.submitRecurring("tick", null, 0, Duration.ofMillis(100), EVERY_SECOND, Instant.now());
        TaskHandle pinned = a.submitRecurring(() -> {}, 0, Duration.ofMillis(100), EVERY_SECOND, Instant.now());

        awaitTrue(() -> runsBy.values().stream().mapToInt(AtomicInteger::get).sum() >= 1, Duration.ofSeconds(5),
                () -> "never ran");
        assertTrue(a.shutdown(Duration.ofSeconds(2), Duration.ofMillis(200)));
        assertInstanceOf(CancellationException.class, pinned.getError());

        int before = runsBy.computeIfAbsent("B", k -> new AtomicInteger()).get();
        awaitTrue(() -> runsBy.get("B").get() >= before + 2, Duration.ofSeconds(5),
                () -> "B did not take over: " + runsBy);
        SchedulerStats st = b.getStats();
        assertEquals(1, st.used, "named task survives, A's Runnable was removed");
    }
}
