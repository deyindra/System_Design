package com.salesforce.einstein.scheduler;

import com.salesforce.einstein.scheduler.spi.TaskStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The scheduler's behavioral contract (F1–F12), run once per {@link TaskStore}: every requirement must
 * hold identically in single-VM and multi-VM mode. Subclasses only say how to build a scheduler.
 *
 * <p>Strategy: {@link SchedulerClock.SystemClock} with short real durations for end-to-end
 * dispatch / timeout / blacklist behavior (the dispatcher and watchdog wait on real time, so
 * advancing a manual clock would not wake them). Pure recurrence math lives in {@link RecurrenceTest}.
 */
@Timeout(value = 15, unit = TimeUnit.SECONDS)
abstract class AbstractJobSchedulerTest {

    protected static final ZoneId UTC = ZoneOffset.UTC;
    private static final Duration ONE_SECOND = Duration.ofSeconds(1);
    private static final Recurrence EVERY_SECOND = new Recurrence(RecurrenceUnit.SECOND, 1);

    private final List<JobScheduler> schedulers = new ArrayList<>();

    /** Builds a scheduler over this test's store. */
    protected abstract JobScheduler create(int poolSize, int maxTasks, int maxConsecutiveTimeouts,
                                           String jobType, JobHandler handler);

    /** Creates a scheduler that is always shut down after the test, even if the test fails. */
    private JobScheduler scheduler(int poolSize, int maxTasks, int maxConsecutiveTimeouts) {
        return track(create(poolSize, maxTasks, maxConsecutiveTimeouts, null, null));
    }

    private JobScheduler track(JobScheduler s) {
        schedulers.add(s);
        return s;
    }

    private JobScheduler scheduler() {
        return scheduler(2, 10, 3);
    }

    @AfterEach
    void shutdownSchedulers() {
        // Idempotent: tests that already shut down return immediately here.
        for (JobScheduler s : schedulers) s.shutdown(Duration.ofMillis(300), Duration.ofMillis(200));
    }

    // ---------------------------------------------------------------- submission

    @Test
    @DisplayName("recurring timeout must be shorter than the interval")
    void recurringTimeoutValidation() {
        JobScheduler s = scheduler();
        assertThrows(IllegalArgumentException.class,
                () -> s.submitRecurring(() -> {}, 0, ONE_SECOND, EVERY_SECOND, Instant.now()));

        TaskHandle h = s.submitRecurring(() -> {}, 0, Duration.ofMillis(100), EVERY_SECOND,
                Instant.now().plusSeconds(3600));
        assertNotNull(h);
    }

    @Test
    @DisplayName("maxTasks capacity is enforced")
    void maxTasksRejection() {
        JobScheduler s = scheduler(2, 2, 3);
        Instant far = Instant.now().plusSeconds(3600);
        s.submitOnce(() -> {}, 0, ONE_SECOND, far);
        s.submitOnce(() -> {}, 0, ONE_SECOND, far);

        assertThrows(RejectedExecutionException.class, () -> s.submitOnce(() -> {}, 0, ONE_SECOND, far));
        assertEquals(2, s.getStats().used);
    }

    // ---------------------------------------------------------------- one-time tasks

    @Test
    @DisplayName("one-time tasks dispatch in priority order")
    void oneTimePriorityOrdering() throws InterruptedException {
        JobScheduler s = scheduler(1, 10, 3);   // one worker → due tasks run serially
        List<Integer> order = Collections.synchronizedList(new ArrayList<>());
        Instant due = Instant.now().plusMillis(200);   // all become due together
        List<TaskHandle> handles = new ArrayList<>();
        for (int p : new int[]{1, 5, 3, 9, 2}) {
            handles.add(s.submitOnce(() -> order.add(p), p, ONE_SECOND, due));
        }

        for (TaskHandle h : handles) assertTrue(h.awaitDone(3, TimeUnit.SECONDS));
        assertEquals(List.of(9, 5, 3, 2, 1), order);
    }

    @Test
    @DisplayName("one-time task exceeding its timeout completes with TimeoutException")
    void oneTimeTimeout() throws InterruptedException {
        JobScheduler s = scheduler();
        TaskHandle h = s.submitOnce(() -> sleepUnlessInterrupted(2000), 0, Duration.ofMillis(150), Instant.now());

        assertTrue(h.awaitDone(3, TimeUnit.SECONDS));
        assertInstanceOf(TimeoutException.class, h.getError());
        assertTrue(s.getStats().timedOut >= 1);
    }

    @Test
    @DisplayName("one-time success has a null error; a thrown failure is propagated")
    void oneTimeSuccessAndFailure() throws InterruptedException {
        JobScheduler s = scheduler();

        TaskHandle ok = s.submitOnce(() -> {}, 0, ONE_SECOND, Instant.now());
        assertTrue(ok.awaitDone(2, TimeUnit.SECONDS));
        assertNull(ok.getError());

        TaskHandle bad = s.submitOnce(() -> { throw new IllegalStateException("boom"); }, 0,
                ONE_SECOND, Instant.now());
        assertTrue(bad.awaitDone(2, TimeUnit.SECONDS));
        assertInstanceOf(IllegalStateException.class, bad.getError());
        assertTrue(s.getStats().failed >= 1);
    }

    @Test
    @DisplayName("untimed awaitDone() blocks until a future task completes")
    void untimedAwaitDone() throws InterruptedException {
        JobScheduler s = scheduler();
        AtomicInteger ran = new AtomicInteger();
        TaskHandle h = s.submitOnce(ran::incrementAndGet, 0, ONE_SECOND, Instant.now().plusMillis(200));
        assertFalse(h.isDone());

        h.awaitDone();   // bounded by the class-level @Timeout if it ever hangs

        assertTrue(h.isDone());
        assertNull(h.getError());
        assertEquals(1, ran.get());
    }

    @Test
    @DisplayName("onComplete fires for an already-completed handle")
    void onCompleteAfterDone() throws InterruptedException {
        JobScheduler s = scheduler();
        TaskHandle h = s.submitOnce(() -> {}, 0, ONE_SECOND, Instant.now());
        assertTrue(h.awaitDone(2, TimeUnit.SECONDS));

        CountDownLatch fired = new CountDownLatch(1);
        h.onComplete(err -> fired.countDown());
        assertTrue(fired.await(1, TimeUnit.SECONDS));
    }

    // ---------------------------------------------------------------- recurring tasks

    @Test
    @DisplayName("recurring task auto-reschedules multiple times")
    void recurringReschedules() {
        JobScheduler s = scheduler();
        AtomicInteger runs = new AtomicInteger();
        s.submitRecurring(runs::incrementAndGet, 0, Duration.ofMillis(100), EVERY_SECOND, Instant.now());

        // First run is immediate, then ~1s apart.
        awaitTrue(() -> runs.get() >= 3, Duration.ofSeconds(5), () -> "expected >= 3 runs, got " + runs.get());
        assertTrue(s.getStats().rescheduled >= 2);
    }

    @Test
    @DisplayName("recurring task is blacklisted after consecutive timeouts")
    void recurringBlacklist() throws InterruptedException {
        JobScheduler s = scheduler(2, 10, 2);   // maxConsecutiveTimeouts = 2
        TaskHandle h = s.submitRecurring(() -> sleepUnlessInterrupted(2000), 0, Duration.ofMillis(150),
                EVERY_SECOND, Instant.now());

        assertTrue(h.awaitDone(6, TimeUnit.SECONDS));
        assertInstanceOf(TimeoutException.class, h.getError());
        SchedulerStats st = s.getStats();
        assertEquals(1, st.blacklisted);
        assertTrue(st.timedOut >= 2, "timedOut was " + st.timedOut);
    }

    // ---------------------------------------------------------------- remove / reschedule

    @Test
    @DisplayName("removing a SCHEDULED task cancels it immediately")
    void removeScheduled() throws InterruptedException {
        JobScheduler s = scheduler();
        TaskHandle h = s.submitOnce(() -> {}, 0, ONE_SECOND, Instant.now().plusSeconds(3600));

        assertTrue(s.remove(h.taskId()));
        assertTrue(h.awaitDone(1, TimeUnit.SECONDS));
        assertInstanceOf(CancellationException.class, h.getError());
        SchedulerStats st = s.getStats();
        assertEquals(0, st.used);
        assertTrue(st.cancelled >= 1);
        assertFalse(s.remove(h.taskId()), "second remove of the same id");
    }

    @Test
    @DisplayName("removing a RUNNING recurring task is deferred until the run ends, then dropped")
    void removeRunningDeferred() throws InterruptedException {
        JobScheduler s = scheduler();
        CountDownLatch started = new CountDownLatch(1);
        TaskHandle h = s.submitRecurring(() -> { started.countDown(); sleepUnlessInterrupted(300); },
                0, Duration.ofMillis(500), EVERY_SECOND, Instant.now());

        assertTrue(started.await(2, TimeUnit.SECONDS));
        assertTrue(s.remove(h.taskId()));
        assertTrue(h.awaitDone(2, TimeUnit.SECONDS));
        assertInstanceOf(CancellationException.class, h.getError());
        assertEquals(0, s.getStats().used, "dropped, not rescheduled");
    }

    @Test
    @DisplayName("reschedule re-keys via IPQ.update and the task runs at the new time")
    void rescheduleReKey() throws InterruptedException {
        JobScheduler s = scheduler();
        TaskHandle h = s.submitOnce(() -> {}, 0, ONE_SECOND, Instant.now().plusSeconds(3600));

        assertFalse(h.isDone());
        assertTrue(s.reschedule(h.taskId(), Instant.now()));
        assertTrue(h.awaitDone(2, TimeUnit.SECONDS));
        assertNull(h.getError());
        assertFalse(s.reschedule(h.taskId(), Instant.now()), "finished task can't be rescheduled");
    }

    // ---------------------------------------------------------------- shutdown

    @Test
    @DisplayName("default shutdown() cancels pending tasks and returns promptly when nothing is running")
    void defaultShutdown() throws InterruptedException {
        JobScheduler s = scheduler();
        TaskHandle pending = s.submitOnce(() -> {}, 0, ONE_SECOND, Instant.now().plusSeconds(3600));

        long t0 = System.currentTimeMillis();
        s.shutdown();
        assertTrue(System.currentTimeMillis() - t0 < 1000, "idle drain must not wait the 30s grace period");
        assertTrue(s.isTerminated());
        assertTrue(pending.awaitDone(1, TimeUnit.SECONDS));
        assertInstanceOf(CancellationException.class, pending.getError());
    }

    @Test
    @DisplayName("shutdown drains in-flight tasks and returns true")
    void shutdownDrains() throws InterruptedException {
        JobScheduler s = scheduler();
        AtomicInteger finished = new AtomicInteger();
        CountDownLatch started = new CountDownLatch(1);
        s.submitOnce(() -> { started.countDown(); sleepUnlessInterrupted(200); finished.incrementAndGet(); },
                0, Duration.ofSeconds(2), Instant.now());
        assertTrue(started.await(2, TimeUnit.SECONDS));

        assertTrue(s.shutdown(Duration.ofSeconds(2), Duration.ofMillis(500)), "drained");
        assertEquals(1, finished.get());
        assertTrue(s.isTerminated());
        assertThrows(RejectedExecutionException.class,
                () -> s.submitOnce(() -> {}, 0, ONE_SECOND, Instant.now()));
    }

    @Test
    @DisplayName("bounded shutdown returns even with a non-interruptible task (abandons it)")
    void boundedShutdownWithNonInterruptibleTask() throws InterruptedException {
        JobScheduler s = scheduler();
        CountDownLatch started = new CountDownLatch(1);
        s.submitOnce(() -> {
            started.countDown();
            // Ignores interrupts and spins for ~1s: exercises the abandon path.
            long end = System.currentTimeMillis() + 1000;
            while (System.currentTimeMillis() < end) Thread.onSpinWait();   // busy; never checks the interrupt flag
        }, 0, Duration.ofSeconds(5), Instant.now());
        assertTrue(started.await(2, TimeUnit.SECONDS));

        long t0 = System.currentTimeMillis();
        boolean clean = s.shutdown(Duration.ofMillis(150), Duration.ofMillis(150));
        long elapsed = System.currentTimeMillis() - t0;

        assertFalse(clean, "task should have been abandoned");
        assertTrue(elapsed < 1000, "returned within bound, was " + elapsed + "ms");
        assertTrue(s.isTerminated());
    }

    // ---------------------------------------------------------------- stats

    @Test
    @DisplayName("stats snapshot invariants hold")
    void statsInvariants() {
        JobScheduler s = scheduler();
        Instant far = Instant.now().plusSeconds(3600);
        for (int i = 0; i < 3; i++) s.submitOnce(() -> {}, i, ONE_SECOND, far);

        SchedulerStats st = s.getStats();
        assertEquals(s.nodeId(), st.nodeId);
        assertEquals(st.used, st.pending + st.clusterRunning + st.blacklisted);
        assertEquals(st.running, st.poolSize - st.idleWorkers);
        assertTrue(st.used <= st.capacity);
        assertEquals(st.capacity - st.used, st.remaining);
        assertEquals(3, st.submitted);
    }

    // ---------------------------------------------------------------- named job types

    @Test
    @DisplayName("a registered job type runs with its payload; a thrown failure is propagated as-is")
    void namedJobRunsWithPayload() throws InterruptedException {
        AtomicReference<String> seen = new AtomicReference<>();
        JobScheduler s = track(create(2, 10, 3, "echo", payload -> {
            if ("fail".equals(payload)) throw new IllegalStateException("boom");
            seen.set(payload);
        }));

        TaskHandle ok = s.submitOnce("echo", "hello", 0, ONE_SECOND, Instant.now());
        assertTrue(ok.awaitDone(2, TimeUnit.SECONDS));
        assertNull(ok.getError());
        assertEquals("hello", seen.get());

        TaskHandle bad = s.submitOnce("echo", "fail", 0, ONE_SECOND, Instant.now());
        assertTrue(bad.awaitDone(2, TimeUnit.SECONDS));
        assertInstanceOf(IllegalStateException.class, bad.getError(), "same node → original exception");
    }

    @Test
    @DisplayName("a recurring named job reschedules like a Runnable one")
    void namedJobRecurs() {
        AtomicInteger runs = new AtomicInteger();
        JobScheduler s = track(create(2, 10, 3, "tick", payload -> runs.incrementAndGet()));
        s.submitRecurring("tick", null, 0, Duration.ofMillis(100), EVERY_SECOND, Instant.now());

        awaitTrue(() -> runs.get() >= 2, Duration.ofSeconds(5), () -> "expected >= 2 runs, got " + runs.get());
    }

    @Test
    @DisplayName("the reserved local job type can't be registered or submitted")
    void reservedJobType() {
        assertThrows(IllegalArgumentException.class,
                () -> JobScheduler.builder().registerJob(JobScheduler.LOCAL_JOB_TYPE, p -> {}));
        JobScheduler s = scheduler();
        assertThrows(IllegalArgumentException.class,
                () -> s.submitOnce(JobScheduler.LOCAL_JOB_TYPE, "x", 0, ONE_SECOND, Instant.now()));
    }

    @Test
    @DisplayName("shutdown completes the handle of an abandoned task")
    void shutdownCompletesAbandonedHandle() throws InterruptedException {
        JobScheduler s = scheduler();
        CountDownLatch started = new CountDownLatch(1);
        TaskHandle h = s.submitOnce(() -> {
            started.countDown();
            long end = System.currentTimeMillis() + 700;
            while (System.currentTimeMillis() < end) Thread.onSpinWait();
        }, 0, Duration.ofSeconds(5), Instant.now());
        assertTrue(started.await(2, TimeUnit.SECONDS));

        assertFalse(s.shutdown(Duration.ofMillis(100), Duration.ofMillis(100)));
        assertTrue(h.isDone());
        assertInstanceOf(CancellationException.class, h.getError());
    }

    // ---------------------------------------------------------------- helpers

    /** Sleeps, but returns promptly if interrupted (so the watchdog's interrupt is honored). */
    static void sleepUnlessInterrupted(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** Polls {@code condition} until it holds or {@code timeout} elapses. */
    static void awaitTrue(BooleanSupplier condition, Duration timeout, Supplier<String> message) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) throw new AssertionError(message.get());
            sleepUnlessInterrupted(20);
        }
    }
}
