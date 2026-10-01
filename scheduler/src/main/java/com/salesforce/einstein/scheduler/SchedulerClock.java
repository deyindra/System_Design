package com.salesforce.einstein.scheduler;

import java.time.ZoneId;
import java.util.Objects;

/**
 * Injectable clock so recurrence math and dispatch timing are testable.
 *
 * <p>Note: the dispatcher and watchdog threads wait on real wall-clock time
 * (via {@code Condition.awaitNanos}). Advancing a {@link ManualClock} therefore
 * does NOT wake them — {@code ManualClock} is intended for deterministic
 * {@link Recurrence} next-run math, while end-to-end timing tests use
 * {@link SystemClock} with short real durations.
 */
public interface SchedulerClock {
    long nowMillis();
    ZoneId zone();

    /** Wall-clock time. The record accessor {@code zone()} implements {@link SchedulerClock#zone()}. */
    record SystemClock(ZoneId zone) implements SchedulerClock {
        public SystemClock {
            Objects.requireNonNull(zone, "zone");
        }
        @Override public long nowMillis() { return System.currentTimeMillis(); }
    }

    /** For deterministic recurrence-math tests. */
    final class ManualClock implements SchedulerClock {
        private long millis;
        private final ZoneId zone;
        public ManualClock(long startMillis, ZoneId zone) {
            this.millis = startMillis;
            this.zone = Objects.requireNonNull(zone, "zone");
        }
        @Override public synchronized long nowMillis() { return millis; }
        public synchronized void advance(long deltaMillis) { millis += deltaMillis; }
        @Override public ZoneId zone() { return zone; }
    }
}
