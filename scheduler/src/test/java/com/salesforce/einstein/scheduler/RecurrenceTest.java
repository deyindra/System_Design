package com.salesforce.einstein.scheduler;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Pure {@link Recurrence} math (no threads, no store) for calendar correctness. */
class RecurrenceTest {

    private static final ZoneId UTC = ZoneOffset.UTC;

    // ---------------------------------------------------------------- recurrence math

    @Test
    @DisplayName("recurrence next-run math is calendar-correct")
    void recurrenceMath() {
        long base = millis(ZonedDateTime.of(2026, 1, 31, 12, 0, 0, 0, UTC));

        assertEquals(base + 5_000L, new Recurrence(RecurrenceUnit.SECOND, 5).nextAfter(base, UTC));
        assertEquals(base + 120_000L, new Recurrence(RecurrenceUnit.MINUTE, 2).nextAfter(base, UTC));
        assertEquals(base + 3_600_000L, new Recurrence(RecurrenceUnit.HOUR, 1).nextAfter(base, UTC));
        assertEquals(base + 86_400_000L, new Recurrence(RecurrenceUnit.DAILY, 1).nextAfter(base, UTC));
        // Jan 31 + 1 month = Feb 28 (2026 is not a leap year), not a fixed 30 days.
        assertEquals(millis(ZonedDateTime.of(2026, 2, 28, 12, 0, 0, 0, UTC)),
                new Recurrence(RecurrenceUnit.MONTHLY, 1).nextAfter(base, UTC));
        assertEquals(millis(ZonedDateTime.of(2027, 1, 31, 12, 0, 0, 0, UTC)),
                new Recurrence(RecurrenceUnit.YEARLY, 1).nextAfter(base, UTC));

        assertEquals(5_000L, new Recurrence(RecurrenceUnit.SECOND, 5).minIntervalMillis());
    }

    @Test
    @DisplayName("ManualClock drives deterministic recurrence math")
    void manualClockDrivesRecurrence() {
        long start = millis(ZonedDateTime.of(2026, 3, 31, 9, 0, 0, 0, UTC));
        SchedulerClock.ManualClock clock = new SchedulerClock.ManualClock(start, UTC);
        Recurrence monthly = new Recurrence(RecurrenceUnit.MONTHLY, 1);

        // Mar 31 + 1 month clamps to Apr 30.
        long first = monthly.nextAfter(clock.nowMillis(), clock.zone());
        assertEquals(millis(ZonedDateTime.of(2026, 4, 30, 9, 0, 0, 0, UTC)), first);

        clock.advance(first - start);
        assertEquals(first, clock.nowMillis());
        // From Apr 30 the next month is May 30 — the day-of-month doesn't spring back to 31.
        assertEquals(millis(ZonedDateTime.of(2026, 5, 30, 9, 0, 0, 0, UTC)),
                monthly.nextAfter(clock.nowMillis(), clock.zone()));
    }

    @Test
    @DisplayName("recurrence rejects invalid arguments and interval overflow")
    void recurrenceValidation() {
        assertThrows(IllegalArgumentException.class, () -> new Recurrence(RecurrenceUnit.SECOND, 0));
        assertThrows(NullPointerException.class, () -> new Recurrence(null, 1));
        assertThrows(ArithmeticException.class,
                () -> new Recurrence(RecurrenceUnit.YEARLY, Integer.MAX_VALUE).minIntervalMillis());
    }

    private static long millis(ZonedDateTime t) {
        return t.toInstant().toEpochMilli();
    }
}
