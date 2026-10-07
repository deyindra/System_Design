package com.salesforce.einstein.scheduler;

import org.jetbrains.annotations.NotNull;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Objects;

/**
 * A repeat rule for a recurring task: {@code amount} times a {@link RecurrenceUnit}.
 *
 * <p>SECOND/MINUTE/HOUR are fixed durations; DAILY/MONTHLY/YEARLY are computed with
 * calendar-correct {@code ZonedDateTime} arithmetic (a month/year is not a fixed
 * number of millis).
 *
 * @param unit   what to repeat on; never {@code null}
 * @param amount how many units between runs; {@code > 0}
 */
public record Recurrence(RecurrenceUnit unit, int amount) {
    private static final long DAY_MILLIS = 86_400_000L;

    public Recurrence {
        if (amount <= 0) throw new IllegalArgumentException("amount must be > 0");
        Objects.requireNonNull(unit, "unit");
    }

    /** Calendar-correct next run strictly after {@code fromMillis}. */
    public long nextAfter(long fromMillis, ZoneId zone) {
        ZonedDateTime t = Instant.ofEpochMilli(fromMillis).atZone(zone);
        ZonedDateTime next = switch (unit) {
            case SECOND  -> t.plusSeconds(amount);
            case MINUTE  -> t.plusMinutes(amount);
            case HOUR    -> t.plusHours(amount);
            case DAILY   -> t.plusDays(amount);
            case MONTHLY -> t.plusMonths(amount);
            case YEARLY  -> t.plusYears(amount);
        };
        return next.toInstant().toEpochMilli();
    }

    /**
     * Smallest possible interval (floor), used to validate that a timeout is shorter than the period.
     *
     * @throws ArithmeticException if {@code amount} is so large the interval overflows a {@code long}
     */
    public long minIntervalMillis() {
        long unitMillis = switch (unit) {
            case SECOND  -> 1_000L;
            case MINUTE  -> 60_000L;
            case HOUR    -> 3_600_000L;
            case DAILY   -> DAY_MILLIS;
            case MONTHLY -> 28L * DAY_MILLIS;   // February floor
            case YEARLY  -> 365L * DAY_MILLIS;
        };
        return Math.multiplyExact(unitMillis, amount);
    }

    @Override public @NotNull String toString() { return "every " + amount + " " + unit; }
}
