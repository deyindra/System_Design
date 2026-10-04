package com.salesforce.einstein.tagging.domain;

import java.time.Instant;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A trending window: {@code buckets} consecutive buckets of {@code grainSeconds}, ending with the
 * current (partial) bucket. The 24 h window sums hourly buckets and the 7 d window daily ones, so a
 * query reads at most 2 × 24 rows per tag (the window and the one before it, for "rising").
 */
public enum TrendWindow {
    LAST_24H("24h", TrendWindow.HOUR, 24),
    LAST_7D("7d", TrendWindow.DAY, 7);

    public static final int HOUR = 3_600;
    public static final int DAY = 86_400;
    /** Every grain a counter is kept at. */
    public static final Set<Integer> GRAINS = Arrays.stream(values()).map(TrendWindow::grainSeconds)
            .collect(Collectors.toUnmodifiableSet());

    private final String wire;
    private final int grainSeconds;
    private final int buckets;

    TrendWindow(String wire, int grainSeconds, int buckets) {
        this.wire = wire;
        this.grainSeconds = grainSeconds;
        this.buckets = buckets;
    }

    public String wire() {
        return wire;
    }

    public int grainSeconds() {
        return grainSeconds;
    }

    public int buckets() {
        return buckets;
    }

    /** The bucket holding {@code at} at {@code grainSeconds} (UTC, epoch-aligned). */
    public static long bucket(Instant at, int grainSeconds) {
        return Math.floorDiv(at.getEpochSecond(), grainSeconds);
    }

    /** The window's last bucket: the one holding {@code now}. */
    public long lastBucket(Instant now) {
        return bucket(now, grainSeconds);
    }

    /** The window's first bucket. The previous window is the {@link #buckets()} buckets before it. */
    public long firstBucket(Instant now) {
        return lastBucket(now) - buckets + 1;
    }

    /** {@code 24h} or {@code 7d} (the wire names), or the enum name. */
    public static TrendWindow parse(String s) {
        String v = s.trim();
        for (TrendWindow w : values()) {
            if (w.wire.equalsIgnoreCase(v) || w.name().equalsIgnoreCase(v)) {
                return w;
            }
        }
        throw new InvalidRequestException("window must be one of " + Arrays.stream(values()).map(TrendWindow::wire)
                .collect(Collectors.joining(", ")) + "; got '" + s + "'");
    }

    @Override
    public String toString() {
        return wire;
    }
}
