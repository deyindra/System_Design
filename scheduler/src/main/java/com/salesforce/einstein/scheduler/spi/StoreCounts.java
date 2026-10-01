package com.salesforce.einstein.scheduler.spi;

/**
 * Store-wide gauges, read atomically in one store operation.
 * {@code live == pending + running + blacklisted} always holds.
 */
public record StoreCounts(int pending, int running, int blacklisted, int live) {}
