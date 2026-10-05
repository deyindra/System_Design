package com.salesforce.einstein.webcrawler.model;

/**
 * Live counters of a job. {@code pending} reaching zero is the completion signal.
 *
 * @param truncated links not followed because a budget (pages or assets) was exhausted
 */
public record JobStats(long discovered, long pending, long pages, long assets, long reused,
                       long duplicates, long failed, long edges, long truncated) { }
