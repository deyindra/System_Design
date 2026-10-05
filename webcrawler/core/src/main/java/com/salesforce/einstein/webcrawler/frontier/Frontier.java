package com.salesforce.einstein.webcrawler.frontier;

import com.salesforce.einstein.webcrawler.model.CrawlTask;

import java.time.Instant;

/**
 * The URL frontier: which URL to fetch next, and when.
 *
 * <p>Contract (Mercator style):
 * <ol>
 *   <li><b>Politeness:</b> at most one task per host is out at a time. After {@link #poll} hands out a host,
 *       the host is unavailable until the worker calls {@link #release} with the earliest time it may be hit again.</li>
 *   <li><b>Priority:</b> among a host's tasks, lower depth first (BFS), then FIFO. BFS reaches the pages nearest the
 *       seeds first, so a budget cut-off loses only the deepest, least valuable pages.</li>
 *   <li><b>Fairness:</b> across hosts, the one that became ready earliest goes first, so one big site cannot starve the rest.</li>
 * </ol>
 * Distributed, this is a partitioned durable log keyed by host (the front queues), and each consumer runs this same
 * in-memory frontier over the partitions it owns (the back queues, where BFS priority is applied). Tasks are
 * acknowledged only after processing, so a crashed worker's task is redelivered.
 *
 * <p>Durability: {@link #push} may return before the task is durable. {@link #release} first makes every task the
 * calling thread pushed durable (a parent is acknowledged only once its children are safe), and {@link #flush}
 * does the same for a thread that doesn't release (the seeds of a new job).
 */
public interface Frontier {

    void push(CrawlTask task);

    /** Next task whose host is ready, waiting up to {@code timeoutMillis}; {@code null} if none. */
    CrawlTask poll(long timeoutMillis) throws InterruptedException;

    /**
     * The worker is done with {@code host}; it may be handed out again at {@code notBefore}.
     *
     * @throws RuntimeException if a task this thread pushed could not be made durable. The host is still released,
     *                          but the current task is not acknowledged, so the caller must leave it open: it is
     *                          redelivered and its children pushed again.
     */
    void release(String host, Instant notBefore);

    /** Blocks until every task this thread pushed is durable; throws if one can't be. */
    default void flush() { }

    long size();
}
