package com.salesforce.einstein.tagging.spi;

import com.salesforce.einstein.tagging.domain.TagEvent;
import com.salesforce.einstein.tagging.domain.TrendRank;

import java.util.List;

/**
 * Port for the trending projection: per-tenant, time-bucketed attach counts built from the event
 * stream. It is derived data (rebuildable by replaying the topic), so it lives in its own database,
 * never on the shard primaries.
 *
 * <p>Built in: {@code JdbcTrendStore} (PostgreSQL/H2) and {@code InMemoryTrendStore}. A Redis
 * (sorted sets) or ClickHouse adapter implements the same interface and must pass
 * {@code TrendStoreContractTest}.
 */
public interface TrendStore {

    /** One tag's attach counts in a window and in the equally long window before it. */
    record TagCount(long tagId, long count, long previousCount) {
    }

    /**
     * Applies {@code events}, all relayed by {@code sourceShard}, <b>exactly once</b>. Delivery is
     * at-least-once and in seq order per tenant, so the store keeps a high-water seq per
     * (tenant, source shard) and skips anything at or below it, atomically with the counts.
     * <ul>
     *   <li>{@code TAGS_ATTACHED} adds one per tag to the bucket of {@code event.at()} at every grain.</li>
     *   <li>{@code TAG_DELETED} drops the tag's counters (reads filter deleted tags too, for the lag).</li>
     *   <li>Other types are ignored.</li>
     * </ul>
     */
    void record(String sourceShard, List<TagEvent> events);

    /**
     * The top {@code limit} tags of a tenant at {@code grainSeconds}, over buckets
     * {@code [firstBucket, lastBucket]}. {@code previousCount} covers the same number of buckets
     * before {@code firstBucket}. Ties break by tag id, so pages are stable.
     */
    List<TagCount> top(String tenantId, int grainSeconds, long firstBucket, long lastBucket, TrendRank rank, int limit);

    /** Deletes counters at {@code grainSeconds} older than {@code beforeBucket}. Returns rows removed. */
    int prune(int grainSeconds, long beforeBucket);
}
