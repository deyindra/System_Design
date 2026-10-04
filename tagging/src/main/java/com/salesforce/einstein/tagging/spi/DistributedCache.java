package com.salesforce.einstein.tagging.spi;

import com.salesforce.einstein.tagging.domain.EntityRef;

import java.util.List;
import java.util.Optional;

/**
 * Port to a shared L2 cache of "tag ids of an entity" that sits behind the per-pod L1 (Caffeine).
 *
 * <p>Entries are versioned by {@code asOfSeq}: a writer must never replace an entry with an older one.
 * A Redis adapter does this with a small Lua compare-and-set. Built in: {@code NoopDistributedCache}.
 * Redis/Valkey/Memcached adapters (ElastiCache, Memorystore or self-hosted) live in their own module.
 */
public interface DistributedCache {
    record Entry(List<Long> tagIds, long asOfSeq) {
        public Entry {
            tagIds = tagIds == null ? null : List.copyOf(tagIds);
        }
    }

    Optional<Entry> get(String tenantId, EntityRef entity);

    /** Stores {@code entry} unless the current entry has a higher {@code asOfSeq}. */
    void putIfNewer(String tenantId, EntityRef entity, Entry entry);

    /** Records that the entity changed at {@code seq}, so entries older than that are discarded. */
    void invalidate(String tenantId, EntityRef entity, long seq);
}
