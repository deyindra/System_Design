package com.salesforce.einstein.tagging.cache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import java.time.Duration;
import java.util.Optional;

/**
 * A Caffeine cache whose entries carry {@code asOfSeq}: the relay position read <i>before</i> the
 * value was loaded. Every change with a seq at or below it is reflected in the value.
 *
 * <p>The classic race of read-through caches is: a reader loads the old value, a writer commits and
 * invalidates, then the reader puts the old value back. A tombstone per invalidated key prevents it. The
 * tombstone remembers the newest invalidated seq, and a put whose {@code asOfSeq} is older is dropped.
 * Tombstones expire after {@code tombstoneTtl}, which must exceed the longest read.
 */
final class VersionedCache<K, V> {
    record Versioned<V>(V value, long asOfSeq) {
    }

    private final Cache<K, Versioned<V>> entries;
    private final Cache<K, Long> tombstones;

    VersionedCache(long maxSize, Duration ttl, Duration tombstoneTtl) {
        this.entries = Caffeine.newBuilder().maximumSize(maxSize).expireAfterWrite(ttl).build();
        this.tombstones = Caffeine.newBuilder().maximumSize(maxSize).expireAfterWrite(tombstoneTtl).build();
    }

    /** The entry if it reflects at least {@code minSeq}. */
    Optional<Versioned<V>> get(K key, long minSeq) {
        Versioned<V> v = entries.getIfPresent(key);
        return v != null && v.asOfSeq() >= minSeq ? Optional.of(v) : Optional.empty();
    }

    void putIfNewer(K key, V value, long asOfSeq) {
        entries.asMap().compute(key, (k, cur) -> {
            Long dead = tombstones.getIfPresent(k);
            if (dead != null && dead > asOfSeq) {
                return cur;   // loaded before a change we already know about
            }
            return cur != null && cur.asOfSeq() >= asOfSeq ? cur : new Versioned<>(value, asOfSeq);
        });
    }

    /** The key changed at {@code seq}; drops older entries and blocks older puts. */
    void invalidate(K key, long seq) {
        tombstones.asMap().merge(key, seq, Math::max);
        entries.asMap().computeIfPresent(key, (k, cur) -> cur.asOfSeq() >= seq ? cur : null);
    }
}
