package com.salesforce.einstein.tagging.spi;

import com.salesforce.einstein.tagging.domain.TagEvent;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Port to the source of truth for <b>one shard</b>. It holds tags, assignments, counters, the outbox
 * and idempotency keys.
 *
 * <p>Built in: {@code JdbcTagStore} (any PostgreSQL-compatible database, H2 for tests) and
 * {@code InMemoryTagStore}. Cloud-specific or NoSQL adapters (Cassandra, DynamoDB, Spanner…) implement
 * this interface in their own module and must pass {@code TagStoreContractTest}.
 *
 * <p>The outbox lives behind this port, not in a separate one, because appending the event must be
 * atomic with the change it describes.
 */
public interface TagStore {
    String shardId();

    Set<Capability> capabilities();

    /** Runs {@code work} as one transaction for {@code tenantId}. Any exception rolls everything back. */
    <T> T write(String tenantId, Function<UnitOfWork, T> work);

    /** Runs {@code work} as a read-only view of {@code tenantId}, served where {@code pref} allows. */
    <T> T read(String tenantId, ReadPreference pref, Function<TagReader, T> work);

    /**
     * Publishes the next unrelayed outbox events in seq order and advances the relay position, both in
     * one transaction. If {@code sink} throws, nothing advances (at-least-once delivery).
     *
     * <p>A seq gap means a transaction that took the seq is still open, or rolled back. The relay stops
     * at a gap until the event after it is older than {@code gapTimeout}. That timeout must exceed the
     * longest possible write transaction, after which the gap is permanent and is skipped. This keeps
     * {@link TagReader#relayedSeq()} an exact watermark: every seq at or below it has been published or
     * will never exist.
     *
     * @return number of events published
     */
    int relay(int maxEvents, Duration gapTimeout, Consumer<List<TagEvent>> sink);

    /**
     * Creation time of the oldest outbox event not yet relayed, or empty when the relay is caught up.
     * {@code now - this} is the shard's projection lag, including time spent waiting on a seq gap.
     */
    Optional<Instant> oldestUnrelayed();

    /** Deletes relayed outbox rows created before {@code cutoff}. */
    int pruneOutbox(Instant cutoff);

    /** Deletes idempotency records created before {@code cutoff}. */
    int pruneIdempotency(Instant cutoff);
}
