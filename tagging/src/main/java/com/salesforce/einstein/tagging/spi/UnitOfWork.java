package com.salesforce.einstein.tagging.spi;

import com.salesforce.einstein.tagging.domain.EntityRef;
import com.salesforce.einstein.tagging.domain.Tag;
import com.salesforce.einstein.tagging.domain.TagEvent;

import java.time.Instant;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * One atomic, single-tenant transaction. Everything done through it, including {@link #append}, commits
 * or rolls back together. That is the transactional-outbox guarantee: there's never a change without its
 * event, nor an event without its change.
 *
 * <p>Rule for callers: {@link #append} is the <b>last</b> mutation of the transaction, so the event's seq
 * is taken after every row lock is held.
 */
public interface UnitOfWork extends TagReader {
    /** @throws com.salesforce.einstein.tagging.domain.DuplicateTagNameException if the normalized name is taken */
    Tag insertTag(Tag tag);

    /**
     * Compare-and-set on {@code version}. Returns the stored tag with {@code version + 1}.
     *
     * @throws com.salesforce.einstein.tagging.domain.StaleVersionException      if the version moved
     * @throws com.salesforce.einstein.tagging.domain.DuplicateTagNameException if a rename collides
     */
    Tag updateTag(Tag tag, long expectedVersion);

    /**
     * The entity's dense seq, creating it when {@code create} is set; 0 when it doesn't exist and
     * {@code create} is false. An existing (or created) entity is <b>locked until commit</b>, so writes
     * to one entity are serialized. Callers lock an entity before touching its assignments, and lock
     * several entities in a fixed order.
     */
    long entitySeq(EntityRef entity, boolean create);

    /** Idempotent set-add. Returns the tag ids that were actually new. */
    Set<Long> attach(EntityRef entity, long entitySeq, Collection<Long> tagIds, String actor, Instant at);

    /** Idempotent set-remove. Returns the tag ids that were actually removed. */
    Set<Long> detach(EntityRef entity, Collection<Long> tagIds);

    /** Adds deltas to the per-tag usage counters (spread over counter buckets to avoid hot rows). */
    void adjustUsage(Map<Long, Long> deltas);

    /** Deletes up to {@code limit} assignments of a (deleted) tag. When none are left it also drops its counters. */
    int purgeAssignments(long tagId, int limit);

    /** Appends to the outbox and returns the event's seq (the consistency token). */
    long append(TagEvent event);

    Optional<IdempotencyRecord> findIdempotency(String key);

    /**
     * Claims the key. Called before the work, so a concurrent request with the same key blocks on the key
     * (or fails) before it allocates any outbox seq.
     *
     * @throws org.springframework.dao.DuplicateKeyException if a concurrent request saved the same key first
     */
    void saveIdempotency(IdempotencyRecord record);

    /** Stores the response of a key claimed earlier in this unit of work. */
    void completeIdempotency(String key, String response);
}
