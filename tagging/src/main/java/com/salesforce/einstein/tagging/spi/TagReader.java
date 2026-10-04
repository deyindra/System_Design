package com.salesforce.einstein.tagging.spi;

import com.salesforce.einstein.tagging.domain.EntityRef;
import com.salesforce.einstein.tagging.domain.Page;
import com.salesforce.einstein.tagging.domain.Tag;
import com.salesforce.einstein.tagging.domain.TagQuery;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * Reads scoped to <b>one tenant</b>. The tenant is bound when the view is opened
 * ({@link TagStore#read}), so no method can reach another tenant's rows.
 */
public interface TagReader {
    /** The tag, including a soft-deleted one. */
    Optional<Tag> findTag(long tagId);

    /** The live tag with this normalized name. */
    Optional<Tag> findTagByName(String nameNorm);

    /** The tags that exist (live or deleted) among {@code tagIds}. */
    List<Tag> findTags(Collection<Long> tagIds);

    /** Live tags whose normalized name starts with {@code prefixNorm}, ordered by name, after {@code afterNameNorm}. */
    Page<Tag> listTags(String prefixNorm, String afterNameNorm, int limit);

    long countLiveTags();

    /** Assignment counts per tag (sum of the sharded counter rows). */
    Map<Long, Long> usage(Collection<Long> tagIds);

    /** Raw tag ids on the entity, ascending. May include a deleted tag whose purge hasn't run yet. */
    List<Long> tagsOf(EntityRef entity);

    /** Entities carrying {@code tagId}, by ascending entitySeq after {@code afterSeq}; up to {@code limit + 1} rows so callers can detect "more". */
    List<EntityHit> entitiesOf(long tagId, String entityType, long afterSeq, int limit);

    /** The SQL path for boolean search: up to {@code q.limit() + 1} hits by ascending entitySeq. */
    List<EntityHit> search(TagQuery q);

    /** Maps entity seqs back to refs (used for a page of index results). Unknown seqs are absent. */
    Map<Long, EntityRef> resolveEntities(Collection<Long> entitySeqs);

    /**
     * The highest outbox seq of this tenant that this view sees committed, or 0 if none is retained. Read on
     * the primary it is a read barrier: every write acknowledged before the call has a seq at or below it.
     */
    long latestSeq();

    /** The outbox position relayed so far as seen by this view: every event with seq ≤ this is published. */
    long relayedSeq();

    /** Streams every assignment of a live tag. Use inside a {@link ReadPreference#snapshotRead()} read. */
    void scanAssignments(Consumer<Posting> sink);
}
