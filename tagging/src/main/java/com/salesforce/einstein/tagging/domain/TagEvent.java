package com.salesforce.einstein.tagging.domain;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

/**
 * A change, written to the outbox in the same transaction as the change itself and published
 * afterward in {@code seq} order. Every projection (inverted index, caches, search, downstream
 * products) is built only from these events.
 *
 * <p>Attach and detach events describe the <i>delta</i> ({@code tagIds} actually added or removed).
 * Because each (entity, tag) pair is guarded by its row lock and the event is appended as the
 * transaction's last statement, two events on the same pair are ordered by {@code seq} exactly as
 * they committed. Applying them in {@code seq} order therefore converges to the database state.
 *
 * @param seq       outbox sequence on {@code shard}. It is 0 until the store assigns it.
 * @param entitySeq the store's dense numeric id of the entity. It's the inverted index's posting id and the pagination key.
 * @param actor     who made the change (the caller's actor id), for the audit trail. Null in events written before
 *                  the field existed.
 */
public record TagEvent(long seq, String shard, String tenantId, Type type, long tagId,
                       String entityType, String entityId, long entitySeq, List<Long> tagIds, String actor,
                       Instant at) {

    public enum Type { TAG_CREATED, TAG_UPDATED, TAG_DELETED, TAGS_ATTACHED, TAGS_DETACHED }

    public TagEvent {
        Objects.requireNonNull(tenantId, "tenantId");
        Objects.requireNonNull(type, "type");
        tagIds = tagIds == null ? List.of() : List.copyOf(tagIds);
    }

    public static TagEvent tag(Type type, String tenantId, long tagId, String actor, Instant at) {
        return new TagEvent(0, null, tenantId, type, tagId, null, null, 0, List.of(), actor, at);
    }

    public static TagEvent assignment(Type type, String tenantId, EntityRef e, long entitySeq,
                                      List<Long> tagIds, String actor, Instant at) {
        return new TagEvent(0, null, tenantId, type, 0, e.type(), e.id(), entitySeq, tagIds, actor, at);
    }

    public TagEvent sequenced(String shardId, long newSeq) {
        return new TagEvent(newSeq, shardId, tenantId, type, tagId, entityType, entityId, entitySeq, tagIds, actor, at);
    }

    @JsonIgnore
    public EntityRef entity() {
        return entityType == null ? null : new EntityRef(entityType, entityId);
    }
}
