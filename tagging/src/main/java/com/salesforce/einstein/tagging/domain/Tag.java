package com.salesforce.einstein.tagging.domain;

import java.time.Instant;
import java.util.Objects;

/**
 * Tag metadata. Assignments reference a tag by {@code tagId} only, so a rename touches one row no
 * matter how many entities carry the tag.
 *
 * @param nameNorm the uniqueness key within a tenant (see {@link TagNames#normalize})
 * @param version  optimistic-concurrency version, exposed to clients as the {@code ETag}
 * @param deleted  soft-delete flag. Assignments of a deleted tag are hidden at once and purged asynchronously.
 */
public record Tag(String tenantId, long tagId, String name, String nameNorm, String color,
                  long version, boolean deleted, String createdBy, Instant createdAt, Instant updatedAt) {

    public Tag {
        Objects.requireNonNull(tenantId, "tenantId");
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(nameNorm, "nameNorm");
    }

    public static Tag create(String tenantId, long tagId, String name, String color, String actor, Instant now) {
        return new Tag(tenantId, tagId, TagNames.display(name), TagNames.normalize(name), color,
                1, false, actor, now, now);
    }

    public Tag renamed(String newName, Instant now) {
        return new Tag(tenantId, tagId, TagNames.display(newName), TagNames.normalize(newName), color,
                version, deleted, createdBy, createdAt, now);
    }

    public Tag recolored(String newColor, Instant now) {
        return new Tag(tenantId, tagId, name, nameNorm, newColor, version, deleted, createdBy, createdAt, now);
    }

    /**
     * Soft-deletes the tag. The name key becomes a per-id tombstone so the name can be reused at once,
     * without a partial unique index (which not every SQL store supports).
     */
    public Tag tombstoned(Instant now) {
        return new Tag(tenantId, tagId, name, TagNames.tombstone(tagId), color, version, true, createdBy, createdAt, now);
    }

    public Tag withVersion(long newVersion) {
        return new Tag(tenantId, tagId, name, nameNorm, color, newVersion, deleted, createdBy, createdAt, updatedAt);
    }
}
