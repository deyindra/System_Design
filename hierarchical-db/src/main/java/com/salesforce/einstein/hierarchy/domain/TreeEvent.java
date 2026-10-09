package com.salesforce.einstein.hierarchy.domain;

import org.springframework.lang.Nullable;

import java.time.Instant;
import java.util.UUID;

/**
 * One outbox row, relayed to Kafka with key = tenant. Delivery is at least once, so consumers must be idempotent; the
 * fields are enough for one to invalidate caches or update a search index without reading the tree back.
 *
 * <p>{@code treeVersion} is the space's version after the change (moves only, else 0). A cross-space move also
 * carries the source space and its new version, since both spaces' cached structure changed.
 */
public record TreeEvent(long seq, UUID tenantId, Type type, long spaceId, long nodeId, @Nullable Long parentId,
                        @Nullable Long oldParentId, @Nullable Long oldSpaceId, long oldTreeVersion,
                        @Nullable Long jobId, long treeVersion, Instant at) {
    public enum Type {
        SPACE_CREATED, NODE_CREATED, NODE_UPDATED, NODE_REORDERED, NODE_MOVED, NODE_TRASHED, NODE_RESTORED,
        NODE_PURGED, RESTRICTIONS_CHANGED, MOVE_STARTED, MOVE_COMPLETED
    }

    /** A change to one node under {@code parentId}; {@code seq} and {@code at} come from the outbox row. */
    public static TreeEvent of(UUID tenantId, Type type, long spaceId, long nodeId, @Nullable Long parentId) {
        return new TreeEvent(0, tenantId, type, spaceId, nodeId, parentId, null, null, 0, null, 0, Instant.EPOCH);
    }

    /** A move (or a step of one) that bumped the space's tree version. */
    public static TreeEvent moved(UUID tenantId, Type type, long spaceId, long nodeId, @Nullable Long parentId,
                                  @Nullable Long oldParentId, @Nullable Long oldSpaceId, long oldTreeVersion,
                                  @Nullable Long jobId, long treeVersion) {
        return new TreeEvent(0, tenantId, type, spaceId, nodeId, parentId, oldParentId, oldSpaceId, oldTreeVersion,
                jobId, treeVersion, Instant.EPOCH);
    }
}
