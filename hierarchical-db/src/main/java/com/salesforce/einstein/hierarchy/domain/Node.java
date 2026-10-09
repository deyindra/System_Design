package com.salesforce.einstein.hierarchy.domain;

import org.springframework.lang.Nullable;

import java.time.Instant;

/**
 * One row of {@code nodes}. Read through the service, {@link #path} and {@link #depth} are always <em>effective</em>
 * values: if a large move is still rewriting this node's stored path, the overlay has already been applied.
 */
public record Node(long id, long spaceId, @Nullable Long parentId, String path, int depth, String rank, String type,
                   String title, NodeStatus status, int version, String createdBy, Instant createdAt,
                   Instant updatedAt) {

    public boolean isRoot() {
        return parentId == null;
    }

    public Node withPath(String newPath, int newDepth) {
        return new Node(id, spaceId, parentId, newPath, newDepth, rank, type, title, status, version, createdBy,
                createdAt, updatedAt);
    }
}
