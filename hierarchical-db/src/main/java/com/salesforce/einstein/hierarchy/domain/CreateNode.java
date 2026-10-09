package com.salesforce.einstein.hierarchy.domain;

import org.springframework.lang.Nullable;

/** Create a child of {@code parentId}; {@code afterId}/{@code beforeId} name siblings, or neither to append. */
public record CreateNode(long parentId, String title, String type, @Nullable Long afterId, @Nullable Long beforeId,
                         @Nullable String body) {
}
