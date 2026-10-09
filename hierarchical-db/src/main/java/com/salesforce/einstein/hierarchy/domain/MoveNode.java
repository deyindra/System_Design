package com.salesforce.einstein.hierarchy.domain;

import org.springframework.lang.Nullable;

/** Move under {@code newParentId}, between the named siblings. The same parent means a reorder. */
public record MoveNode(long newParentId, @Nullable Long afterId, @Nullable Long beforeId) {
}
