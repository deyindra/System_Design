package com.salesforce.einstein.tagging.domain;

import java.util.List;

/** The live tags of one entity, plus the token a client should keep for read-your-writes. */
public record EntityTags(EntityRef entity, List<Tag> tags, ConsistencyToken token) {
    public EntityTags {
        tags = List.copyOf(tags);
    }
}
