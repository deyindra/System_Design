package com.salesforce.einstein.tagging.spi;

/** One (tag, entity) assignment as streamed by {@link TagReader#scanAssignments} to bootstrap an index. */
public record Posting(long tagId, String entityType, long entitySeq) {
}
