package com.salesforce.einstein.tagging.spi;

import com.salesforce.einstein.tagging.domain.TagEvent;

import java.util.List;

/**
 * A projection fed by tag events: the inverted index, cache invalidation, the purge job, or a
 * downstream search index. Delivery is at-least-once and in seq order per tenant, so implementations
 * must ignore events they've already applied.
 */
public interface TagEventListener {
    void onEvents(List<TagEvent> events);
}
