package com.salesforce.einstein.tagging.domain;

import java.util.List;

/**
 * One page of matching entities.
 *
 * @param total    exact match count when the inverted index served the query; {@link #TOTAL_UNKNOWN} when the SQL path did
 * @param servedBy {@code "index"} or {@code "store"}. It's for observability and tests, not a contract.
 */
public record SearchResult(List<EntityRef> items, String nextCursor, long total, String servedBy) {
    public static final long TOTAL_UNKNOWN = -1;

    public SearchResult {
        items = List.copyOf(items);
    }
}
