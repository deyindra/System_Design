package com.salesforce.einstein.tagging.domain;

import java.util.List;
import java.util.function.Function;

/**
 * One page of a keyset-paginated listing. {@code nextCursor} is opaque to clients and {@code null} on
 * the last page. Keyset pagination (never OFFSET) keeps page N as cheap as page 1, even on a tag with
 * 50M entities.
 */
public record Page<T>(List<T> items, String nextCursor) {
    public Page {
        items = List.copyOf(items);
    }

    public static <T> Page<T> empty() {
        return new Page<>(List.of(), null);
    }

    /**
     * Builds a page from a query that fetched up to {@code limit + 1} rows. The extra row only signals
     * that another page exists; the cursor points at the last row returned.
     */
    public static <T> Page<T> fromRows(List<T> rows, int limit, Function<T, String> cursorOf) {
        if (rows.size() <= limit) {
            return new Page<>(rows, null);
        }
        List<T> page = rows.subList(0, limit);
        return new Page<>(page, cursorOf.apply(page.get(limit - 1)));
    }
}
