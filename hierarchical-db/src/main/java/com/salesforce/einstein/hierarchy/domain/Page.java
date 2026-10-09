package com.salesforce.einstein.hierarchy.domain;

import org.springframework.lang.Nullable;

import java.util.List;

/** One keyset page. {@code nextCursor} is null on the last page. */
public record Page<T>(List<T> items, @Nullable String nextCursor) {
    public Page {
        items = List.copyOf(items);
    }
}
