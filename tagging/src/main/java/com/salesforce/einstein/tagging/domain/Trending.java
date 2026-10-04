package com.salesforce.einstein.tagging.domain;

import java.time.Instant;
import java.util.List;

/**
 * A tenant's trending tags. Always an <b>eventual</b> read: the counts come from a projection of the
 * event stream, so they trail the source of truth by the relay and consumer lag (and the read cache).
 *
 * @param asOf when the counts were read; the window ends here
 */
public record Trending(TrendWindow window, TrendRank rank, List<Item> items, Instant asOf) {
    public Trending {
        items = List.copyOf(items);
    }

    /** A live tag with its attach counts in the window and in the window before it. */
    public record Item(Tag tag, long count, long previousCount) {
    }
}
