package com.salesforce.einstein.webcrawler.store;

import java.util.List;

/** One page of a keyset-paginated listing; {@code nextCursor} is {@code null} on the last page. */
public record Slice<T>(List<T> items, String nextCursor) { }
