package com.salesforce.einstein.tagging.spi;

import java.util.List;

/**
 * A page of matching entity seqs from the inverted index.
 *
 * @param entitySeqs ascending, at most {@code limit}
 * @param total      exact number of matches
 * @param hasMore    whether matches exist after the last seq in this page
 */
public record SearchPostings(List<Long> entitySeqs, long total, boolean hasMore) {
    public SearchPostings {
        entitySeqs = List.copyOf(entitySeqs);
    }
}
