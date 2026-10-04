package com.salesforce.einstein.tagging.spi;

import com.salesforce.einstein.tagging.domain.TagQuery;

import java.util.Optional;

/**
 * Port to a search projection that answers boolean tag queries. It is fed by events and always
 * rebuildable from the {@link TagStore}.
 *
 * <p>Built in: {@code RoaringInvertedIndex} (in-process compressed bitmaps). Other adapters, such as
 * OpenSearch, Elasticsearch or a cloud search service, implement this interface and pass
 * {@code TagSearchIndexContractTest}.
 */
public interface TagSearchIndex extends TagEventListener {
    /**
     * Answers {@code q} for {@code tenantId}, or returns empty when this index can't right now (the tenant
     * isn't loaded yet). The caller then falls back to the store. An empty return may start a background load.
     */
    Optional<SearchPostings> search(String tenantId, TagQuery q);

    /** The highest event seq applied for the tenant, or -1 when the tenant isn't ready. */
    long watermark(String tenantId);
}
