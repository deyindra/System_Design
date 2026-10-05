package com.salesforce.einstein.webcrawler.fetch;

/** Downloads one URL. Implementations must be thread-safe and must not follow redirects. */
public interface Fetcher {
    FetchResult fetch(FetchRequest request);
}
