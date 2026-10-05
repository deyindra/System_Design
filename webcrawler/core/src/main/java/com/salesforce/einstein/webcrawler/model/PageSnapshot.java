package com.salesforce.einstein.webcrawler.model;

import java.time.Instant;

/**
 * The latest global fetch of a URL, shared by all jobs and tenants ({@code url_records}).
 * It is what makes "already crawled" cheap: a job reuses it while it is fresh, or revalidates it
 * with {@code If-None-Match}/{@code If-Modified-Since}.
 */
public record PageSnapshot(String urlHash, String url, Instant fetchedAt, int httpStatus,
                           String contentType, String contentHash, long size,
                           String etag, String lastModified) {

    public PageSnapshot revalidated(Instant at) {
        return new PageSnapshot(urlHash, url, at, httpStatus, contentType, contentHash, size, etag, lastModified);
    }
}
