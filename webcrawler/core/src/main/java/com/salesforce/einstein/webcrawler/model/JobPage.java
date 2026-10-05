package com.salesforce.einstein.webcrawler.model;

import java.time.Instant;

/**
 * One node of one job's crawl graph ({@code job_pages}). It points at immutable, content-addressed bytes
 * via {@code contentHash}, so a job's result never changes when the URL is re-crawled later by another job.
 */
public record JobPage(String jobId, String urlHash, String url, ResourceType type, int depth,
                      String parentHash, PageStatus status, int httpStatus, String contentType,
                      String contentHash, long size, Instant fetchedAt,
                      String duplicateOf, String redirectTo, String error) {

    public static JobPage queued(CrawlTask t) {
        return new JobPage(t.jobId(), t.url().hash(), t.url().value(), t.expectedType(), t.depth(),
                t.parentHash(), PageStatus.QUEUED, 0, null, null, 0, null, null, null, null);
    }

    public static JobPage referenced(CrawlTask t) {
        return new JobPage(t.jobId(), t.url().hash(), t.url().value(), t.expectedType(), t.depth(),
                t.parentHash(), PageStatus.REFERENCED, 0, null, null, 0, null, null, null, null);
    }

    /** Reached by a shorter path: its depth (and the parent on that path) change, nothing else does. */
    public JobPage atDepth(int d, String parent) {
        return new JobPage(jobId, urlHash, url, type, d, parent, status, httpStatus, contentType,
                contentHash, size, fetchedAt, duplicateOf, redirectTo, error);
    }

    public JobPage withContent(PageStatus st, ResourceType ty, PageSnapshot s) {
        return new JobPage(jobId, urlHash, url, ty, depth, parentHash, st, s.httpStatus(), s.contentType(),
                s.contentHash(), s.size(), s.fetchedAt(), null, null, null);
    }

    public JobPage duplicateOf(String otherUrlHash) {
        return new JobPage(jobId, urlHash, url, type, depth, parentHash, PageStatus.DUPLICATE, httpStatus,
                contentType, contentHash, size, fetchedAt, otherUrlHash, null, null);
    }

    public JobPage redirect(int http, String targetHash, Instant at) {
        return new JobPage(jobId, urlHash, url, type, depth, parentHash, PageStatus.REDIRECT, http, null,
                null, 0, at, null, targetHash, null);
    }

    public JobPage failed(PageStatus st, int http, String err, Instant at) {
        return new JobPage(jobId, urlHash, url, type, depth, parentHash, st, http, null,
                null, 0, at, null, null, err);
    }
}
