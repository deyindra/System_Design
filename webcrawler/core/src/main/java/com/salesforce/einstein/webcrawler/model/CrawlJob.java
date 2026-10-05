package com.salesforce.einstein.webcrawler.model;

import java.time.Instant;

/** A crawl job ({@code crawl_jobs}). Immutable; the store swaps versions. */
public record CrawlJob(String jobId, String tenantId, String idempotencyKey, CrawlRequest request,
                       JobStatus status, Instant createdAt, Instant finishedAt, String error) {

    public CrawlJob withStatus(JobStatus s, Instant at, String err) {
        return new CrawlJob(jobId, tenantId, idempotencyKey, request, s, createdAt,
                s.isTerminal() ? at : finishedAt, err);
    }
}
