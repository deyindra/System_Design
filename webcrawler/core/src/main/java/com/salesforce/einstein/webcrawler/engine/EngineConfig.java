package com.salesforce.einstein.webcrawler.engine;

import java.time.Duration;

/**
 * @param politenessDelay minimum gap between two requests to one host (robots {@code Crawl-delay} can raise it)
 * @param retryBackoff    first retry delay; doubles per attempt
 * @param maxRedirects    a chain longer than this is a loop or a trap
 * @param maxPageBytes    HTML larger than this is not stored or parsed
 * @param maxPagesPerJob  the largest {@code maxPages} a request may ask for
 * @param maxSitemapBytes a {@code sitemapUrl} file larger than this is refused
 */
public record EngineConfig(int workers, Duration politenessDelay, int maxRetries, Duration retryBackoff,
                           int maxRedirects, long maxPageBytes, String agentToken, int maxPagesPerJob,
                           long maxSitemapBytes) {

    public static EngineConfig defaults() {
        return new EngineConfig(16, Duration.ofSeconds(1), 3, Duration.ofSeconds(2), 5, 5L << 20, "EinsteinCrawler",
                1_000_000, 50L << 20);
    }

    public EngineConfig withWorkers(int n) {
        return new EngineConfig(n, politenessDelay, maxRetries, retryBackoff, maxRedirects, maxPageBytes, agentToken,
                maxPagesPerJob, maxSitemapBytes);
    }

    public EngineConfig withPoliteness(Duration d) {
        return new EngineConfig(workers, d, maxRetries, retryBackoff, maxRedirects, maxPageBytes, agentToken,
                maxPagesPerJob, maxSitemapBytes);
    }

    public EngineConfig withRetryBackoff(Duration d) {
        return new EngineConfig(workers, politenessDelay, maxRetries, d, maxRedirects, maxPageBytes, agentToken,
                maxPagesPerJob, maxSitemapBytes);
    }

    public EngineConfig withMaxPagesPerJob(int n) {
        return new EngineConfig(workers, politenessDelay, maxRetries, retryBackoff, maxRedirects, maxPageBytes,
                agentToken, n, maxSitemapBytes);
    }
}
