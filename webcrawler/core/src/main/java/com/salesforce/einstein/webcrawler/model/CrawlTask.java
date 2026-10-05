package com.salesforce.einstein.webcrawler.model;

/**
 * One unit of work in the frontier: "fetch this URL for this job".
 *
 * @param depth        link hops from a seed; assets inherit their page's depth
 * @param parentHash   the page that discovered it ({@code null} for seeds)
 * @param expectedType guess before fetching; the response Content-Type is authoritative
 * @param redirectHops consecutive redirects that led here (bounded, so redirect loops terminate)
 * @param attempt      0 on first try; retries of timeouts and 5xx increment it
 */
public record CrawlTask(String jobId, CanonicalUrl url, int depth, String parentHash,
                        ResourceType expectedType, int redirectHops, int attempt) {

    public String host() { return url.host(); }

    public CrawlTask retry() { return new CrawlTask(jobId, url, depth, parentHash, expectedType, redirectHops, attempt + 1); }

    /** The same task reached by a shorter path. */
    public CrawlTask atDepth(int d) { return new CrawlTask(jobId, url, d, parentHash, expectedType, redirectHops, attempt); }

    /**
     * Identity of this delivery for exactly-once accounting: a redelivered copy has the same key, a retry or a
     * re-expansion at a smaller depth does not.
     */
    public String key() { return key(url.hash(), depth, attempt); }

    public static String key(String urlHash, int depth, int attempt) { return urlHash + "#" + depth + "#" + attempt; }
}
