package com.salesforce.einstein.webcrawler.sitemap;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Objects;

/**
 * The catalog entry of a sitemap graph loaded through the API: who owns it, where it came from, and how far its load
 * got. A bulk-imported graph has none.
 *
 * @param loadId    tells this load from a later one of the same name, so a load that was deleted can't update its
 *                  successor's entry
 * @param roots     the sitemap's roots, at most {@link #MAX_ROOTS}, to use as a crawl's {@code seeds}
 * @param rootCount how many roots the sitemap has
 * @param error     why a {@link Status#FAILED} load failed, for the caller
 */
public record SitemapGraphRecord(String name, String loadId, String tenantId, String sitemapUrl, Status status,
                                 long pages, long edges, List<String> roots, int rootCount, String error,
                                 Instant createdAt, Instant finishedAt) {

    /** As many roots as a crawl takes seeds. */
    public static final int MAX_ROOTS = 100;

    public enum Status { LOADING, READY, FAILED }

    public SitemapGraphRecord {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(loadId, "loadId");
        Objects.requireNonNull(tenantId, "tenantId");
        Objects.requireNonNull(sitemapUrl, "sitemapUrl");
        Objects.requireNonNull(status, "status");
        Objects.requireNonNull(createdAt, "createdAt");
        roots = List.copyOf(roots);
        if (roots.size() > MAX_ROOTS) throw new IllegalArgumentException("at most " + MAX_ROOTS + " roots");
        createdAt = createdAt.truncatedTo(ChronoUnit.MILLIS);                // what every store keeps
        finishedAt = finishedAt == null ? null : finishedAt.truncatedTo(ChronoUnit.MILLIS);
    }

    /** A load that has just started. */
    public static SitemapGraphRecord loading(String name, String loadId, String tenantId, String sitemapUrl, Instant now) {
        return new SitemapGraphRecord(name, loadId, tenantId, sitemapUrl, Status.LOADING, 0, 0, List.of(), 0, null, now, null);
    }

    public SitemapGraphRecord ready(long pages, long edges, List<String> allRoots, Instant now) {
        return new SitemapGraphRecord(name, loadId, tenantId, sitemapUrl, Status.READY, pages, edges,
                allRoots.subList(0, Math.min(MAX_ROOTS, allRoots.size())), allRoots.size(), null, createdAt, now);
    }

    public SitemapGraphRecord failed(String error, Instant now) {
        return new SitemapGraphRecord(name, loadId, tenantId, sitemapUrl, Status.FAILED, 0, 0, List.of(), 0, error,
                createdAt, now);
    }
}
