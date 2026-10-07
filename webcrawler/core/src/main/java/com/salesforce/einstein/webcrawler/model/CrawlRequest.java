package com.salesforce.einstein.webcrawler.model;

import java.time.Duration;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * What the caller asked for. Every unbounded dimension of a graph crawl has an explicit cap here:
 * depth, pages, assets, asset size, and (for {@link CrawlMode#SYNC}) wall-clock time. The page cap's upper bound is
 * the service's ({@code crawler.max-pages-per-job}), checked when the job is submitted.
 *
 * <p>A <b>sitemap crawl</b> sets one of {@code sitemapUrl} or {@code sitemapGraph}: pages then come only from the
 * sitemap's graph, breadth-first from its roots, and the links in a page's HTML are recorded but not followed.
 *
 * @param seeds          where the crawl starts. With a sitemap, its roots: with {@code sitemapUrl} optional (the
 *                       sitemap's own roots by default), with {@code sitemapGraph} required
 * @param sitemapUrl     a navigation sitemap to fetch and crawl, loaded for this job only
 * @param sitemapGraph   a sitemap graph already in the graph store, by name
 * @param downloadAssets asset types whose bytes are stored; any other asset is {@link PageStatus#REFERENCED}
 * @param maxAge         reuse a global snapshot younger than this instead of fetching ({@link Duration#ZERO} = always revalidate)
 */
public record CrawlRequest(String tenantId, List<String> seeds, String sitemapUrl, String sitemapGraph, int maxDepth, int maxPages, int maxAssets,
                           Scope scope, Set<ResourceType> downloadAssets, long maxAssetBytes,
                           Duration maxAge, boolean respectRobots, CrawlMode mode, String callbackUrl) {

    public static final int MAX_DEPTH = 50;
    public static final int MAX_SEEDS = 100;
    public static final int MAX_URL_LENGTH = 2048;
    /** A graph-store identifier, valid in every adapter. */
    public static final Pattern SITEMAP_GRAPH = Pattern.compile("[A-Za-z_][A-Za-z0-9_]{2,62}");
    /** Begins the name of the graph loaded for a {@code sitemapUrl} job; no request may name one. */
    public static final String JOB_GRAPH_PREFIX = "sitemap_";
    /** A SYNC request must be small enough to finish inside one HTTP request. */
    public static final int SYNC_MAX_DEPTH = 1;
    public static final int SYNC_MAX_PAGES = 25;

    public CrawlRequest {
        seeds = List.copyOf(seeds);
        downloadAssets = downloadAssets.isEmpty() ? EnumSet.noneOf(ResourceType.class) : EnumSet.copyOf(downloadAssets);
        if (downloadAssets.contains(ResourceType.PAGE)) throw new IllegalArgumentException("PAGE is not an asset type");
    }

    /** Throws {@link IllegalArgumentException} with a caller-facing message. */
    public void validate() {
        if (tenantId == null || tenantId.isBlank()) throw new IllegalArgumentException("tenantId is required");
        if (sitemapUrl != null && sitemapGraph != null)
            throw new IllegalArgumentException("set at most one of sitemapUrl and sitemapGraph");
        if (sitemapUrl != null && (sitemapUrl.length() > MAX_URL_LENGTH || !sitemapUrl.matches("(?i)https?://.+")))
            throw new IllegalArgumentException("sitemapUrl: an http(s) URL of at most " + MAX_URL_LENGTH + " characters");
        if (sitemapGraph != null && !SITEMAP_GRAPH.matcher(sitemapGraph).matches())
            throw new IllegalArgumentException("sitemapGraph must match " + SITEMAP_GRAPH);
        if (sitemapGraph != null && sitemapGraph.startsWith(JOB_GRAPH_PREFIX))
            throw new IllegalArgumentException("sitemapGraph names beginning " + JOB_GRAPH_PREFIX + " are reserved");
        int minSeeds = sitemapUrl != null ? 0 : 1;
        if (seeds.size() < minSeeds || seeds.size() > MAX_SEEDS)
            throw new IllegalArgumentException("seeds: " + minSeeds + ".." + MAX_SEEDS + " URLs"
                    + (sitemapGraph != null ? " (the roots of the sitemapGraph slice)" : ""));
        if (maxDepth < 0 || maxDepth > MAX_DEPTH) throw new IllegalArgumentException("maxDepth: 0.." + MAX_DEPTH);
        if (maxPages < 1) throw new IllegalArgumentException("maxPages must be >= 1");
        if (maxAssets < 0) throw new IllegalArgumentException("maxAssets must be >= 0");
        if (maxAssetBytes < 0) throw new IllegalArgumentException("maxAssetBytes must be >= 0");
        if (maxAge.isNegative()) throw new IllegalArgumentException("maxAge must be >= 0");
        if (mode == CrawlMode.SYNC && (maxDepth > SYNC_MAX_DEPTH || maxPages > SYNC_MAX_PAGES))
            throw new IllegalArgumentException("SYNC crawls are limited to maxDepth<=" + SYNC_MAX_DEPTH
                    + " and maxPages<=" + SYNC_MAX_PAGES + "; use mode=ASYNC");
    }

    /** Whether this is a sitemap crawl. */
    public boolean isSitemap() { return sitemapUrl != null || sitemapGraph != null; }

    public static Builder builder(String tenantId, String... seeds) { return new Builder(tenantId, List.of(seeds)); }

    public static final class Builder {
        private final String tenantId;
        private final List<String> seeds;
        private String sitemapUrl, sitemapGraph;
        private int maxDepth = 2, maxPages = 1000, maxAssets = 5000;
        private Scope scope = Scope.SAME_HOST;
        private Set<ResourceType> downloadAssets = EnumSet.of(ResourceType.IMAGE, ResourceType.CSS);
        private long maxAssetBytes = 10L << 20;
        private Duration maxAge = Duration.ofHours(24);
        private boolean respectRobots = true;
        private CrawlMode mode = CrawlMode.ASYNC;
        private String callbackUrl;

        private Builder(String tenantId, List<String> seeds) { this.tenantId = tenantId; this.seeds = seeds; }

        public Builder sitemapUrl(String v) { sitemapUrl = v; return this; }
        public Builder sitemapGraph(String v) { sitemapGraph = v; return this; }
        public Builder maxDepth(int v) { maxDepth = v; return this; }
        public Builder maxPages(int v) { maxPages = v; return this; }
        public Builder maxAssets(int v) { maxAssets = v; return this; }
        public Builder scope(Scope v) { scope = v; return this; }
        public Builder downloadAssets(Set<ResourceType> v) { downloadAssets = v; return this; }
        public Builder maxAssetBytes(long v) { maxAssetBytes = v; return this; }
        public Builder maxAge(Duration v) { maxAge = v; return this; }
        public Builder respectRobots(boolean v) { respectRobots = v; return this; }
        public Builder mode(CrawlMode v) { mode = v; return this; }
        public Builder callbackUrl(String v) { callbackUrl = v; return this; }

        public CrawlRequest build() {
            return new CrawlRequest(tenantId, seeds, sitemapUrl, sitemapGraph, maxDepth, maxPages, maxAssets, scope, downloadAssets,
                    maxAssetBytes, maxAge, respectRobots, mode, callbackUrl);
        }
    }
}
