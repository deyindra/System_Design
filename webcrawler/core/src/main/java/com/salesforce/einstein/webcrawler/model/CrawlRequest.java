package com.salesforce.einstein.webcrawler.model;

import java.time.Duration;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

/**
 * What the caller asked for. Every unbounded dimension of a graph crawl has an explicit cap here:
 * depth, pages, assets, asset size, and (for {@link CrawlMode#SYNC}) wall-clock time.
 *
 * @param downloadAssets asset types whose bytes are stored; any other asset is {@link PageStatus#REFERENCED}
 * @param maxAge         reuse a global snapshot younger than this instead of fetching ({@link Duration#ZERO} = always revalidate)
 */
public record CrawlRequest(String tenantId, List<String> seeds, int maxDepth, int maxPages, int maxAssets,
                           Scope scope, Set<ResourceType> downloadAssets, long maxAssetBytes,
                           Duration maxAge, boolean respectRobots, CrawlMode mode, String callbackUrl) {

    public static final int MAX_DEPTH = 50;
    public static final int MAX_PAGES = 1_000_000;
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
        if (seeds.isEmpty() || seeds.size() > 100) throw new IllegalArgumentException("seeds: 1..100 URLs");
        if (maxDepth < 0 || maxDepth > MAX_DEPTH) throw new IllegalArgumentException("maxDepth: 0.." + MAX_DEPTH);
        if (maxPages < 1 || maxPages > MAX_PAGES) throw new IllegalArgumentException("maxPages: 1.." + MAX_PAGES);
        if (maxAssets < 0) throw new IllegalArgumentException("maxAssets must be >= 0");
        if (maxAssetBytes < 0) throw new IllegalArgumentException("maxAssetBytes must be >= 0");
        if (maxAge.isNegative()) throw new IllegalArgumentException("maxAge must be >= 0");
        if (mode == CrawlMode.SYNC && (maxDepth > SYNC_MAX_DEPTH || maxPages > SYNC_MAX_PAGES))
            throw new IllegalArgumentException("SYNC crawls are limited to maxDepth<=" + SYNC_MAX_DEPTH
                    + " and maxPages<=" + SYNC_MAX_PAGES + "; use mode=ASYNC");
    }

    public static Builder builder(String tenantId, String... seeds) { return new Builder(tenantId, List.of(seeds)); }

    public static final class Builder {
        private final String tenantId;
        private final List<String> seeds;
        private int maxDepth = 2, maxPages = 1000, maxAssets = 5000;
        private Scope scope = Scope.SAME_HOST;
        private Set<ResourceType> downloadAssets = EnumSet.of(ResourceType.IMAGE, ResourceType.CSS);
        private long maxAssetBytes = 10L << 20;
        private Duration maxAge = Duration.ofHours(24);
        private boolean respectRobots = true;
        private CrawlMode mode = CrawlMode.ASYNC;
        private String callbackUrl;

        private Builder(String tenantId, List<String> seeds) { this.tenantId = tenantId; this.seeds = seeds; }

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
            return new CrawlRequest(tenantId, seeds, maxDepth, maxPages, maxAssets, scope, downloadAssets,
                    maxAssetBytes, maxAge, respectRobots, mode, callbackUrl);
        }
    }
}
