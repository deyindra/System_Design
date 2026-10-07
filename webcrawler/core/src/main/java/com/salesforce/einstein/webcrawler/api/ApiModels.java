package com.salesforce.einstein.webcrawler.api;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.salesforce.einstein.webcrawler.model.CrawlJob;
import com.salesforce.einstein.webcrawler.model.CrawlMode;
import com.salesforce.einstein.webcrawler.model.CrawlRequest;
import com.salesforce.einstein.webcrawler.model.JobPage;
import com.salesforce.einstein.webcrawler.model.JobStats;
import com.salesforce.einstein.webcrawler.model.LinkEdge;
import com.salesforce.einstein.webcrawler.model.ResourceType;
import com.salesforce.einstein.webcrawler.model.Scope;
import com.salesforce.einstein.webcrawler.sitemap.SitemapGraphRecord;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;

import java.time.Duration;
import java.time.Instant;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Wire format of the REST API. Timestamps are ISO-8601 strings. Absent request fields take the defaults below. */
public final class ApiModels {
    private ApiModels() { }

    /** {@code POST /v1/crawls}. {@code seeds} may be left out only with {@code sitemapUrl}. */
    public record CreateCrawl(
            @Size(max = CrawlRequest.MAX_SEEDS) List<@NotBlank @Size(max = CrawlRequest.MAX_URL_LENGTH) String> seeds,
            @Size(max = CrawlRequest.MAX_URL_LENGTH) @Pattern(regexp = "(?i)https?://.+") String sitemapUrl,
            @Pattern(regexp = "[A-Za-z_][A-Za-z0-9_]{2,62}") String sitemapGraph,
            @Min(0) @Max(CrawlRequest.MAX_DEPTH) Integer maxDepth,
            @Min(1) Integer maxPages,
            @Min(0) Integer maxAssets,
            Scope scope,
            Set<ResourceType> downloadAssets,
            @Min(0) Long maxAssetBytes,
            @Min(0) Long maxAgeSeconds,
            Boolean respectRobots,
            CrawlMode mode,
            @Pattern(regexp = "https://.+") String callbackUrl,
            @Min(100) @Max(30_000) Long syncTimeoutMs) {

        public CrawlRequest toDomain(String tenantId) {
            CrawlRequest.Builder b = CrawlRequest.builder(tenantId, seeds == null ? new String[0] : seeds.toArray(String[]::new));
            if (sitemapUrl != null) b.sitemapUrl(sitemapUrl);
            if (sitemapGraph != null) b.sitemapGraph(sitemapGraph);
            if (maxDepth != null) b.maxDepth(maxDepth);
            if (maxPages != null) b.maxPages(maxPages);
            if (maxAssets != null) b.maxAssets(maxAssets);
            if (scope != null) b.scope(scope);
            if (downloadAssets != null) b.downloadAssets(downloadAssets.isEmpty() ? EnumSet.noneOf(ResourceType.class) : downloadAssets);
            if (maxAssetBytes != null) b.maxAssetBytes(maxAssetBytes);
            if (maxAgeSeconds != null) b.maxAge(Duration.ofSeconds(maxAgeSeconds));
            if (respectRobots != null) b.respectRobots(respectRobots);
            if (mode != null) b.mode(mode);
            if (callbackUrl != null) b.callbackUrl(callbackUrl);
            return b.build();
        }
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record RequestView(List<String> seeds, String sitemapUrl, String sitemapGraph, int maxDepth, int maxPages, int maxAssets, Scope scope,
                              Set<ResourceType> downloadAssets, long maxAssetBytes, long maxAgeSeconds,
                              boolean respectRobots, CrawlMode mode, String callbackUrl) {
        static RequestView of(CrawlRequest r) {
            return new RequestView(r.seeds(), r.sitemapUrl(), r.sitemapGraph(), r.maxDepth(), r.maxPages(), r.maxAssets(), r.scope(), r.downloadAssets(),
                    r.maxAssetBytes(), r.maxAge().toSeconds(), r.respectRobots(), r.mode(), r.callbackUrl());
        }
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record JobView(String jobId, String status, String createdAt, String finishedAt, String error,
                          RequestView request, JobStats stats, Map<String, String> links) {
        public static JobView of(CrawlJob j, JobStats s) {
            String self = "/v1/crawls/" + j.jobId();
            return new JobView(j.jobId(), j.status().name(), ts(j.createdAt()), ts(j.finishedAt()), j.error(),
                    RequestView.of(j.request()), s,
                    Map.of("self", self, "pages", self + "/pages", "export", self + "/export", "cancel", self + "/cancel"));
        }
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record PageView(String urlHash, String url, ResourceType type, int depth, String parentHash, String status,
                           Integer httpStatus, String contentType, String contentHash, Long size, String fetchedAt,
                           String duplicateOf, String redirectTo, String error, String contentUrl) {
        public static PageView of(JobPage p) {
            String content = "/v1/crawls/" + p.jobId() + "/pages/" + p.urlHash() + "/content";
            boolean servable = p.status().hasContent() || p.duplicateOf() != null || p.redirectTo() != null;
            return new PageView(p.urlHash(), p.url(), p.type(), p.depth(), p.parentHash(), p.status().name(),
                    p.httpStatus() == 0 ? null : p.httpStatus(), p.contentType(), p.contentHash(),
                    p.size() == 0 ? null : p.size(), ts(p.fetchedAt()), p.duplicateOf(), p.redirectTo(), p.error(),
                    servable ? content : null);
        }
    }

    /** {@code inCrawl}: the target is a node of this job's graph (the edge may still point outside it). */
    public record LinkView(String toUrl, String toHash, ResourceType type, String rawHref, boolean inCrawl) {
        public static LinkView of(LinkEdge e, boolean inCrawl) {
            return new LinkView(e.toUrl(), e.toHash(), e.type(), e.rawHref(), inCrawl);
        }
    }

    public record PageDetail(PageView page, List<LinkView> outLinks) { }

    public record PageList(List<PageView> items, String nextCursor) { }

    /** SYNC result when the crawl finished inside the deadline: the job and every node of its graph. */
    public record SyncResult(JobView job, List<PageView> pages) { }

    /** {@code PUT /v1/sitemap-graphs/{name}}. */
    public record LoadSitemapGraph(
            @NotBlank @Size(max = CrawlRequest.MAX_URL_LENGTH) @Pattern(regexp = "(?i)https?://.+") String sitemapUrl) { }

    /** A sitemap graph and its load. {@code roots} (at most 100 of {@code rootCount}) are seeds for a whole-graph crawl. */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record SitemapGraphView(String name, String status, String sitemapUrl, String createdAt, String finishedAt,
                                   String error, Long pages, Long edges, List<String> roots, Integer rootCount,
                                   Map<String, String> links) {
        public static SitemapGraphView of(SitemapGraphRecord g) {
            boolean ready = g.status() == SitemapGraphRecord.Status.READY;
            return new SitemapGraphView(g.name(), g.status().name(), g.sitemapUrl(), ts(g.createdAt()), ts(g.finishedAt()),
                    g.error(), ready ? g.pages() : null, ready ? g.edges() : null, ready ? g.roots() : null,
                    ready ? g.rootCount() : null, Map.of("self", "/v1/sitemap-graphs/" + g.name()));
        }
    }

    /** The tenant's latest copy of a URL; {@code content} is where to read it. */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record SnapshotView(String urlHash, String url, String jobId, String status, String fetchedAt,
                               int httpStatus, String contentType, String contentHash, long size, String content) {
        public static SnapshotView of(JobPage p) {
            return new SnapshotView(p.urlHash(), p.url(), p.jobId(), p.status().name(), ts(p.fetchedAt()),
                    p.httpStatus(), p.contentType(), p.contentHash(), p.size(),
                    "/v1/crawls/" + p.jobId() + "/pages/" + p.urlHash() + "/content");
        }
    }

    private static String ts(Instant i) { return i == null ? null : i.toString(); }
}
