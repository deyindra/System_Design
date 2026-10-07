package com.salesforce.einstein.webcrawler.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Set;

/**
 * {@code crawler.*} in application.yml.
 *
 * @param maxSyncWait     upper bound on how long a SYNC request may hold the HTTP connection
 * @param maxPagesPerJob  the largest {@code maxPages} a request may ask for. Above the default, use a job store
 *                        that keeps a job's graph out of memory ({@code redis-cql})
 * @param maxSitemapBytes a {@code sitemapUrl} file larger than this is refused (50 MiB: the sitemaps.org limit)
 */
@ConfigurationProperties("crawler")
public record CrawlerProperties(
        @DefaultValue("16") int workers,
        @DefaultValue("1s") Duration politenessDelay,
        @DefaultValue("3") int maxRetries,
        @DefaultValue("2s") Duration retryBackoff,
        @DefaultValue("5") int maxRedirects,
        @DefaultValue("5242880") long maxPageBytes,
        @DefaultValue("EinsteinCrawler") String agentToken,
        @DefaultValue("5s") Duration connectTimeout,
        @DefaultValue("30s") Duration fetchTimeout,
        @DefaultValue("30s") Duration maxSyncWait,
        @DefaultValue("1000000") int maxPagesPerJob,
        @DefaultValue("52428800") long maxSitemapBytes,
        @DefaultValue Adapters adapters,
        @DefaultValue Egress egress,
        @DefaultValue Sitemap sitemap) {

    /**
     * Which implementation backs each port. The engine never sees these names. An adapter module (for one cloud,
     * or a self-hosted stack) registers its beans under its own value, e.g. {@code content-store: object-store},
     * so moving between providers is a dependency and a config change, not a code change.
     *
     * @param contentStore {@code memory} or {@code filesystem}
     * @param contentRoot  root directory for {@code content-store: filesystem}
     * @param sitemapGraph where sitemap graphs live: {@code memory} (one node), or a graph store module's value
     *                     ({@code age}, {@code neo4j})
     */
    public record Adapters(
            @DefaultValue("memory") String frontier,
            @DefaultValue("memory") String jobStore,
            @DefaultValue("memory") String pageStore,
            @DefaultValue("memory") String contentStore,
            @DefaultValue("./data/blobs") Path contentRoot,
            @DefaultValue("memory") String sitemapGraph) { }

    /**
     * SSRF guard for fetches and webhooks; see {@code EgressPolicy}.
     *
     * @param allowPrivate {@code true} only for local development against services on a private network
     * @param ports        allowed destination ports; empty allows any
     */
    public record Egress(
            @DefaultValue("false") boolean allowPrivate,
            @DefaultValue({"80", "443", "8080", "8443"}) Set<Integer> ports) { }

    /**
     * Sitemap graphs. The store modules read their connection settings under {@code crawler.sitemap.<value>}.
     *
     * @param shards how many shards (by host) a graph loaded from a {@code sitemapUrl} has, and a bulk import should
     *               use. A crawl reads successors only, so a graph with another count still crawls
     * @param loadThreads how many {@code PUT /v1/sitemap-graphs} loads run at once on a node; more wait their turn
     */
    public record Sitemap(@DefaultValue("16") int shards, @DefaultValue("2") int loadThreads) { }
}
