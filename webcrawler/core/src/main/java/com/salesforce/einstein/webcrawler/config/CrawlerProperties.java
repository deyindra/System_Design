package com.salesforce.einstein.webcrawler.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Set;

/**
 * {@code crawler.*} in application.yml.
 *
 * @param maxSyncWait upper bound on how long a SYNC request may hold the HTTP connection
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
        @DefaultValue Adapters adapters,
        @DefaultValue Egress egress) {

    /**
     * Which implementation backs each port. The engine never sees these names. An adapter module (for one cloud,
     * or a self-hosted stack) registers its beans under its own value, e.g. {@code content-store: object-store},
     * so moving between providers is a dependency and a config change, not a code change.
     *
     * @param contentStore {@code memory} or {@code filesystem}
     * @param contentRoot  root directory for {@code content-store: filesystem}
     */
    public record Adapters(
            @DefaultValue("memory") String frontier,
            @DefaultValue("memory") String jobStore,
            @DefaultValue("memory") String pageStore,
            @DefaultValue("memory") String contentStore,
            @DefaultValue("./data/blobs") Path contentRoot) { }

    /**
     * SSRF guard for fetches and webhooks; see {@code EgressPolicy}.
     *
     * @param allowPrivate {@code true} only for local development against services on a private network
     * @param ports        allowed destination ports; empty allows any
     */
    public record Egress(
            @DefaultValue("false") boolean allowPrivate,
            @DefaultValue({"80", "443", "8080", "8443"}) Set<Integer> ports) { }
}
