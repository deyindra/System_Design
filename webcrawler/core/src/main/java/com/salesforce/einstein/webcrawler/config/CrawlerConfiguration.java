package com.salesforce.einstein.webcrawler.config;

import com.salesforce.einstein.webcrawler.api.WebhookNotifier;
import com.salesforce.einstein.webcrawler.engine.CrawlEngine;
import com.salesforce.einstein.webcrawler.engine.EngineConfig;
import com.salesforce.einstein.webcrawler.fetch.CachingDnsResolver;
import com.salesforce.einstein.webcrawler.fetch.EgressFilteringFetcher;
import com.salesforce.einstein.webcrawler.fetch.EgressPolicy;
import com.salesforce.einstein.webcrawler.fetch.Fetcher;
import com.salesforce.einstein.webcrawler.fetch.HttpFetcher;
import com.salesforce.einstein.webcrawler.frontier.Frontier;
import com.salesforce.einstein.webcrawler.frontier.InMemoryFrontier;
import com.salesforce.einstein.webcrawler.render.CrawlResults;
import com.salesforce.einstein.webcrawler.sitemap.InMemorySitemapGraphs;
import com.salesforce.einstein.webcrawler.sitemap.SitemapGraphService;
import com.salesforce.einstein.webcrawler.sitemap.SitemapGraphs;
import com.salesforce.einstein.webcrawler.store.ContentStore;
import com.salesforce.einstein.webcrawler.store.FileSystemContentStore;
import com.salesforce.einstein.webcrawler.store.InMemoryContentStore;
import com.salesforce.einstein.webcrawler.store.InMemoryJobStore;
import com.salesforce.einstein.webcrawler.store.InMemoryPageStore;
import com.salesforce.einstein.webcrawler.store.JobStore;
import com.salesforce.einstein.webcrawler.store.PageStore;
import com.salesforce.einstein.webcrawler.url.ParamRules;
import com.salesforce.einstein.webcrawler.url.TrapDetector;
import com.salesforce.einstein.webcrawler.url.UrlNormalizer;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Clock;

/**
 * Wires the plain-Java engine to its ports ({@link Frontier}, {@link JobStore}, {@link PageStore}, {@link ContentStore},
 * {@link SitemapGraphs}, {@link Fetcher}). The engine depends only on those interfaces, never on a vendor SDK.
 *
 * <p>Each built-in adapter is selected by {@code crawler.adapters.*} and backs off if another bean is present. A
 * provider module (one per cloud, or self-hosted) ships its own auto-configuration that registers beans for its
 * value, e.g. {@code @ConditionalOnProperty(name = "crawler.adapters.content-store", havingValue = "object-store")}.
 * Switching providers means swapping that dependency and the config values; no engine, API or design change. An
 * unknown value fails at startup with a missing-bean error rather than silently falling back to memory.
 */
@Configuration
public class CrawlerConfiguration {

    @Bean @ConditionalOnMissingBean Clock clock() { return Clock.systemUTC(); }

    @Bean @ConditionalOnMissingBean UrlNormalizer urlNormalizer() { return new UrlNormalizer(new ParamRules()); }

    @Bean @ConditionalOnMissingBean TrapDetector trapDetector() { return TrapDetector.defaults(); }

    private static final String ADAPTERS = "crawler.adapters";

    @Bean @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = ADAPTERS, name = "frontier", havingValue = "memory", matchIfMissing = true)
    Frontier frontier(Clock clock) { return new InMemoryFrontier(clock); }

    @Bean @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = ADAPTERS, name = "job-store", havingValue = "memory", matchIfMissing = true)
    JobStore jobStore() { return new InMemoryJobStore(); }

    @Bean @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = ADAPTERS, name = "page-store", havingValue = "memory", matchIfMissing = true)
    PageStore pageStore() { return new InMemoryPageStore(); }

    @Bean @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = ADAPTERS, name = "content-store", havingValue = "memory", matchIfMissing = true)
    ContentStore contentStore() { return new InMemoryContentStore(); }

    @Bean @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = ADAPTERS, name = "content-store", havingValue = "filesystem")
    ContentStore fileSystemContentStore(CrawlerProperties p) {
        return new FileSystemContentStore(p.adapters().contentRoot());
    }

    @Bean(destroyMethod = "close") @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = ADAPTERS, name = "sitemap-graph", havingValue = "memory", matchIfMissing = true)
    SitemapGraphs sitemapGraphs(CrawlerProperties p) { return new InMemorySitemapGraphs(p.sitemap().shards()); }

    @Bean(destroyMethod = "close")
    SitemapGraphService sitemapGraphService(CrawlerProperties p, SitemapGraphs sitemaps, Fetcher fetcher,
                                            UrlNormalizer normalizer, Clock clock) {
        return new SitemapGraphService(sitemaps, fetcher, normalizer, p.maxSitemapBytes(), p.sitemap().loadThreads(), clock);
    }

    @Bean @ConditionalOnMissingBean EgressPolicy egressPolicy(CrawlerProperties p, Clock clock) {
        return new EgressPolicy(CachingDnsResolver.system(clock), p.egress().allowPrivate(), p.egress().ports());
    }

    /** The real fetcher always sits behind the egress check; tests replace the whole bean with a fake web. */
    @Bean @ConditionalOnMissingBean Fetcher fetcher(CrawlerProperties p, EgressPolicy egress) {
        return new EgressFilteringFetcher(new HttpFetcher(p.connectTimeout(), p.fetchTimeout()), egress);
    }

    @Bean(destroyMethod = "close")
    CrawlEngine crawlEngine(CrawlerProperties p, Frontier frontier, JobStore jobs, PageStore pages, ContentStore contents,
                            Fetcher fetcher, UrlNormalizer normalizer, TrapDetector traps, SitemapGraphs sitemaps,
                            Clock clock, ObjectMapper json, EgressPolicy egress) {
        EngineConfig cfg = new EngineConfig(p.workers(), p.politenessDelay(), p.maxRetries(), p.retryBackoff(),
                p.maxRedirects(), p.maxPageBytes(), p.agentToken(), p.maxPagesPerJob(), p.maxSitemapBytes());
        CrawlEngine engine = new CrawlEngine(cfg, frontier, jobs, pages, contents, fetcher, normalizer, traps, sitemaps,
                clock);
        engine.onFinished(new WebhookNotifier(json, jobs, egress));
        return engine;
    }

    @Bean CrawlResults crawlResults(JobStore jobs, ContentStore contents, UrlNormalizer normalizer) {
        return new CrawlResults(jobs, contents, normalizer);
    }
}
