package com.salesforce.einstein.webcrawler.sitemap;

import com.salesforce.einstein.webcrawler.fetch.Fetcher;
import com.salesforce.einstein.webcrawler.model.CanonicalUrl;
import com.salesforce.einstein.webcrawler.model.CrawlRequest;
import com.salesforce.einstein.webcrawler.url.UrlNormalizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Named sitemap graphs loaded on request, for crawls that set {@code sitemapGraph}: {@link #load} records the graph
 * as {@link SitemapGraphRecord.Status#LOADING} in the catalog and returns; the sitemap is fetched, parsed and written in
 * the background, and the entry ends {@code READY} with the sitemap's roots, or {@code FAILED} with the reason.
 *
 * <p>Every node can answer for every graph, because the catalog lives in the graph store. A graph belongs to the
 * tenant that loaded it: to any other tenant it does not exist. A node that dies mid-load leaves its entry
 * {@code LOADING}; {@link #delete} clears it, and the graph can be loaded again.
 */
public final class SitemapGraphService implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(SitemapGraphService.class);

    private final SitemapGraphs graphs;
    private final Fetcher fetcher;
    private final UrlNormalizer normalizer;
    private final long maxSitemapBytes;
    private final Clock clock;
    private final ExecutorService loads;

    /**
     * @param fetcher the crawler's fetcher, behind its egress check
     * @param threads how many loads run at once; more wait their turn
     */
    public SitemapGraphService(SitemapGraphs graphs, Fetcher fetcher, UrlNormalizer normalizer, long maxSitemapBytes,
                               int threads, Clock clock) {
        if (threads < 1) throw new IllegalArgumentException("threads must be >= 1");
        this.graphs = graphs;
        this.fetcher = fetcher;
        this.normalizer = normalizer;
        this.maxSitemapBytes = maxSitemapBytes;
        this.clock = clock;
        AtomicInteger n = new AtomicInteger();
        this.loads = Executors.newFixedThreadPool(threads, r -> {
            Thread t = new Thread(r, "sitemap-load-" + n.getAndIncrement());
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * Starts loading {@code sitemapUrl} as graph {@code name}, or returns the entry of the same load (same tenant,
     * same URL) if there is one, so a retried request is harmless.
     *
     * @throws IllegalArgumentException      for an invalid name or URL
     * @throws SitemapGraphConflictException if the name is taken
     */
    public SitemapGraphRecord load(String tenantId, String name, String sitemapUrl) {
        checkName(name);
        if (sitemapUrl == null || sitemapUrl.length() > CrawlRequest.MAX_URL_LENGTH || !sitemapUrl.matches("(?i)https?://.+"))
            throw new IllegalArgumentException("sitemapUrl: an http(s) URL of at most " + CrawlRequest.MAX_URL_LENGTH + " characters");
        while (true) {
            Optional<SitemapGraphRecord> existing = graphs.record(name);
            if (existing.isPresent()) {
                SitemapGraphRecord e = existing.get();
                if (e.tenantId().equals(tenantId) && e.sitemapUrl().equals(sitemapUrl)) return e;
                throw new SitemapGraphConflictException("sitemap graph name in use: " + name);
            }
            if (graphs.exists(name)) throw new SitemapGraphConflictException("sitemap graph name in use: " + name);
            SitemapGraphRecord entry = SitemapGraphRecord.loading(name, UUID.randomUUID().toString(), tenantId,
                    sitemapUrl, clock.instant());
            if (graphs.register(entry)) {
                loads.execute(() -> run(entry));
                return entry;
            }
            // lost a race for the name: look again
        }
    }

    /** The tenant's graph {@code name}; another tenant's reads as absent. */
    public Optional<SitemapGraphRecord> get(String tenantId, String name) {
        return graphs.record(name).filter(e -> e.tenantId().equals(tenantId));
    }

    /**
     * Deletes the tenant's graph {@code name}, in any state, with its entry. A load still running notices when it
     * ends, and drops what it wrote. Crawls of the graph still running stop finding successors.
     *
     * @return false if the tenant has no such graph
     */
    public boolean delete(String tenantId, String name) {
        if (get(tenantId, name).isEmpty()) return false;
        graphs.drop(name);
        graphs.unregister(name);
        return true;
    }

    private void run(SitemapGraphRecord entry) {
        String name = entry.name();
        boolean wrote = false;
        SitemapGraphRecord outcome;
        try {
            NavigationSitemap sitemap = Sitemaps.fetch(fetcher, normalizer, entry.sitemapUrl(), maxSitemapBytes);
            if (sitemap.graph().nodeCount() == 0) throw new IllegalArgumentException("sitemap has no pages");
            if (graphs.exists(name)) throw new IllegalArgumentException("sitemap graph name in use: " + name);
            wrote = true;
            graphs.load(name, sitemap.graph());
            List<String> roots = (sitemap.roots().isEmpty() ? sitemap.defaultRoots() : sitemap.roots()).stream()
                    .map(CanonicalUrl::value).toList();
            outcome = entry.ready(sitemap.graph().nodeCount(), sitemap.graph().edgeCount(), roots, clock.instant());
        } catch (IllegalArgumentException e) {                              // what was wrong with the sitemap
            outcome = entry.failed(e.getMessage(), clock.instant());
        } catch (RuntimeException e) {
            log.warn("could not load sitemap graph {}", name, e);
            outcome = entry.failed("internal error", clock.instant());
        }
        boolean current = graphs.update(outcome);                           // false: deleted while it loaded
        if (wrote && (!current || outcome.status() == SitemapGraphRecord.Status.FAILED)) dropQuietly(name);
    }

    private void dropQuietly(String name) {
        try {
            graphs.drop(name);
        } catch (RuntimeException e) {
            log.warn("could not drop sitemap graph {}", name, e);
        }
    }

    private static void checkName(String name) {
        if (name == null || !CrawlRequest.SITEMAP_GRAPH.matcher(name).matches())
            throw new IllegalArgumentException("sitemap graph name must match " + CrawlRequest.SITEMAP_GRAPH);
        if (name.startsWith(CrawlRequest.JOB_GRAPH_PREFIX))
            throw new IllegalArgumentException("sitemap graph names beginning " + CrawlRequest.JOB_GRAPH_PREFIX + " are reserved");
    }

    /** Stops the loads; one cut short ends {@code FAILED}, or stays {@code LOADING} if the store went first. */
    @Override
    public void close() {
        loads.shutdownNow();
        try {
            if (!loads.awaitTermination(5, TimeUnit.SECONDS)) log.warn("sitemap loads still running at shutdown");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
