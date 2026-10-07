package com.salesforce.einstein.webcrawler.engine;

import com.salesforce.einstein.webcrawler.fetch.FetchRequest;
import com.salesforce.einstein.webcrawler.fetch.FetchResult;
import com.salesforce.einstein.webcrawler.fetch.Fetcher;
import com.salesforce.einstein.webcrawler.fetch.RobotsCache;
import com.salesforce.einstein.webcrawler.fetch.RobotsRules;
import com.salesforce.einstein.webcrawler.frontier.Frontier;
import com.salesforce.einstein.webcrawler.model.CanonicalUrl;
import com.salesforce.einstein.webcrawler.model.CrawlJob;
import com.salesforce.einstein.webcrawler.model.CrawlRequest;
import com.salesforce.einstein.webcrawler.model.CrawlTask;
import com.salesforce.einstein.webcrawler.model.JobPage;
import com.salesforce.einstein.webcrawler.model.JobStatus;
import com.salesforce.einstein.webcrawler.model.LinkEdge;
import com.salesforce.einstein.webcrawler.model.PageSnapshot;
import com.salesforce.einstein.webcrawler.model.PageStatus;
import com.salesforce.einstein.webcrawler.model.ResourceType;
import com.salesforce.einstein.webcrawler.model.Scope;
import com.salesforce.einstein.webcrawler.parse.CssParser;
import com.salesforce.einstein.webcrawler.parse.ExtractedLink;
import com.salesforce.einstein.webcrawler.parse.HtmlParser;
import com.salesforce.einstein.webcrawler.parse.ParsedPage;
import com.salesforce.einstein.webcrawler.sitemap.NavigationSitemap;
import com.salesforce.einstein.webcrawler.sitemap.SitemapGraphRecord;
import com.salesforce.einstein.webcrawler.sitemap.SitemapGraphs;
import com.salesforce.einstein.webcrawler.sitemap.Sitemaps;
import com.salesforce.einstein.webcrawler.store.Admission;
import com.salesforce.einstein.webcrawler.store.ContentStore;
import com.salesforce.einstein.webcrawler.store.JobStore;
import com.salesforce.einstein.webcrawler.store.PageStore;
import com.salesforce.einstein.webcrawler.url.BloomFilter;
import com.salesforce.einstein.webcrawler.url.Hashing;
import com.salesforce.einstein.webcrawler.url.TrapDetector;
import com.salesforce.einstein.webcrawler.url.UrlNormalizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

/**
 * The crawl loop: workers pull a task from the {@link Frontier}, fetch it, store it, and turn its links into
 * new tasks. Every guard that keeps a graph crawl finite is applied in {@link #follow} / {@link #addAsset}:
 *
 * <table>
 *   <tr><th>Problem</th><th>Guard</th></tr>
 *   <tr><td>cycles, many links to one page</td><td>canonical URL + atomic seen-test ({@link JobStore#admit})</td></tr>
 *   <tr><td>large depth</td><td>BFS; {@code depth+1 > maxDepth} is recorded as an edge but not followed; page budget</td></tr>
 *   <tr><td>same page, different URL params</td><td>{@link UrlNormalizer} + learned {@code ParamRules} + content-seen test</td></tr>
 *   <tr><td>redirect loops</td><td>redirect targets go through the seen-test; hop limit</td></tr>
 *   <tr><td>spider traps</td><td>{@link TrapDetector}; per-path variant cap</td></tr>
 *   <tr><td>already crawled</td><td>per job: seen-test; across jobs: fresh snapshot reuse or conditional GET</td></tr>
 *   <tr><td>images / video / audio</td><td>leaf nodes, depth not consumed, download per policy, content-addressed</td></tr>
 * </table>
 *
 * <p>A <b>sitemap crawl</b> ({@link CrawlRequest#isSitemap()}) takes its pages from a graph in {@link SitemapGraphs}
 * instead of from the HTML: a fetched page's successors are read from the store and followed at depth+1, a
 * streamed BFS that any node can continue, since the graph's name follows from the job. The HTML's page links are
 * recorded but not followed; its assets are fetched as usual. The sitemap is the scope, so its pages skip the scope,
 * trap and variant checks.
 */
public final class CrawlEngine implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(CrawlEngine.class);

    private final EngineConfig config;
    private final Frontier frontier;
    private final JobStore jobs;
    private final PageStore pages;
    private final ContentStore contents;
    private final Fetcher fetcher;
    private final RobotsCache robots;
    private final UrlNormalizer normalizer;
    private final TrapDetector traps;
    private final SitemapGraphs sitemaps;
    private final HtmlParser parser = new HtmlParser();
    /** Negative cache in front of {@link PageStore}: "this URL was never crawled by anyone". */
    private final BloomFilter everCrawled = new BloomFilter(10_000_000, 0.01);
    private final Clock clock;

    /** Sync waiters on <i>this</i> node, completed by the store's terminal event whichever node finished the job. */
    private final Map<String, CompletableFuture<CrawlJob>> completions = new ConcurrentHashMap<>();
    private final List<Consumer<CrawlJob>> listeners = new CopyOnWriteArrayList<>();
    private final List<Thread> workers = new ArrayList<>();
    private volatile boolean running = true;

    public CrawlEngine(EngineConfig config, Frontier frontier, JobStore jobs, PageStore pages, ContentStore contents,
                       Fetcher fetcher, UrlNormalizer normalizer, TrapDetector traps, SitemapGraphs sitemaps,
                       Clock clock) {
        this.config = config;
        this.frontier = frontier;
        this.jobs = jobs;
        this.pages = pages;
        this.contents = contents;
        this.fetcher = fetcher;
        this.normalizer = normalizer;
        this.traps = traps;
        this.sitemaps = sitemaps;
        this.clock = clock;
        this.robots = new RobotsCache(fetcher, clock, config.agentToken());
        jobs.onTerminal(j -> Optional.ofNullable(completions.remove(j.jobId())).ifPresent(f -> f.complete(j)));
        for (int i = 0; i < config.workers(); i++) {
            Thread t = new Thread(this::workLoop, "crawl-worker-" + i);
            t.setDaemon(true);
            t.start();
            workers.add(t);
        }
    }

    // ------------------------------------------------------------------ jobs

    /**
     * Validates, creates (or returns the idempotent twin of) a job, and enqueues its seeds. Never blocks on crawling:
     * this is the async boundary. {@link #await} is the optional synchronous wait on top of it.
     *
     * <p>A {@code sitemapUrl} is fetched and parsed here, before the job exists, so a bad one creates no job. Its
     * graph is loaded once the job is created, under {@link #graphName}, and dropped when the job ends.
     */
    public CrawlJob submit(CrawlRequest req, String idempotencyKey) {
        req.validate();
        if (req.maxPages() > config.maxPagesPerJob())
            throw new IllegalArgumentException("maxPages: 1.." + config.maxPagesPerJob());
        List<CanonicalUrl> seeds = new ArrayList<>();
        for (String s : req.seeds()) normalizer.normalize(s).ifPresent(seeds::add);
        if (seeds.isEmpty() && !req.seeds().isEmpty()) throw new IllegalArgumentException("no valid http(s) seed URL");
        NavigationSitemap sitemap = null;
        if (req.sitemapUrl() != null) {
            sitemap = Sitemaps.fetch(fetcher, normalizer, req.sitemapUrl(), config.maxSitemapBytes());
            if (sitemap.graph().nodeCount() == 0) throw new IllegalArgumentException("sitemap has no pages");
            for (CanonicalUrl s : seeds)
                if (!sitemap.graph().containsNode(s)) throw new IllegalArgumentException("seed not in the sitemap: " + s);
            if (seeds.isEmpty())
                seeds.addAll(sitemap.roots().isEmpty() ? sitemap.defaultRoots() : sitemap.roots());
        } else if (req.sitemapGraph() != null) {
            checkSitemapGraph(req);
            Set<CanonicalUrl> known = sitemaps.open(req.sitemapGraph()).successors(seeds).keySet();
            for (CanonicalUrl s : seeds)
                if (!known.contains(s)) throw new IllegalArgumentException("seed not in sitemapGraph: " + s);
        }
        if (seeds.isEmpty()) throw new IllegalArgumentException("no valid http(s) seed URL");

        CrawlJob fresh = new CrawlJob(UUID.randomUUID().toString(), req.tenantId(), idempotencyKey, req,
                JobStatus.QUEUED, clock.instant(), null, null);
        CrawlJob job = jobs.createOrGet(fresh);
        if (job != fresh) {
            if (!job.request().equals(req)) throw new IdempotencyConflictException(idempotencyKey);
            return job;
        }
        for (CanonicalUrl s : seeds) jobs.addScopeKey(job.jobId(), scopeKey(req.scope(), s.host()));

        jobs.transition(job.jobId(), JobStatus.QUEUED, JobStatus.RUNNING, null);
        jobs.incrementPending(job.jobId());            // guard: the job can't complete while seeds are being added
        String step = "load the sitemap";
        try {
            if (sitemap != null) sitemaps.load(graphName(job.jobId(), req), sitemap.graph());
            step = "enqueue seeds";
            for (CanonicalUrl s : seeds)
                enqueue(new CrawlTask(job.jobId(), s, 0, null, ResourceType.PAGE, 0, 0), req.maxPages());
            frontier.flush();                          // the caller is told "accepted" only once the seeds are durable
        } catch (RuntimeException e) {
            if (jobs.transition(job.jobId(), JobStatus.RUNNING, JobStatus.FAILED, "could not " + step + ": " + e))
                finished(job.jobId());
            throw e;
        }
        settled(job.jobId(), jobs.decrementPending(job.jobId()));
        return jobs.get(job.jobId()).orElseThrow();
    }

    /**
     * Waits until the job is terminal or the timeout passes; returns the job's state either way. Works on any node:
     * the workers finishing the crawl may run elsewhere.
     */
    public CrawlJob await(String jobId, Duration timeout) throws InterruptedException {
        CompletableFuture<CrawlJob> f = completions.computeIfAbsent(jobId, k -> new CompletableFuture<>());
        CrawlJob now = jobs.get(jobId).orElseThrow();
        if (now.status().isTerminal()) {                                // finished before we subscribed
            completions.remove(jobId);
            return now;
        }
        try {
            f.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException ignored) {
        } catch (ExecutionException e) {
            throw new IllegalStateException(e.getCause());
        }
        return jobs.get(jobId).orElseThrow();
    }

    /** Stops the job. Queued tasks are dropped when a worker polls them, without touching the host. */
    public boolean cancel(String jobId) {
        boolean ok = jobs.transition(jobId, JobStatus.RUNNING, JobStatus.CANCELLED, null)
                || jobs.transition(jobId, JobStatus.QUEUED, JobStatus.CANCELLED, null);
        if (ok) finished(jobId);
        return ok;
    }

    public void onFinished(Consumer<CrawlJob> listener) { listeners.add(listener); }

    public JobStore jobs() { return jobs; }
    public PageStore pages() { return pages; }
    public ContentStore contents() { return contents; }
    public UrlNormalizer normalizer() { return normalizer; }

    // ------------------------------------------------------------------ worker

    private void workLoop() {
        while (running) {
            CrawlTask task;
            try {
                task = frontier.poll(200);
            } catch (InterruptedException e) {
                return;
            }
            if (task == null) continue;
            // The frontier delivers at least once (a dead node's tasks are replayed elsewhere). A closed key is a
            // copy of finished work: skip it, and don't count it again.
            boolean open = jobs.isTaskOpen(task.jobId(), task.key());
            Duration hostDelay = Duration.ZERO;
            try {
                if (open) hostDelay = process(task);
            } catch (RuntimeException e) {
                jobs.page(task.jobId(), task.url().hash()).ifPresent(p ->
                        jobs.update(p.failed(PageStatus.FAILED, 0, e.toString(), clock.instant())));
            } finally {
                boolean durable = true;
                try {
                    frontier.release(task.host(), clock.instant().plus(hostDelay));
                } catch (RuntimeException e) {          // a child may be lost: keep the task open, it is redelivered
                    durable = false;
                }
                if (open && durable)
                    jobs.closeTask(task.jobId(), task.key()).ifPresent(left -> settled(task.jobId(), left));
            }
        }
    }

    /** @return how long the host must rest before its next request ({@code 0} if we didn't contact it) */
    Duration process(CrawlTask task) {
        CrawlJob job = jobs.get(task.jobId()).orElseThrow();
        if (job.status() != JobStatus.RUNNING) return Duration.ZERO;    // cancelled: drop
        CrawlRequest req = job.request();
        JobPage page = jobs.page(task.jobId(), task.url().hash()).orElseThrow();
        if (page.depth() < task.depth()) task = task.atDepth(page.depth());   // a shorter path was found while queued
        URI uri = task.url().uri();
        Instant now = clock.instant();

        Duration delay = config.politenessDelay();
        if (req.respectRobots()) {
            RobotsRules rules = robots.rulesFor(uri);
            String pq = uri.getRawPath() + (uri.getRawQuery() == null ? "" : "?" + uri.getRawQuery());
            if (!rules.isAllowed(pq)) {
                jobs.update(page.failed(PageStatus.BLOCKED_ROBOTS, 0, "disallowed by robots.txt", now));
                return Duration.ZERO;
            }
            delay = rules.crawlDelay().filter(d -> d.compareTo(config.politenessDelay()) > 0).orElse(delay);
        }

        // Already crawled by someone (any job, any tenant) and fresh enough: no request to the origin at all.
        Optional<PageSnapshot> prev = everCrawled.mightContain(task.url().hash())
                ? pages.get(task.url().hash()).filter(s -> contents.contains(s.contentHash()))
                : Optional.empty();
        if (prev.isPresent() && Duration.between(prev.get().fetchedAt(), now).compareTo(req.maxAge()) < 0) {
            onContent(task, page, prev.get(), PageStatus.REUSED, null);
            return Duration.ZERO;
        }

        long maxBytes = task.expectedType().isAsset() ? req.maxAssetBytes() : config.maxPageBytes();
        FetchResult r = fetcher.fetch(new FetchRequest(uri, prev.map(PageSnapshot::etag).orElse(null),
                prev.map(PageSnapshot::lastModified).orElse(null), maxBytes));
        now = clock.instant();

        if (r.status() == 304 && prev.isPresent()) {                    // revalidated: reuse the stored bytes
            PageSnapshot s = prev.get().revalidated(now);
            pages.put(s);
            onContent(task, page, s, PageStatus.REUSED, null);
        } else if (r.isRedirect()) {
            onRedirect(task, page, r, req, now);
        } else if (r.isRetryable() && task.attempt() < config.maxRetries()) {
            CrawlTask retry = task.retry();                              // a new key; opened once even if redelivered
            if (jobs.openTask(task.jobId(), retry.key())) frontier.push(retry);
            return config.retryBackoff().multipliedBy(1L << task.attempt());   // host backs off exponentially
        } else if (r.status() != 200) {
            jobs.update(page.failed(PageStatus.FAILED, r.status(),
                    r.error() != null ? r.error() : "HTTP " + r.status(), now));
        } else if (r.truncated()) {
            jobs.update(page.failed(PageStatus.TOO_LARGE, r.status(), "larger than " + maxBytes + " bytes", now));
        } else {
            String contentHash = Hashing.sha256Hex(r.body());
            contents.put(contentHash, r.body());                         // idempotent, content-addressed
            PageSnapshot s = new PageSnapshot(task.url().hash(), task.url().value(), now, r.status(),
                    r.contentType(), contentHash, r.body().length, r.etag(), r.lastModified());
            pages.put(s);
            everCrawled.put(task.url().hash());
            onContent(task, page, s, PageStatus.FETCHED, r.body());
        }
        return delay;
    }

    private void onContent(CrawlTask task, JobPage page, PageSnapshot s, PageStatus status, byte[] body) {
        ResourceType byHeader = ResourceType.fromContentType(s.contentType());
        ResourceType type = byHeader == ResourceType.OTHER && task.expectedType().isAsset() ? task.expectedType() : byHeader;
        if (status != PageStatus.REUSED) learnParams(task.url(), s.contentHash());   // a reused snapshot is no new evidence

        boolean expand = type == ResourceType.PAGE && task.expectedType() == ResourceType.PAGE;
        if (!expand) {
            jobs.update(page.withContent(status, type, s));
            if (type == ResourceType.CSS) {                              // stylesheets reference images and fonts
                String css = new String(body != null ? body : contents.get(s.contentHash()).orElseThrow(), StandardCharsets.UTF_8);
                for (String u : CssParser.urls(css)) addAsset(task, URI.create(task.url().value()), u, null);
            }
            return;
        }

        // Content-seen test: the same bytes under another URL of this job (?sessionid=…, /index.html vs /).
        // Store a pointer and do not expand it again, otherwise every variant re-enqueues the same links.
        // The key includes the directory: identical bytes in /a/ and /b/ have different relative links, so both expand.
        // A sitemap page is not: the same bytes still have their own successors in the sitemap.
        CrawlRequest req = jobs.get(task.jobId()).orElseThrow().request();
        Optional<String> owner = req.isSitemap() ? Optional.empty()
                : jobs.claimContent(task.jobId(), contentKey(s.contentHash(), task.url()), task.url().hash());
        if (owner.isPresent()) {
            jobs.update(page.withContent(status, type, s).duplicateOf(owner.get()));
            relaxNode(task, owner.get(), task.depth(), req);             // the owner is now reachable this close too
            catchUp(task, req);
            return;
        }
        jobs.update(page.withContent(status, type, s));
        expand(task, body != null ? body : contents.get(s.contentHash()).orElseThrow(), req, true);
        catchUp(task, req);
    }

    /**
     * The other half of relaxation. A shorter path that arrives while this node is still QUEUED (being fetched) finds
     * nothing to relax: its discoverer sees no children yet. So after writing the final status, the worker re-reads
     * the depth and pushes any drop itself. One of the two always sees the other's write, because both go through the
     * node's row: the discoverer lowers the depth, then reads the status; the worker writes the status, then reads the
     * depth.
     */
    private void catchUp(CrawlTask task, CrawlRequest req) {
        jobs.page(task.jobId(), task.url().hash()).filter(n -> n.depth() < task.depth())
                .ifPresent(n -> relax(task.atDepth(n.depth()), req));
    }

    /**
     * Turns a page's links into edges and tasks. {@code recordEdges} is false on a re-expansion: the edges are already
     * stored, only the depth they are followed from changed. In a sitemap crawl the page links are only recorded, and
     * the pages followed are the sitemap's successors.
     */
    private void expand(CrawlTask task, byte[] html, CrawlRequest req, boolean recordEdges) {
        ParsedPage parsed = parser.parse(html, task.url().value());
        URI base = baseOf(parsed, task.url());
        boolean sitemap = req.isSitemap();
        Set<String> linked = new HashSet<>();

        // rel=canonical: the same page, so the same depth, but it costs a redirect hop, or A→B→C→… canonical
        // chains would walk the site without ever consuming depth.
        if (!sitemap && parsed.canonical() != null && task.redirectHops() < config.maxRedirects())
            normalizer.normalize(base, parsed.canonical()).filter(c -> !c.hash().equals(task.url().hash()))
                    .ifPresent(c -> follow(task, c, task.depth(), task.redirectHops() + 1, req));

        for (ExtractedLink link : parsed.links()) {
            if (link.raw().startsWith("#")) continue;                    // in-page anchor, not an edge
            if (link.type() == ResourceType.PAGE) {
                Optional<CanonicalUrl> c = normalizer.normalize(base, link.raw());
                if (c.isEmpty()) continue;
                if (recordEdges) record(task, c.get(), link);
                if (sitemap) linked.add(c.get().hash());
                if (!sitemap && !parsed.noFollow()) follow(task, c.get(), task.depth() + 1, 0, req);
            } else if (recordEdges) {
                addAsset(task, base, link.raw(), link.type());               // assets are leaves: depth doesn't matter
            }
        }
        if (sitemap) expandFromSitemap(task, req, recordEdges, linked);
    }

    /**
     * The sitemap's successors of the task's page: pages at depth+1, assets as leaves. An edge the page's HTML
     * already recorded ({@code linked}) is not recorded twice.
     */
    private void expandFromSitemap(CrawlTask task, CrawlRequest req, boolean recordEdges, Collection<String> linked) {
        List<CanonicalUrl> next = sitemaps.open(graphName(task.jobId(), req)).successors(List.of(task.url()))
                .getOrDefault(task.url(), List.of());
        for (CanonicalUrl to : next) {
            ResourceType type = ResourceType.guessFromPath(to.path());
            if (recordEdges && !linked.contains(to.hash())) record(task, to, new ExtractedLink(to.value(), type));
            if (type == ResourceType.PAGE) follow(task, to, task.depth() + 1, 0, req);
            else if (recordEdges)
                admitAsset(new CrawlTask(task.jobId(), to, task.depth(), task.url().hash(), type, 0, 0), req);
        }
    }

    /**
     * Depth relaxation. Workers on many hosts and nodes make the crawl only <i>roughly</i> breadth-first, so a page
     * can be reached by a long path first, and its children cut by {@code maxDepth} although a shorter path exists.
     * When the shorter path shows up, {@link JobStore#admit} lowers the node's depth ({@code SHALLOWER}); this pushes
     * the change to whatever hangs off the node. No refetch: a page is re-expanded from its stored bytes. Each node's
     * depth only decreases, at most {@code maxDepth} times, so this terminates.
     */
    private void relax(CrawlTask t, CrawlRequest req) {
        // Not visible yet: its discoverer has admitted it but not written its row. It is QUEUED in all but name.
        JobPage n = jobs.page(t.jobId(), t.url().hash()).orElse(null);
        if (n == null) return;
        switch (n.status()) {
            case FETCHED, REUSED -> {
                if (n.type() == ResourceType.PAGE && t.expectedType() == ResourceType.PAGE)
                    contents.get(n.contentHash()).ifPresent(html -> expand(t.atDepth(n.depth()), html, req, false));
            }
            case DUPLICATE -> relaxNode(t, n.duplicateOf(), n.depth(), req);
            case REDIRECT -> {
                relaxNode(t, n.redirectTo(), n.depth(), req);            // a redirect is not a hop
                if (req.isSitemap() && t.expectedType() == ResourceType.PAGE)
                    expandFromSitemap(t.atDepth(n.depth()), req, false, List.of());
            }
            default -> { }   // QUEUED: will run at the new depth (process() reads it); failures have no children
        }
    }

    /** Offers a known node a path of length {@code depth} (through {@link #follow}, so the usual limits apply). */
    private void relaxNode(CrawlTask from, String urlHash, int depth, CrawlRequest req) {
        jobs.page(from.jobId(), urlHash).filter(n -> n.type() == ResourceType.PAGE)
                .ifPresent(n -> follow(from, new CanonicalUrl(n.url(), URI.create(n.url()).getHost(), n.urlHash()),
                        depth, 0, req));
    }

    /**
     * A link to a page. The edge is already recorded; this decides whether it becomes a node. In a sitemap crawl
     * every link followed is the sitemap's, which is the scope, so only depth, budget and the seen-test apply.
     */
    private void follow(CrawlTask parent, CanonicalUrl c, int depth, int hops, CrawlRequest req) {
        if (depth > req.maxDepth()) return;                              // too deep: edge kept, node not created
        // Cheap pre-check; admit() is the real test. A REFERENCED node (seen only as an embed) may still become a page.
        Optional<JobPage> known = jobs.page(parent.jobId(), c.hash()).filter(p -> p.status() != PageStatus.REFERENCED);
        if (known.isPresent()) {
            if (known.get().depth() <= depth) return;                    // already reached by a path this short
        } else if (!req.isSitemap()) {
            if (outOfScope(parent.jobId(), req.scope(), c)) return;
            if (traps.check(c).isPresent()) return;
            if (!admitVariant(parent.jobId(), c)) return;
        }
        enqueue(new CrawlTask(parent.jobId(), c, depth, parent.url().hash(), ResourceType.PAGE, hops, 0), req.maxPages());
    }

    /** An embedded resource: a leaf at the parent's depth, from any host (CDNs), downloaded only if policy says so. */
    private void addAsset(CrawlTask parent, URI base, String raw, ResourceType hint) {
        Optional<CanonicalUrl> c = normalizer.normalize(base, raw);
        if (c.isEmpty()) return;
        ResourceType type = hint != null ? hint : assetType(c.get());
        record(parent, c.get(), new ExtractedLink(raw, type));
        admitAsset(new CrawlTask(parent.jobId(), c.get(), parent.depth(), parent.url().hash(), type, 0, 0),
                jobs.get(parent.jobId()).orElseThrow().request());
    }

    /** An asset task: fetched if its type is downloaded, else a {@link PageStatus#REFERENCED} node. */
    private void admitAsset(CrawlTask t, CrawlRequest req) {
        if (req.downloadAssets().contains(t.expectedType())) enqueue(t, req.maxAssets());
        else if (jobs.admit(JobPage.referenced(t), false, req.maxAssets()) == Admission.OVER_BUDGET)
            jobs.markTruncated(t.jobId());
    }

    private void onRedirect(CrawlTask task, JobPage page, FetchResult r, CrawlRequest req, Instant now) {
        Optional<CanonicalUrl> target = normalizer.normalize(task.url().uri(), r.location());
        if (target.isEmpty()) {
            jobs.update(page.failed(PageStatus.FAILED, r.status(), "bad Location: " + r.location(), now));
            return;
        }
        jobs.update(page.redirect(r.status(), target.get().hash(), now));
        record(task, target.get(), new ExtractedLink(r.location(), task.expectedType()));
        boolean asset = task.expectedType().isAsset();
        if (!asset && req.isSitemap()) expandFromSitemap(task, req, true, List.of());   // the sitemap knows this URL
        if (task.redirectHops() >= config.maxRedirects()) {
            jobs.update(page.failed(PageStatus.FAILED, r.status(), "too many redirects", now));
            return;
        }
        // Same depth: a redirect is not a link hop. A loop (A→B→A) ends at the seen-test. In a sitemap crawl a page
        // may redirect within its own scope (its host, under SAME_HOST), wherever the sitemap took the crawl.
        if (!asset && task.depth() == 0)        // a seed that redirects (example.com → www.example.com): the user
            jobs.addScopeKey(task.jobId(), scopeKey(req.scope(), target.get().host()));   // meant the destination
        boolean inScope = req.isSitemap()
                && scopeKey(req.scope(), target.get().host()).equals(scopeKey(req.scope(), task.url().host()));
        if (!asset && ((!inScope && outOfScope(task.jobId(), req.scope(), target.get()))
                || traps.check(target.get()).isPresent())) return;
        enqueue(new CrawlTask(task.jobId(), target.get(), task.depth(), task.url().hash(), task.expectedType(),
                task.redirectHops() + 1, 0), asset ? req.maxAssets() : req.maxPages());
        if (!asset) catchUp(task, req);
    }

    private void enqueue(CrawlTask t, long budget) {
        switch (jobs.admit(JobPage.queued(t), true, budget)) {
            case ADMITTED -> frontier.push(t);
            case SHALLOWER -> relax(t, jobs.get(t.jobId()).orElseThrow().request());
            case OVER_BUDGET -> jobs.markTruncated(t.jobId());
            case SEEN -> { }                                             // cycle or second link: nothing to do
        }
    }

    /** The per-path variant cap, counted in the shared store so it holds across nodes. */
    private boolean admitVariant(String jobId, CanonicalUrl c) {
        return traps.variantKey(c).map(k -> jobs.countVariant(jobId, k) <= traps.maxVariantsPerPath()).orElse(true);
    }

    private void record(CrawlTask from, CanonicalUrl to, ExtractedLink link) {
        jobs.addLink(new LinkEdge(from.jobId(), from.url().hash(), to.hash(), to.value(), link.type(), link.raw()));
    }

    /**
     * Parameter learning: if {@code /p?x=1} has the same bytes as {@code /p}, that's a vote for "x is irrelevant on
     * this host". Votes count distinct values of {@code x}, so one default value ({@code ?page=1}) seen many times
     * cannot condemn the parameter. Uses global snapshots, so evidence accumulates across jobs.
     */
    private void learnParams(CanonicalUrl url, String contentHash) {
        for (Map.Entry<String, String> p : UrlNormalizer.params(url)) {
            normalizer.without(url, p.getKey()).flatMap(w -> pages.get(w.hash()))
                    .ifPresent(other -> normalizer.params().observe(url.host(), p.getKey(), p.getValue(),
                            other.contentHash().equals(contentHash)));
        }
    }

    /** Content-seen key: the bytes plus the directory relative links resolve against. */
    static String contentKey(String contentHash, CanonicalUrl url) {
        String v = url.value();
        int q = v.indexOf('?');
        String noQuery = q < 0 ? v : v.substring(0, q);
        return contentHash + " " + noQuery.substring(0, noQuery.lastIndexOf('/') + 1);
    }

    /** {@code pending} reached {@code left}; at zero, whichever node gets there first completes the job. */
    private void settled(String jobId, long left) {
        if (left == 0 && jobs.transition(jobId, JobStatus.RUNNING, JobStatus.COMPLETED, null)) finished(jobId);
    }

    /**
     * Runs once per job, on the node that won the terminal transition (so webhooks fire once). A {@code sitemapUrl}
     * job's graph is dropped: nothing is pending, and its edges are in the job store. A named graph is kept.
     */
    private void finished(String jobId) {
        CrawlJob job = jobs.get(jobId).orElseThrow();
        if (job.request().sitemapUrl() != null) {
            try {
                sitemaps.drop(graphName(jobId, job.request()));
            } catch (RuntimeException e) {
                log.warn("could not drop the sitemap graph of job {}", jobId, e);
            }
        }
        for (Consumer<CrawlJob> l : listeners) {
            try { l.accept(job); } catch (RuntimeException ignored) { }
        }
    }

    /**
     * A named graph must be there, and, if it was loaded through the API, be the tenant's and {@code READY}. Another
     * tenant's graph reads as unknown, so names can't be probed.
     */
    private void checkSitemapGraph(CrawlRequest req) {
        String name = req.sitemapGraph();
        sitemaps.record(name).ifPresentOrElse(entry -> {
            if (!entry.tenantId().equals(req.tenantId()))
                throw new IllegalArgumentException("unknown sitemapGraph: " + name);
            if (entry.status() != SitemapGraphRecord.Status.READY)
                throw new IllegalArgumentException("sitemapGraph " + name + " is " + entry.status());
        }, () -> {
            if (!sitemaps.exists(name)) throw new IllegalArgumentException("unknown sitemapGraph: " + name);  // a bulk import
        });
    }

    /** The job's sitemap graph: the named one, or the one loaded for it from {@code sitemapUrl}. */
    static String graphName(String jobId, CrawlRequest req) {
        return req.sitemapGraph() != null ? req.sitemapGraph() : CrawlRequest.JOB_GRAPH_PREFIX + jobId.replace("-", "");
    }

    private boolean outOfScope(String jobId, Scope scope, CanonicalUrl c) {
        if (scope == Scope.ANY) return false;
        return !jobs.hasScopeKey(jobId, scopeKey(scope, c.host()));
    }

    static String scopeKey(Scope scope, String host) {
        return scope == Scope.SAME_DOMAIN ? registrableDomain(host) : host;
    }

    /** Approximation; production uses the Public Suffix List ({@code a.b.co.uk} → {@code b.co.uk}). */
    static String registrableDomain(String host) {
        if (host.indexOf(':') >= 0 || host.matches("[0-9.]+")) return host;   // IP literal: no domain to widen to
        String[] l = host.split("\\.");
        if (l.length <= 2) return host;
        boolean secondLevelSuffix = l[l.length - 1].length() == 2
                && Set.of("co", "com", "org", "net", "gov", "ac", "edu").contains(l[l.length - 2]);
        int keep = secondLevelSuffix ? 3 : 2;
        return String.join(".", Arrays.copyOfRange(l, Math.max(0, l.length - keep), l.length));
    }

    private static URI baseOf(ParsedPage parsed, CanonicalUrl page) {
        try {
            return parsed.baseUri() == null || parsed.baseUri().isBlank() ? page.uri() : URI.create(parsed.baseUri());
        } catch (IllegalArgumentException e) {
            return page.uri();
        }
    }

    private static ResourceType assetType(CanonicalUrl c) {
        ResourceType t = ResourceType.guessFromPath(c.path());
        return t == ResourceType.PAGE ? ResourceType.OTHER : t;
    }

    @Override public void close() {
        running = false;
        workers.forEach(Thread::interrupt);
    }
}
