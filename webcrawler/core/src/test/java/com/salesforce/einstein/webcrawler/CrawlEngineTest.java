package com.salesforce.einstein.webcrawler;

import com.salesforce.einstein.webcrawler.engine.CrawlEngine;
import com.salesforce.einstein.webcrawler.engine.EngineConfig;
import com.salesforce.einstein.webcrawler.engine.IdempotencyConflictException;
import com.salesforce.einstein.webcrawler.fetch.FetchResult;
import com.salesforce.einstein.webcrawler.store.InMemoryContentStore;
import com.salesforce.einstein.webcrawler.model.CanonicalUrl;
import com.salesforce.einstein.webcrawler.model.CrawlJob;
import com.salesforce.einstein.webcrawler.model.CrawlMode;
import com.salesforce.einstein.webcrawler.model.CrawlRequest;
import com.salesforce.einstein.webcrawler.model.JobPage;
import com.salesforce.einstein.webcrawler.model.JobStatus;
import com.salesforce.einstein.webcrawler.model.PageStatus;
import com.salesforce.einstein.webcrawler.model.ResourceType;
import com.salesforce.einstein.webcrawler.model.Scope;
import com.salesforce.einstein.webcrawler.url.ParamRules;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CrawlEngineTest {

    private static final String S = "https://site.com";
    private final FakeWeb web = new FakeWeb();
    private CrawlEngine engine;

    @AfterEach void stop() { if (engine != null) engine.close(); }

    private CrawlJob crawl(CrawlRequest req) throws InterruptedException {
        if (engine == null) engine = TestEngines.engine(web, 4);
        CrawlJob job = engine.submit(req, null);
        CrawlJob done = engine.await(job.jobId(), Duration.ofSeconds(10));
        assertEquals(JobStatus.COMPLETED, done.status());
        return done;
    }

    private Optional<JobPage> node(CrawlJob job, String url) {
        CanonicalUrl c = engine.normalizer().normalize(url).orElseThrow();
        return engine.jobs().page(job.jobId(), c.hash());
    }

    private List<JobPage> nodes(CrawlJob job) { return engine.jobs().pages(job.jobId(), null, null, 10_000).items(); }

    private static String links(String... hrefs) {
        StringBuilder sb = new StringBuilder("<html><body>");
        for (String h : hrefs) sb.append("<a href=\"").append(h).append("\">x</a>");
        return sb.append("</body></html>").toString();
    }

    // ---------------------------------------------------------------- cycles

    @Test void cyclesAreVisitedOnceButEdgesAreKept() throws Exception {
        web.page(S + "/a", links("/b", "/a", "#top"))          // self-link and fragment-only link
           .page(S + "/b", links("/c", "a"))                   // back-edge to /a (relative)
           .page(S + "/c", links("/a", "/b", "https://SITE.com:443/a#x"));
        CrawlJob job = crawl(CrawlRequest.builder("t1", S + "/a").maxDepth(10).build());

        assertEquals(1, web.hits(S + "/a"));
        assertEquals(1, web.hits(S + "/b"));
        assertEquals(1, web.hits(S + "/c"));
        assertEquals(3, nodes(job).size());
        assertEquals(7, engine.jobs().stats(job.jobId()).edges());   // every edge, including the 5 that close cycles
        assertEquals(1, node(job, S + "/b").orElseThrow().depth());
        assertEquals(2, node(job, S + "/c").orElseThrow().depth());
    }

    @Test void redirectLoopTerminates() throws Exception {
        web.page(S + "/", links("/r1")).redirect(S + "/r1", S + "/r2").redirect(S + "/r2", "/r1");
        CrawlJob job = crawl(CrawlRequest.builder("t1", S + "/").build());
        assertEquals(PageStatus.REDIRECT, node(job, S + "/r1").orElseThrow().status());
        assertEquals(PageStatus.REDIRECT, node(job, S + "/r2").orElseThrow().status());
        assertEquals(1, web.hits(S + "/r1"));
        assertEquals(1, node(job, S + "/r2").orElseThrow().depth(), "a redirect does not consume depth");
    }

    // ---------------------------------------------------------------- depth and budgets

    @Test void depthLimitKeepsTheEdgeButNotTheNode() throws Exception {
        web.page(S + "/0", links("/1")).page(S + "/1", links("/2")).page(S + "/2", links("/3")).page(S + "/3", links("/4"));
        CrawlJob job = crawl(CrawlRequest.builder("t1", S + "/0").maxDepth(2).build());

        assertTrue(node(job, S + "/2").isPresent());
        assertTrue(node(job, S + "/3").isEmpty());
        assertEquals(0, web.hits(S + "/3"));
        String h2 = node(job, S + "/2").orElseThrow().urlHash();
        assertEquals(S + "/3", engine.jobs().outLinks(job.jobId(), h2).get(0).toUrl());
    }

    @Test void pageBudgetTruncatesBreadthFirst() throws Exception {
        String[] kids = IntStream.range(0, 50).mapToObj(i -> "/k" + i).toArray(String[]::new);
        web.page(S + "/", links(kids));
        for (String k : kids) web.page(S + k, links("/deep" + k));
        CrawlJob job = crawl(CrawlRequest.builder("t1", S + "/").maxDepth(5).maxPages(10).build());

        assertEquals(10, nodes(job).stream().filter(p -> p.type() == ResourceType.PAGE).count());
        assertTrue(engine.jobs().stats(job.jobId()).truncated() > 0);
        assertTrue(nodes(job).stream().allMatch(p -> p.depth() <= 1), "BFS: the budget is spent nearest the seed");
    }

    @Test void scopeSameHostIgnoresOtherHostsButSameDomainFollowsSubdomains() throws Exception {
        web.page(S + "/", links("https://blog.site.com/", "https://other.com/"))
           .page("https://blog.site.com/", links()).page("https://other.com/", links());
        CrawlJob host = crawl(CrawlRequest.builder("t1", S + "/").scope(Scope.SAME_HOST).build());
        assertTrue(node(host, "https://blog.site.com/").isEmpty());

        CrawlJob domain = crawl(CrawlRequest.builder("t1", S + "/").scope(Scope.SAME_DOMAIN).maxAge(Duration.ZERO).build());
        assertTrue(node(domain, "https://blog.site.com/").isPresent());
        assertTrue(node(domain, "https://other.com/").isEmpty());
    }

    @Test void spiderTrapPathsAreNotFollowed() throws Exception {
        // A relative-link bug: every page links to "a/" under itself → /a/, /a/a/, /a/a/a/ … forever
        web.page(S + "/", links("a/"));
        for (int i = 1; i <= 8; i++) web.page(S + "/" + "a/".repeat(i), links("a/"));
        CrawlJob job = crawl(CrawlRequest.builder("t1", S + "/").maxDepth(50).build());
        assertTrue(nodes(job).size() <= 4, "stopped by the repeating-segment check, not by depth");
    }

    @Test void seedThatRedirectsToAnotherHostKeepsItsDestinationInScope() throws Exception {
        web.redirect(S + "/", "https://www.site.com/")
           .page("https://www.site.com/", links("/about")).page("https://www.site.com/about", links());
        CrawlJob job = crawl(CrawlRequest.builder("t1", S + "/").scope(Scope.SAME_HOST).build());
        assertEquals(PageStatus.FETCHED, node(job, "https://www.site.com/about").orElseThrow().status());
    }

    @Test void canonicalChainsConsumeHopsSoTheyCannotBypassDepth() throws Exception {
        for (int i = 0; i < 20; i++)
            web.page(S + "/c" + i, "<html><head><link rel=canonical href=/c" + (i + 1) + "></head></html>");
        CrawlJob job = crawl(CrawlRequest.builder("t1", S + "/c0").maxDepth(0).build());
        assertTrue(nodes(job).size() <= 1 + EngineConfig.defaults().maxRedirects(), "bounded by maxRedirects");
        assertTrue(node(job, S + "/c19").isEmpty());
    }

    @Test void identicalBytesInDifferentDirectoriesAreBothExpanded() throws Exception {
        String body = links("child");                                  // relative: means /a/child or /b/child
        web.page(S + "/", links("/a/", "/b/")).page(S + "/a/", body).page(S + "/b/", body)
           .page(S + "/a/child", "<p>a</p>").page(S + "/b/child", "<p>b</p>");
        CrawlJob job = crawl(CrawlRequest.builder("t1", S + "/").build());
        assertEquals(1, web.hits(S + "/a/child"));
        assertEquals(1, web.hits(S + "/b/child"));
        assertEquals(0, engine.jobs().stats(job.jobId()).duplicates());
    }

    @Test void urlFirstSeenAsAnEmbedIsStillCrawledWhenAPageLinksToIt() throws Exception {
        web.page(S + "/", "<video src=/watch></video><a href=/next>next</a>")   // video: reference-only
           .page(S + "/next", links("/watch"))
           .page(S + "/watch", "<p>an HTML page after all</p>");
        CrawlJob job = crawl(CrawlRequest.builder("t1", S + "/").build());
        assertEquals(1, web.hits(S + "/watch"));
        assertEquals(PageStatus.FETCHED, node(job, S + "/watch").orElseThrow().status());
    }

    // ---------------------------------------------------------------- URL parameters

    @Test void trackingParamsCollapseIntoOneNode() throws Exception {
        web.page(S + "/", links("/p?utm_source=a", "/p?sessionid=1&utm_medium=b", "/p", "/item?id=1", "/item?id=2"))
           .page(S + "/p", links()).page(S + "/item?id=1", "<p>one</p>").page(S + "/item?id=2", "<p>two</p>");
        CrawlJob job = crawl(CrawlRequest.builder("t1", S + "/").build());
        assertEquals(1, web.hits(S + "/p"));
        assertEquals(4, nodes(job).size());   // /, /p, /item?id=1, /item?id=2
    }

    @Test void sameContentUnderDifferentParamsIsADuplicateAndNotExpanded() throws Exception {
        String body = links("/only-from-p");
        web.page(S + "/", links("/p", "/p?view=grid"))
           .page(S + "/p", body).page(S + "/p?view=grid", body).page(S + "/only-from-p", links());
        CrawlJob job = crawl(CrawlRequest.builder("t1", S + "/").build());

        JobPage a = node(job, S + "/p").orElseThrow(), b = node(job, S + "/p?view=grid").orElseThrow();
        assertEquals(a.contentHash(), b.contentHash());
        JobPage dup = a.status() == PageStatus.DUPLICATE ? a : b;
        JobPage owner = dup == a ? b : a;
        assertEquals(PageStatus.DUPLICATE, dup.status());
        assertEquals(owner.urlHash(), dup.duplicateOf());
        assertTrue(engine.jobs().outLinks(job.jobId(), dup.urlHash()).isEmpty(), "a duplicate is not parsed again");
        assertEquals(1, blobs() - 2);   // "/", "/only-from-p" + ONE blob for both /p variants
    }

    @Test void irrelevantParamIsLearnedFromIdenticalContent() throws Exception {
        // /list links to itself with ?ref=N, and every variant returns /list's bytes
        String listBody = links("/list?ref=1", "/list?ref=2", "/list?ref=3");
        web.page(S + "/", links("/list")).page(S + "/list", listBody)
           .page(S + "/list?ref=1", listBody).page(S + "/list?ref=2", listBody).page(S + "/list?ref=3", listBody);
        engine = TestEngines.engine(web, 1);
        crawl(CrawlRequest.builder("t1", S + "/").maxDepth(3).build());

        ParamRules rules = engine.normalizer().params();
        assertEquals(Set.of("ref"), rules.learnedIrrelevant("site.com"));
        assertEquals(S + "/list", engine.normalizer().normalize(S + "/list?ref=99").orElseThrow().value());
    }

    // ---------------------------------------------------------------- assets

    @Test void assetsAreLeavesStoredOncePerContentAndMediaIsReferenceOnly() throws Exception {
        byte[] png = {(byte) 0x89, 'P', 'N', 'G', 1, 2, 3};
        String page = "<html><head><link rel=stylesheet href=/s.css></head><body>"
                + "<img src=/logo.png srcset='/logo.png 1x, /logo@2x.png 2x'>"
                + "<video src=/intro.mp4 poster=/poster.jpg></video><audio><source src=/a.mp3></audio>"
                + "<a href=/next>next</a></body></html>";
        web.page(S + "/", page).page(S + "/next", page.replace("/next", "/"))
           .asset(S + "/s.css", "text/css", "body{background:url('bg.png')}".getBytes(StandardCharsets.UTF_8))
           .asset(S + "/logo.png", "image/png", png).asset(S + "/logo@2x.png", "image/png", png)
           .asset(S + "/bg.png", "image/png", new byte[]{9}).asset(S + "/poster.jpg", "image/jpeg", new byte[]{7})
           .asset(S + "/intro.mp4", "video/mp4", new byte[1 << 16]);
        CrawlJob job = crawl(CrawlRequest.builder("t1", S + "/").maxDepth(1)
                .downloadAssets(EnumSet.of(ResourceType.IMAGE, ResourceType.CSS)).build());

        assertEquals(1, web.hits(S + "/logo.png"), "shared by both pages, fetched once");
        assertEquals(1, web.hits(S + "/bg.png"), "found inside the stylesheet");
        assertEquals(0, web.hits(S + "/intro.mp4"));
        assertEquals(0, web.hits(S + "/a.mp3"));
        assertEquals(PageStatus.REFERENCED, node(job, S + "/intro.mp4").orElseThrow().status());
        assertEquals(ResourceType.AUDIO, node(job, S + "/a.mp3").orElseThrow().type());
        JobPage logo = node(job, S + "/logo.png").orElseThrow(), logo2x = node(job, S + "/logo@2x.png").orElseThrow();
        assertEquals(logo.contentHash(), logo2x.contentHash(), "identical bytes → one blob");
        assertEquals(0, logo.depth(), "an asset inherits its page's depth");
        // /next is at maxDepth=1, yet its images are part of it; they were already fetched via "/"
        assertEquals(PageStatus.FETCHED, node(job, S + "/next").orElseThrow().status());
    }

    @Test void oversizedAssetIsNotStored() throws Exception {
        web.page(S + "/", "<img src=/huge.png>").asset(S + "/huge.png", "image/png", new byte[2048]);
        CrawlJob job = crawl(CrawlRequest.builder("t1", S + "/").maxAssetBytes(1024).build());
        assertEquals(PageStatus.TOO_LARGE, node(job, S + "/huge.png").orElseThrow().status());
    }

    @Test void assetBudgetCapsDownloads() throws Exception {
        web.page(S + "/", "<img src=/1.png><img src=/2.png><img src=/3.png>")
           .asset(S + "/1.png", "image/png", new byte[]{1}).asset(S + "/2.png", "image/png", new byte[]{2})
           .asset(S + "/3.png", "image/png", new byte[]{3});
        crawl(CrawlRequest.builder("t1", S + "/").maxAssets(2).build());
        assertEquals(2, web.hits(S + "/1.png") + web.hits(S + "/2.png") + web.hits(S + "/3.png"));
    }

    // ---------------------------------------------------------------- already crawled

    @Test void secondJobReusesFreshSnapshotsWithoutTouchingTheOrigin() throws Exception {
        web.page(S + "/", links("/a")).page(S + "/a", links());
        crawl(CrawlRequest.builder("t1", S + "/").build());
        int before = web.totalHitsExcludingRobots();

        CrawlJob again = crawl(CrawlRequest.builder("t2", S + "/").maxAge(Duration.ofHours(1)).build());
        assertEquals(before, web.totalHitsExcludingRobots());
        assertEquals(PageStatus.REUSED, node(again, S + "/a").orElseThrow().status());
        assertEquals(2, engine.jobs().stats(again.jobId()).reused());
    }

    @Test void staleSnapshotIsRevalidatedWithConditionalGet() throws Exception {
        web.page(S + "/", links());
        crawl(CrawlRequest.builder("t1", S + "/").build());
        CrawlJob again = crawl(CrawlRequest.builder("t1", S + "/").maxAge(Duration.ZERO).build());
        assertEquals(2, web.hits(S + "/"));                                   // asked, origin said 304
        assertEquals(PageStatus.REUSED, node(again, S + "/").orElseThrow().status());
        assertEquals(1, blobs());
    }

    private int blobs() { return ((InMemoryContentStore) engine.contents()).size(); }

    // ---------------------------------------------------------------- failures, robots, API semantics

    @Test void transientErrorsAreRetriedWithBackoff() throws Exception {
        web.sequence(S + "/", FetchResult.status(503), FetchResult.networkError("timeout"),
                FetchResult.ok("text/html", "<p>ok</p>".getBytes(StandardCharsets.UTF_8)));
        CrawlJob job = crawl(CrawlRequest.builder("t1", S + "/").build());
        assertEquals(PageStatus.FETCHED, node(job, S + "/").orElseThrow().status());
        assertEquals(3, web.hits(S + "/"));
    }

    @Test void notFoundIsFinal() throws Exception {
        web.page(S + "/", links("/missing"));
        CrawlJob job = crawl(CrawlRequest.builder("t1", S + "/").build());
        JobPage m = node(job, S + "/missing").orElseThrow();
        assertEquals(PageStatus.FAILED, m.status());
        assertEquals(404, m.httpStatus());
        assertEquals(1, web.hits(S + "/missing"));
    }

    @Test void robotsDisallowIsHonouredWithoutFetching() throws Exception {
        web.robots(S, "User-agent: *\nDisallow: /private\n").page(S + "/", links("/private/x", "/public"))
           .page(S + "/public", links());
        CrawlJob job = crawl(CrawlRequest.builder("t1", S + "/").build());
        assertEquals(PageStatus.BLOCKED_ROBOTS, node(job, S + "/private/x").orElseThrow().status());
        assertEquals(0, web.hits(S + "/private/x"));
        assertEquals(PageStatus.FETCHED, node(job, S + "/public").orElseThrow().status());
    }

    @Test void robotsTxtRedirectIsFollowed() throws Exception {
        //noinspection HttpUrlsUsage: the plain-http origin is what this test redirects away from
        String http = "http://site.com";                                // http → https, the most common robots redirect
        web.redirect(http + "/robots.txt", S + "/robots.txt").robots(S, "User-agent: *\nDisallow: /private\n")
           .page(http + "/", links("/private", "/public")).page(http + "/public", links());
        CrawlJob job = crawl(CrawlRequest.builder("t1", http + "/").build());
        assertEquals(PageStatus.FETCHED, node(job, http + "/public").orElseThrow().status(), "not disallow-all");
        assertEquals(PageStatus.BLOCKED_ROBOTS, node(job, http + "/private").orElseThrow().status());
    }

    @Test void metaNofollowRecordsEdgesButFollowsNothing() throws Exception {
        web.page(S + "/", "<meta name=robots content=nofollow><a href=/a>a</a>").page(S + "/a", links());
        CrawlJob job = crawl(CrawlRequest.builder("t1", S + "/").build());
        assertTrue(node(job, S + "/a").isEmpty());
        assertEquals(1, engine.jobs().stats(job.jobId()).edges());
    }

    @Test void idempotencyKeyReturnsTheSameJobAndRejectsADifferentBody() {
        web.page(S + "/", links());
        engine = TestEngines.engine(web, 2);
        CrawlRequest req = CrawlRequest.builder("t1", S + "/").build();
        CrawlJob first = engine.submit(req, "k1");
        assertEquals(first.jobId(), engine.submit(req, "k1").jobId());
        assertThrows(IdempotencyConflictException.class,
                () -> engine.submit(CrawlRequest.builder("t1", S + "/other").build(), "k1"));
        assertNotEquals(first.jobId(), engine.submit(req, "k2").jobId());
    }

    @Test void concurrentSubmitsWithOneIdempotencyKeyCreateOneJob() throws Exception {
        web.page(S + "/", links());
        engine = TestEngines.engine(web, 2);
        CrawlRequest req = CrawlRequest.builder("t1", S + "/").build();
        ExecutorService pool = Executors.newFixedThreadPool(16);
        try {
            List<Future<CrawlJob>> fs = pool.invokeAll(Collections.nCopies(16, () -> engine.submit(req, "race")));
            Set<String> ids = new HashSet<>();
            for (Future<CrawlJob> f : fs) ids.add(f.get().jobId());
            assertEquals(1, ids.size());
        } finally {
            pool.shutdown();
        }
    }

    @Test void syncModeIsBounded() {
        CrawlRequest tooBig = CrawlRequest.builder("t1", S + "/").mode(CrawlMode.SYNC).maxDepth(3).build();
        assertThrows(IllegalArgumentException.class, tooBig::validate);
        CrawlRequest ok = CrawlRequest.builder("t1", S + "/").mode(CrawlMode.SYNC).maxDepth(1).maxPages(20).build();
        ok.validate();
        assertSame(CrawlMode.SYNC, ok.mode());
        CrawlRequest withHook = CrawlRequest.builder("t1", S + "/").callbackUrl("https://hooks.example/done").build();
        assertEquals("https://hooks.example/done", withHook.callbackUrl());
    }

    @Test void cancelStopsTheCrawl() throws Exception {
        String[] kids = IntStream.range(0, 200).mapToObj(i -> "https://h" + i + ".com/").toArray(String[]::new);
        web.page(S + "/", links(kids));
        CountDownLatch seedInFlight = new CountDownLatch(1), cancelled = new CountDownLatch(1);
        engine = TestEngines.engine(req -> {                  // hold the seed fetch until cancel() has run, so the
            if (req.url().toString().equals(S + "/")) {       // crawl can't finish first (in-memory fetches are fast)
                seedInFlight.countDown();
                try { cancelled.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            }
            return web.fetch(req);
        }, 1);
        CrawlJob job = engine.submit(CrawlRequest.builder("t1", S + "/").scope(Scope.ANY).build(), null);
        assertTrue(seedInFlight.await(5, TimeUnit.SECONDS));
        engine.cancel(job.jobId());
        cancelled.countDown();
        assertEquals(JobStatus.CANCELLED, engine.await(job.jobId(), Duration.ofSeconds(5)).status());
        Thread.sleep(200);
        assertTrue(web.totalHitsExcludingRobots() <= 2);
    }
}
