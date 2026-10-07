package com.salesforce.einstein.webcrawler;

import com.salesforce.einstein.webcrawler.model.CanonicalUrl;
import com.salesforce.einstein.webcrawler.model.CrawlJob;
import com.salesforce.einstein.webcrawler.model.CrawlRequest;
import com.salesforce.einstein.webcrawler.model.CrawlTask;
import com.salesforce.einstein.webcrawler.model.JobPage;
import com.salesforce.einstein.webcrawler.model.JobStats;
import com.salesforce.einstein.webcrawler.model.JobStatus;
import com.salesforce.einstein.webcrawler.model.LinkEdge;
import com.salesforce.einstein.webcrawler.model.PageSnapshot;
import com.salesforce.einstein.webcrawler.model.PageStatus;
import com.salesforce.einstein.webcrawler.model.ResourceType;
import com.salesforce.einstein.webcrawler.model.Scope;
import com.salesforce.einstein.webcrawler.store.Admission;
import com.salesforce.einstein.webcrawler.store.JobStore;
import com.salesforce.einstein.webcrawler.store.Slice;
import com.salesforce.einstein.webcrawler.url.Hashing;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * What every {@link JobStore} must do, whatever backs it. The engine's correctness across nodes rests on the atomic
 * parts (admit, open/close task, claim content), so those are also checked under contention.
 */
public abstract class JobStoreContract {

    protected abstract JobStore store();

    private final String tenant = "t-" + UUID.randomUUID();

    private CrawlJob newJob(String idempotencyKey) {
        CrawlRequest req = CrawlRequest.builder(tenant, "https://a.com/").sitemapUrl("https://a.com/nav.xml")
                .scope(Scope.SAME_DOMAIN).maxDepth(3)
                .maxAge(Duration.ofMinutes(5)).callbackUrl("https://hooks.example/x").build();
        return new CrawlJob(UUID.randomUUID().toString(), tenant, idempotencyKey, req, JobStatus.QUEUED,
                Instant.now().truncatedTo(ChronoUnit.MILLIS), null, null);
    }

    private String job() { return store().createOrGet(newJob(null)).jobId(); }

    private static CanonicalUrl url(String u) {
        return new CanonicalUrl(u, java.net.URI.create(u).getHost(), Hashing.sha256Hex(u.getBytes(StandardCharsets.UTF_8)).substring(0, 32));
    }

    private static CrawlTask task(String job, String u, int depth) {
        return new CrawlTask(job, url(u), depth, null, ResourceType.PAGE, 0, 0);
    }

    private static CrawlTask asset(String job, String u) {
        return new CrawlTask(job, url(u), 1, null, ResourceType.IMAGE, 0, 0);
    }

    private static PageSnapshot snapshot(CrawlTask t, String contentType, Instant at) {
        return new PageSnapshot(t.url().hash(), t.url().value(), at, 200, contentType, "sha-" + t.url().hash(), 42, null, null);
    }

    // ------------------------------------------------------------------ jobs

    @Test void createOrGetIsIdempotentPerTenantAndKey() {
        CrawlJob first = store().createOrGet(newJob("k1"));
        CrawlJob again = store().createOrGet(newJob("k1"));
        assertEquals(first.jobId(), again.jobId(), "same tenant and key: the existing job");
        assertEquals(first.request(), again.request(), "the request survives the round trip unchanged");
        assertNotEquals(first.jobId(), store().createOrGet(newJob("k2")).jobId());
        assertNotEquals(first.jobId(), store().createOrGet(newJob(null)).jobId(), "no key: always a new job");
        assertEquals(first, store().get(first.jobId()).orElseThrow());
        assertTrue(store().get(UUID.randomUUID().toString()).isEmpty());
    }

    @Test void transitionIsACompareAndSetAndTerminalIsBroadcast() throws Exception {
        String j = job();
        CountDownLatch done = new CountDownLatch(1);
        List<CrawlJob> seen = new ArrayList<>();
        store().onTerminal(x -> { if (x.jobId().equals(j)) { seen.add(x); done.countDown(); } });

        assertTrue(store().transition(j, JobStatus.QUEUED, JobStatus.RUNNING, null));
        assertFalse(store().transition(j, JobStatus.QUEUED, JobStatus.RUNNING, null), "lost the race");
        assertEquals(JobStatus.RUNNING, store().get(j).orElseThrow().status());
        assertTrue(store().transition(j, JobStatus.RUNNING, JobStatus.FAILED, "boom"));

        assertTrue(done.await(5, TimeUnit.SECONDS), "terminal listeners are notified");
        CrawlJob after = seen.get(0);
        assertEquals(JobStatus.FAILED, after.status());
        assertEquals("boom", after.error());
        assertNotNull(after.finishedAt());
        assertEquals(after.status(), store().get(j).orElseThrow().status());
    }

    // ------------------------------------------------------------------ the seen-test

    @Test void admitIsInsertIfAbsentAndOpensOneTask() {
        String j = job();
        CrawlTask t = task(j, "https://a.com/x", 1);
        assertEquals(Admission.ADMITTED, store().admit(JobPage.queued(t), true, 10));
        assertEquals(Admission.SEEN, store().admit(JobPage.queued(t), true, 10));
        assertEquals(Admission.SEEN, store().admit(JobPage.queued(task(j, "https://a.com/x", 2)), true, 10), "deeper");
        assertTrue(store().isTaskOpen(j, t.key()));
        assertEquals(1, store().stats(j).pending());
        assertEquals(1, store().stats(j).discovered());

        JobPage p = store().page(j, t.url().hash()).orElseThrow();
        assertEquals(PageStatus.QUEUED, p.status());
        assertEquals(1, p.depth());
        assertEquals("https://a.com/x", p.url());
        assertTrue(store().page(j, url("https://a.com/never").hash()).isEmpty());
    }

    @Test void aShorterPathLowersTheDepthAndUpdatesNeverRaiseIt() {
        String j = job();
        CrawlTask deep = task(j, "https://a.com/x", 3);
        store().admit(JobPage.queued(deep), true, 10);
        CrawlTask shallow = new CrawlTask(j, deep.url(), 1, url("https://a.com/p").hash(), ResourceType.PAGE, 0, 0);
        assertEquals(Admission.SHALLOWER, store().admit(JobPage.queued(shallow), true, 10));
        assertEquals(1, store().page(j, deep.url().hash()).orElseThrow().depth());
        assertEquals(1, store().stats(j).pending(), "no new task: the queued one runs at the new depth");

        // the worker writes back the copy it read at depth 3: the status lands, the depth stays 1
        store().update(JobPage.queued(deep).withContent(PageStatus.FETCHED, ResourceType.PAGE,
                snapshot(deep, "text/html", Instant.now())));
        JobPage after = store().page(j, deep.url().hash()).orElseThrow();
        assertEquals(PageStatus.FETCHED, after.status());
        assertEquals(1, after.depth());
        assertEquals(shallow.parentHash(), after.parentHash(), "the parent on the shorter path");

        assertEquals(Admission.SEEN, store().admit(JobPage.queued(task(j, "https://a.com/x", 2)), true, 10));
        assertEquals(Admission.SHALLOWER, store().admit(JobPage.queued(task(j, "https://a.com/x", 0)), true, 10));
        assertEquals(0, store().page(j, deep.url().hash()).orElseThrow().depth());
        assertEquals(PageStatus.FETCHED, store().page(j, deep.url().hash()).orElseThrow().status(),
                "lowering the depth doesn't touch the status a worker wrote");
    }

    @Test void aReferencedAssetCanBeUpgradedToATask() {
        String j = job();
        CrawlTask img = asset(j, "https://cdn.com/a.png");
        assertEquals(Admission.ADMITTED, store().admit(JobPage.referenced(img), false, 10));
        assertEquals(Admission.SEEN, store().admit(JobPage.referenced(img), false, 10));
        assertEquals(0, store().stats(j).pending(), "a referenced node is not work");
        assertEquals(PageStatus.REFERENCED, store().page(j, img.url().hash()).orElseThrow().status());

        assertEquals(Admission.ADMITTED, store().admit(JobPage.queued(img), true, 10), "now it is downloaded");
        assertEquals(PageStatus.QUEUED, store().page(j, img.url().hash()).orElseThrow().status());
        assertEquals(1, store().stats(j).pending());
        assertEquals(1, store().stats(j).discovered(), "still one node");
        assertEquals(1, store().pages(j, null, null, 10).items().size(), "listed once");
        assertEquals(Admission.SEEN, store().admit(JobPage.queued(img), true, 10));
    }

    @Test void budgetsArePerKindAndOverBudgetAdmitsNothing() {
        String j = job();
        assertEquals(Admission.ADMITTED, store().admit(JobPage.queued(task(j, "https://a.com/1", 1)), true, 2));
        assertEquals(Admission.ADMITTED, store().admit(JobPage.queued(task(j, "https://a.com/2", 1)), true, 2));
        assertEquals(Admission.OVER_BUDGET, store().admit(JobPage.queued(task(j, "https://a.com/3", 1)), true, 2));
        assertEquals(Admission.ADMITTED, store().admit(JobPage.referenced(asset(j, "https://cdn.com/1.png")), false, 1),
                "assets have their own budget");
        assertEquals(Admission.OVER_BUDGET, store().admit(JobPage.referenced(asset(j, "https://cdn.com/2.png")), false, 1));
        assertTrue(store().page(j, url("https://a.com/3").hash()).isEmpty());
        assertEquals(3, store().stats(j).discovered());
        assertEquals(2, store().stats(j).pending());
    }

    @Test void concurrentDiscoveryOfOneUrlAdmitsItOnce() throws Exception {
        String j = job();
        int threads = 16;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            for (int round = 0; round < 20; round++) {
                CrawlTask t = task(j, "https://a.com/race" + round, 1);
                CountDownLatch go = new CountDownLatch(1);
                List<Future<Admission>> out = new ArrayList<>();
                for (int i = 0; i < threads; i++) out.add(pool.submit(() -> {
                    go.await();
                    return store().admit(JobPage.queued(t), true, 1000);
                }));
                go.countDown();
                int admitted = 0;
                for (Future<Admission> f : out) if (f.get() == Admission.ADMITTED) admitted++;
                assertEquals(1, admitted, "round " + round);
            }
        } finally {
            pool.shutdownNow();
        }
        assertEquals(20, store().stats(j).pending());
        assertEquals(20, store().stats(j).discovered());
    }

    // ------------------------------------------------------------------ exactly-once accounting

    @Test void tasksOpenAndCloseOncePerKey() {
        String j = job();
        assertTrue(store().openTask(j, "k#1#0"));
        assertFalse(store().openTask(j, "k#1#0"), "a redelivered retry doesn't open twice");
        assertTrue(store().openTask(j, "k#1#1"));
        assertEquals(2, store().stats(j).pending());
        assertFalse(store().isTaskOpen(j, "never"));

        assertEquals(1, store().closeTask(j, "k#1#0").orElseThrow());
        assertTrue(store().closeTask(j, "k#1#0").isEmpty(), "a duplicate completion is not counted");
        assertFalse(store().isTaskOpen(j, "k#1#0"));
        assertFalse(store().openTask(j, "k#1#0"), "a closed key stays closed");
        assertTrue(store().closeTask(j, "never").isEmpty());

        store().incrementPending(j);
        assertEquals(1, store().decrementPending(j));
        assertEquals(0, store().closeTask(j, "k#1#1").orElseThrow());
    }

    @Test void concurrentCompletionsOfOneTaskCountOnce() throws Exception {
        String j = job();
        for (int i = 0; i < 50; i++) store().openTask(j, "t" + i);
        ExecutorService pool = Executors.newFixedThreadPool(8);
        AtomicInteger closed = new AtomicInteger();
        ConcurrentLinkedQueue<Long> zeros = new ConcurrentLinkedQueue<>();
        try {
            List<Future<?>> fs = new ArrayList<>();
            for (int copy = 0; copy < 4; copy++)                 // every task delivered four times
                for (int i = 0; i < 50; i++) {
                    String key = "t" + i;
                    fs.add(pool.submit(() -> store().closeTask(j, key).ifPresent(left -> {
                        closed.incrementAndGet();
                        if (left == 0) zeros.add(left);
                    })));
                }
            for (Future<?> f : fs) f.get();
        } finally {
            pool.shutdownNow();
        }
        assertEquals(50, closed.get());
        assertEquals(1, zeros.size(), "exactly one caller sees pending reach zero, so the job completes once");
        assertEquals(0, store().stats(j).pending());
    }

    // ------------------------------------------------------------------ content, scope, traps

    @Test void contentHasOneOwnerPerJob() {
        String j = job();
        assertTrue(store().claimContent(j, "sha1|/a/", "u1").isEmpty(), "first claim owns it");
        assertTrue(store().claimContent(j, "sha1|/a/", "u1").isEmpty(), "the owner again (a redelivery)");
        assertEquals("u1", store().claimContent(j, "sha1|/a/", "u2").orElseThrow());
        assertTrue(store().claimContent(j, "sha1|/b/", "u2").isEmpty());
        assertTrue(store().claimContent(job(), "sha1|/a/", "u2").isEmpty(), "per job");
    }

    @Test void scopeVariantsAndTruncation() {
        String j = job();
        store().addScopeKey(j, "a.com");
        assertTrue(store().hasScopeKey(j, "a.com"));
        assertFalse(store().hasScopeKey(j, "b.com"));
        assertEquals(1, store().countVariant(j, "a.com/p"));
        assertEquals(2, store().countVariant(j, "a.com/p"));
        assertEquals(1, store().countVariant(j, "a.com/q"));
        store().markTruncated(j);
        store().markTruncated(j);
        assertEquals(2, store().stats(j).truncated());
    }

    // ------------------------------------------------------------------ graph

    @Test void statsCountEachNodeByItsLatestStatus() {
        String j = job();
        Instant now = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        CrawlTask p1 = task(j, "https://a.com/1", 1), p2 = task(j, "https://a.com/2", 1);
        CrawlTask p3 = task(j, "https://a.com/3", 1), img = asset(j, "https://a.com/i.png");
        for (CrawlTask t : List.of(p1, p2, p3, img)) store().admit(JobPage.queued(t), true, 100);

        store().update(JobPage.queued(p1).withContent(PageStatus.FETCHED, ResourceType.PAGE, snapshot(p1, "text/html", now)));
        store().update(JobPage.queued(p1).withContent(PageStatus.FETCHED, ResourceType.PAGE, snapshot(p1, "text/html", now)));
        store().update(JobPage.queued(p2).withContent(PageStatus.REUSED, ResourceType.PAGE, snapshot(p2, "text/html", now))
                .duplicateOf(p1.url().hash()));
        store().update(JobPage.queued(p3).failed(PageStatus.TOO_LARGE, 200, "big", now));
        store().update(JobPage.queued(img).withContent(PageStatus.REUSED, ResourceType.IMAGE, snapshot(img, "image/png", now)));

        JobStats s = store().stats(j);
        assertEquals(4, s.discovered());
        assertEquals(1, s.pages(), "p1; p2 became a DUPLICATE");
        assertEquals(1, s.assets());
        assertEquals(1, s.reused(), "img (p2's REUSED was replaced by DUPLICATE)");
        assertEquals(1, s.duplicates());
        assertEquals(1, s.failed());

        JobPage dup = store().page(j, p2.url().hash()).orElseThrow();
        assertEquals(PageStatus.DUPLICATE, dup.status());
        assertEquals(p1.url().hash(), dup.duplicateOf());
        JobPage fetched = store().page(j, p1.url().hash()).orElseThrow();
        assertEquals("text/html", fetched.contentType());
        assertEquals(42, fetched.size());
        assertEquals(now, fetched.fetchedAt());
        assertEquals("big", store().page(j, p3.url().hash()).orElseThrow().error());
    }

    @Test void edgesAreKeptPerSource() {
        String j = job();
        CrawlTask from = task(j, "https://a.com/", 0);
        LinkEdge e1 = new LinkEdge(j, from.url().hash(), url("https://a.com/x").hash(), "https://a.com/x", ResourceType.PAGE, "/x");
        LinkEdge e2 = new LinkEdge(j, from.url().hash(), url("https://a.com/x").hash(), "https://a.com/x", ResourceType.PAGE, "x");
        LinkEdge e3 = new LinkEdge(j, from.url().hash(), url("https://a.com/i.png").hash(), "https://a.com/i.png", ResourceType.IMAGE, "i.png");
        for (LinkEdge e : List.of(e1, e2, e3)) store().addLink(e);

        assertEquals(Set.of(e1, e2, e3), Set.copyOf(store().outLinks(j, from.url().hash())),
                "two spellings of one target are two edges: the rewriter needs each raw href");
        assertTrue(store().outLinks(j, "nothing").isEmpty());
        assertEquals(3, store().stats(j).edges());
    }

    @Test void pagesComeInDiscoveryOrderWithAStableCursor() {
        String j = job();
        List<String> order = new ArrayList<>();
        for (int i = 0; i < 25; i++) {
            CrawlTask t = i % 5 == 4 ? asset(j, "https://a.com/" + i + ".png") : task(j, "https://a.com/" + i, 1);
            store().admit(i % 5 == 4 ? JobPage.referenced(t) : JobPage.queued(t), i % 5 != 4, 100);
            order.add(t.url().hash());
        }
        List<String> all = new ArrayList<>();
        String cursor = null;
        int calls = 0;
        do {
            Slice<JobPage> s = store().pages(j, null, cursor, 7);
            assertTrue(s.items().size() <= 7);
            s.items().forEach(p -> all.add(p.urlHash()));
            cursor = s.nextCursor();
            calls++;
        } while (cursor != null && calls < 20);
        assertEquals(order, all, "BFS order, every node once");

        List<String> images = new ArrayList<>();
        cursor = null;
        calls = 0;
        do {
            Slice<JobPage> s = store().pages(j, ResourceType.IMAGE, cursor, 2);
            s.items().forEach(p -> images.add(p.urlHash()));
            cursor = s.nextCursor();
            calls++;
        } while (cursor != null && calls < 30);
        assertEquals(order.stream().filter(h -> order.indexOf(h) % 5 == 4).collect(Collectors.toList()), images);
        assertNull(store().pages(job(), null, null, 10).nextCursor(), "an empty job has one empty page");
    }

    @Test void latestForTenantIsTheNewestContentAcrossItsJobs() {
        String j1 = job(), j2 = job();
        Instant t0 = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        CrawlTask a = task(j1, "https://a.com/x", 0), b = task(j2, "https://a.com/x", 0);
        store().admit(JobPage.queued(a), true, 10);
        store().admit(JobPage.queued(b), true, 10);
        store().update(JobPage.queued(b).withContent(PageStatus.FETCHED, ResourceType.PAGE, snapshot(b, "text/html", t0.plusSeconds(5))));
        store().update(JobPage.queued(a).withContent(PageStatus.FETCHED, ResourceType.PAGE, snapshot(a, "text/html", t0)));

        JobPage latest = store().latestForTenant(tenant, a.url().hash()).orElseThrow();
        assertEquals(j2, latest.jobId(), "the later fetch wins even though it was written first");
        assertTrue(store().latestForTenant("someone-else", a.url().hash()).isEmpty(), "tenants never see each other");

        CrawlTask failed = task(j1, "https://a.com/f", 0);
        store().admit(JobPage.queued(failed), true, 10);
        store().update(JobPage.queued(failed).failed(PageStatus.FAILED, 500, "x", t0));
        assertTrue(store().latestForTenant(tenant, failed.url().hash()).isEmpty(), "only nodes with content");
    }
}
