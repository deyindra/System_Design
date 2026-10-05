package com.salesforce.einstein.webcrawler;

import com.salesforce.einstein.webcrawler.engine.CrawlEngine;
import com.salesforce.einstein.webcrawler.engine.EngineConfig;
import com.salesforce.einstein.webcrawler.fetch.FetchResult;
import com.salesforce.einstein.webcrawler.fetch.Fetcher;
import com.salesforce.einstein.webcrawler.frontier.Frontier;
import com.salesforce.einstein.webcrawler.frontier.PartitionedFrontier;
import com.salesforce.einstein.webcrawler.model.CrawlJob;
import com.salesforce.einstein.webcrawler.model.CrawlRequest;
import com.salesforce.einstein.webcrawler.model.JobPage;
import com.salesforce.einstein.webcrawler.model.JobStatus;
import com.salesforce.einstein.webcrawler.model.PageStatus;
import com.salesforce.einstein.webcrawler.model.Scope;
import com.salesforce.einstein.webcrawler.store.InMemoryContentStore;
import com.salesforce.einstein.webcrawler.store.InMemoryJobStore;
import com.salesforce.einstein.webcrawler.store.InMemoryPageStore;
import com.salesforce.einstein.webcrawler.store.JobStore;
import com.salesforce.einstein.webcrawler.url.ParamRules;
import com.salesforce.einstein.webcrawler.url.TrapDetector;
import com.salesforce.einstein.webcrawler.url.UrlNormalizer;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Several engine nodes, one job. The nodes share only what production shares (the job store, the snapshot and
 * content stores, the partitioned frontier); each has its own workers, robots cache and Bloom filter.
 */
class DistributedCrawlTest {

    private final Clock clock = Clock.systemUTC();
    private final FakeWeb web = new FakeWeb();
    private final JobStore jobs = new InMemoryJobStore();
    private final InMemoryPageStore pages = new InMemoryPageStore();
    private final InMemoryContentStore contents = new InMemoryContentStore();
    private final UrlNormalizer normalizer = new UrlNormalizer(new ParamRules());

    /** A node; the test closes it. */
    private CrawlEngine node(Frontier frontier, Fetcher fetcher, Duration politeness) {
        EngineConfig cfg = EngineConfig.defaults().withWorkers(4).withPoliteness(politeness)
                .withRetryBackoff(Duration.ofMillis(1));
        return new CrawlEngine(cfg, frontier, jobs, pages, contents, fetcher, normalizer, TrapDetector.defaults(), clock);
    }

    private static String links(String... hrefs) {
        StringBuilder sb = new StringBuilder("<html><body>");
        for (String h : hrefs) sb.append("<a href=\"").append(h).append("\">x</a>");
        return sb.append("</body></html>").toString();
    }

    private JobPage node(CrawlJob job, String url) {
        return jobs.page(job.jobId(), normalizer.normalize(url).orElseThrow().hash()).orElseThrow();
    }

    // ---------------------------------------------------------------- assignment

    @Test void addingANodeMovesOnlyItsShare() {
        PartitionedFrontier f = new PartitionedFrontier(64, clock);
        for (int i = 1; i <= 4; i++) f.join("node-" + i);
        Map<Integer, String> before = owners(f.assignment());
        f.join("node-5");
        Map<Integer, String> after = owners(f.assignment());

        int moved = 0;
        for (int p = 0; p < 64; p++) {
            if (before.get(p).equals(after.get(p))) continue;
            moved++;
            assertEquals("node-5", after.get(p), "a partition only ever moves to the new node");
        }
        assertTrue(moved >= 5 && moved <= 22, "about 1/5 of the partitions move, not a reshuffle: " + moved);
    }

    private static Map<Integer, String> owners(Map<String, Set<Integer>> assignment) {
        Map<Integer, String> out = new HashMap<>();
        assignment.forEach((n, ps) -> ps.forEach(p -> out.put(p, n)));
        return out;
    }

    // ---------------------------------------------------------------- politeness across nodes

    @Test void eachHostIsCrawledByOneNodeAtATime() throws Exception {
        List<String> hosts = IntStream.range(0, 8).mapToObj(i -> "https://h" + i + ".com").toList();
        web.page("https://hub.com/", links(hosts.stream().map(h -> h + "/").toArray(String[]::new)));
        for (String h : hosts) {
            web.page(h + "/", links("/1", "/2", "/3", "/4"));
            for (int i = 1; i <= 4; i++) web.page(h + "/" + i, links("/", "/" + i));   // distinct bytes per page
        }
        Map<String, Set<String>> nodesPerHost = new ConcurrentHashMap<>();
        Map<String, AtomicInteger> active = new ConcurrentHashMap<>();
        AtomicBoolean overlap = new AtomicBoolean();

        Function<String, Fetcher> watched = id -> req -> {        // records which node fetched each host, and overlaps
            String host = req.url().getHost();
            nodesPerHost.computeIfAbsent(host, k -> ConcurrentHashMap.newKeySet()).add(id);
            AtomicInteger a = active.computeIfAbsent(host, k -> new AtomicInteger());
            if (a.incrementAndGet() > 1) overlap.set(true);
            try {
                Thread.sleep(2);
                return web.fetch(req);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return FetchResult.status(503);
            } finally {
                a.decrementAndGet();
            }
        };
        PartitionedFrontier cluster = new PartitionedFrontier(16, clock);
        Duration politeness = Duration.ofMillis(5);

        try (CrawlEngine n0 = node(cluster.join("node-0"), watched.apply("node-0"), politeness);
             CrawlEngine ignored = node(cluster.join("node-1"), watched.apply("node-1"), politeness);
             CrawlEngine n2 = node(cluster.join("node-2"), watched.apply("node-2"), politeness)) {
            CrawlJob job = n0.submit(CrawlRequest.builder("t1", "https://hub.com/")
                    .scope(Scope.ANY).maxDepth(2).respectRobots(false).build(), null);
            CrawlJob done = n2.await(job.jobId(), Duration.ofSeconds(20));   // waits on another node

            assertEquals(JobStatus.COMPLETED, done.status());
            assertEquals(1 + 8 * 5, jobs.stats(job.jobId()).pages());
            assertEquals(1 + 8 * 5, web.totalHitsExcludingRobots(), "every page fetched exactly once, cluster-wide");
            nodesPerHost.forEach((h, ns) -> assertEquals(1, ns.size(), h + " was fetched by " + ns));
            assertTrue(nodesPerHost.values().stream().flatMap(Set::stream).distinct().count() > 1, "work was spread");
            assertFalse(overlap.get(), "no host had two requests in flight");
            assertEquals(0, cluster.backlog());
        }
    }

    // ---------------------------------------------------------------- failure

    @Test void aNodeDyingMidFetchLosesNoWorkAndCountsNothingTwice() throws Exception {
        PartitionedFrontier cluster = new PartitionedFrontier(16, clock);
        Frontier fa = cluster.join("node-a");
        Frontier fb = cluster.join("node-b");
        Set<Integer> bParts = cluster.assignment().get("node-b");
        String seedHost = IntStream.range(0, 100).mapToObj(i -> "s" + i + ".com")
                .filter(h -> bParts.contains(cluster.partitionOf(h))).findFirst().orElseThrow();
        String seed = "https://" + seedHost + "/";
        web.page(seed, links("/x", "/y")).page(seed + "x", links("/", "/x")).page(seed + "y", links("/", "/y"));

        CountDownLatch bHolding = new CountDownLatch(1);
        CountDownLatch bResume = new CountDownLatch(1);
        Fetcher stuck = req -> {                       // node-b hangs on its first fetch (a GC pause, a partition)
            bHolding.countDown();
            try {
                assertTrue(bResume.await(10, TimeUnit.SECONDS));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return web.fetch(req);
        };
        try (CrawlEngine a = node(fa, web, Duration.ZERO); CrawlEngine b = node(fb, stuck, Duration.ZERO)) {
            CrawlJob job = b.submit(CrawlRequest.builder("t1", seed).maxDepth(1).respectRobots(false).build(), null);
            assertTrue(bHolding.await(5, TimeUnit.SECONDS));
            cluster.leave("node-b");                   // missed heartbeats: its partitions and the seed move to node-a

            CrawlJob done = a.await(job.jobId(), Duration.ofSeconds(10));
            assertEquals(JobStatus.COMPLETED, done.status(), "node-a finished the crawl from the replayed seed");
            assertEquals(3, jobs.stats(job.jobId()).pages());

            bResume.countDown();                       // the zombie wakes up and finishes its copy of the seed
            b.close();
            Thread.sleep(100);
            assertEquals(0, jobs.stats(job.jobId()).pending(), "the duplicate completion was not counted");
            assertEquals(JobStatus.COMPLETED, jobs.get(job.jobId()).orElseThrow().status());
            assertEquals(1, web.hits(seed + "x"), "children admitted once, whoever expanded the seed");
        }
    }

    // ---------------------------------------------------------------- graph: depth is a shortest path

    /** The short path (via https://b.com/) arrives while {@code target} is being fetched on the long one. */
    private Fetcher bExpandedWhileFetching(String target) {
        CountDownLatch targetStarted = new CountDownLatch(1);
        CountDownLatch bExpanded = new CountDownLatch(1);
        return req -> {
            String u = req.url().toString();
            try {
                if (u.equals("https://b.com/")) {
                    assertTrue(targetStarted.await(10, TimeUnit.SECONDS));
                    new Thread(() -> {                 // B's links are followed right after this fetch returns
                        try { Thread.sleep(50); } catch (InterruptedException ignored) { }
                        bExpanded.countDown();
                    }).start();
                }
                if (u.equals(target)) {
                    targetStarted.countDown();
                    assertTrue(bExpanded.await(10, TimeUnit.SECONDS));
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return web.fetch(req);
        };
    }

    /** The short path (via https://b.com/) arrives only after {@code target} was fetched and expanded on the long one. */
    private Fetcher bAfterExpanding(String target) {
        CountDownLatch targetFetched = new CountDownLatch(1);
        return req -> {
            String u = req.url().toString();
            if (u.equals("https://b.com/")) {
                try {
                    assertTrue(targetFetched.await(10, TimeUnit.SECONDS));
                    Thread.sleep(50);                  // let the target's expansion finish on the long path
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            FetchResult r = web.fetch(req);
            if (u.equals(target)) targetFetched.countDown();
            return r;
        };
    }

    /**
     * Seed S links to a slow page B and to a chain A1 → A2 → A3. Both reach T, which links to U. With maxDepth 4,
     * T is first reached via the chain at depth 4, so U (depth 5) would be cut. B answers while T is being fetched:
     * T is really at depth 2, so U is at 3 and must be crawled. Without relaxation, discovery order would decide.
     */
    @Test void aShorterPathFoundWhileFetchingLowersDepthAndReachesTheChildren() throws Exception {
        web.page("https://s.com/", links("https://b.com/", "https://a.com/1"))
           .page("https://a.com/1", links("/2")).page("https://a.com/2", links("/3"))
           .page("https://a.com/3", links("https://t.com/"))
           .page("https://b.com/", links("https://t.com/"))
           .page("https://t.com/", links("/u"))
           .page("https://t.com/u", links("/"));
        Fetcher slowB = bExpandedWhileFetching("https://t.com/");
        PartitionedFrontier cluster = new PartitionedFrontier(8, clock);
        try (CrawlEngine e1 = node(cluster.join("node-1"), slowB, Duration.ZERO);
             CrawlEngine ignored = node(cluster.join("node-2"), slowB, Duration.ZERO)) {
            CrawlJob job = e1.submit(CrawlRequest.builder("t1", "https://s.com/").scope(Scope.ANY).maxDepth(4)
                    .respectRobots(false).build(), null);
            assertEquals(JobStatus.COMPLETED, e1.await(job.jobId(), Duration.ofSeconds(15)).status());

            assertEquals(2, node(job, "https://t.com/").depth(), "depth is the shortest path, not the first one found");
            assertEquals(3, node(job, "https://t.com/u").depth());
            assertEquals(PageStatus.FETCHED, node(job, "https://t.com/u").status());
            assertEquals(1, web.hits("https://t.com/"), "relaxation re-expands from stored bytes, never refetches");
        }
    }

    @Test void aPageProcessedAtTheWrongDepthIsReExpanded() throws Exception {
        // Like above, but T is shallow enough to be fetched on the long path (depth 3 of max 3), so U is cut;
        // then the short path (depth 2) arrives after T was already expanded.
        web.page("https://s.com/", links("https://b.com/", "https://a.com/1"))
           .page("https://a.com/1", links("/2")).page("https://a.com/2", links("https://t.com/"))
           .page("https://b.com/", links("https://t.com/"))
           .page("https://t.com/", links("/u"))
           .page("https://t.com/u", links("/"));
        Fetcher slowB = bAfterExpanding("https://t.com/");
        PartitionedFrontier cluster = new PartitionedFrontier(8, clock);
        try (CrawlEngine e1 = node(cluster.join("node-1"), slowB, Duration.ZERO);
             CrawlEngine ignored = node(cluster.join("node-2"), slowB, Duration.ZERO)) {
            CrawlJob job = e1.submit(CrawlRequest.builder("t1", "https://s.com/").scope(Scope.ANY).maxDepth(3)
                    .respectRobots(false).build(), null);
            assertEquals(JobStatus.COMPLETED, e1.await(job.jobId(), Duration.ofSeconds(15)).status());

            assertEquals(2, node(job, "https://t.com/").depth());
            assertEquals(PageStatus.FETCHED, node(job, "https://t.com/u").status(), "U was reached once T got closer");
            assertEquals(1, web.hits("https://t.com/"));
        }
    }

    /**
     * The shorter path arrives while R is being fetched, and R turns out to be a redirect. The discoverer sees R still
     * QUEUED (nothing to relax yet), so R's own worker must notice the lower depth after writing REDIRECT, or the
     * target keeps the long path's depth and its children are cut.
     */
    @Test void aRedirectWhoseDepthDropsMidFetchPassesItToTheTarget() throws Exception {
        web.page("https://s.com/", links("https://b.com/", "https://a.com/1"))
           .page("https://a.com/1", links("/2")).page("https://a.com/2", links("https://r.com/"))
           .redirect("https://r.com/", "https://t.com/")
           .page("https://b.com/", links("https://r.com/"))
           .page("https://t.com/", links("/u"))
           .page("https://t.com/u", links("/"));
        Fetcher slowB = bExpandedWhileFetching("https://r.com/");
        PartitionedFrontier cluster = new PartitionedFrontier(8, clock);
        try (CrawlEngine e1 = node(cluster.join("node-1"), slowB, Duration.ZERO);
             CrawlEngine ignored = node(cluster.join("node-2"), slowB, Duration.ZERO)) {
            CrawlJob job = e1.submit(CrawlRequest.builder("t1", "https://s.com/").scope(Scope.ANY).maxDepth(3)
                    .respectRobots(false).build(), null);
            assertEquals(JobStatus.COMPLETED, e1.await(job.jobId(), Duration.ofSeconds(15)).status());

            assertEquals(2, node(job, "https://r.com/").depth());
            assertEquals(2, node(job, "https://t.com/").depth(), "a redirect is not a hop: the target gets R's new depth");
            assertEquals(PageStatus.FETCHED, node(job, "https://t.com/u").status());
        }
    }
}
