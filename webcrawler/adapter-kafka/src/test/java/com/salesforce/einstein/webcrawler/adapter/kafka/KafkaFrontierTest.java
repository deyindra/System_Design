package com.salesforce.einstein.webcrawler.adapter.kafka;

import com.salesforce.einstein.webcrawler.FakeWeb;
import com.salesforce.einstein.webcrawler.engine.CrawlEngine;
import com.salesforce.einstein.webcrawler.engine.EngineConfig;
import com.salesforce.einstein.webcrawler.fetch.FetchResult;
import com.salesforce.einstein.webcrawler.fetch.Fetcher;
import com.salesforce.einstein.webcrawler.frontier.HostSchedule;
import com.salesforce.einstein.webcrawler.frontier.InMemoryHostSchedule;
import com.salesforce.einstein.webcrawler.model.CrawlJob;
import com.salesforce.einstein.webcrawler.model.CrawlRequest;
import com.salesforce.einstein.webcrawler.model.JobStatus;
import com.salesforce.einstein.webcrawler.model.Scope;
import com.salesforce.einstein.webcrawler.sitemap.InMemorySitemapGraphs;
import com.salesforce.einstein.webcrawler.store.InMemoryContentStore;
import com.salesforce.einstein.webcrawler.store.InMemoryJobStore;
import com.salesforce.einstein.webcrawler.store.InMemoryPageStore;
import com.salesforce.einstein.webcrawler.store.JobStore;
import com.salesforce.einstein.webcrawler.url.ParamRules;
import com.salesforce.einstein.webcrawler.url.TrapDetector;
import com.salesforce.einstein.webcrawler.url.UrlNormalizer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.IntStream;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The distributed scenarios of {@code DistributedCrawlTest}, on a real broker: separate consumers in one group, each
 * with its own engine, sharing only the log and the job store.
 */
@Testcontainers(disabledWithoutDocker = true)
class KafkaFrontierTest {

    @Container
    static final ConfluentKafkaContainer KAFKA = new ConfluentKafkaContainer("confluentinc/cp-kafka:7.6.0");

    private static final int PARTITIONS = 8;
    /** Short, so a survivor that respects a dead node's lease on a host doesn't stretch the test. */
    private static final Duration LEASE = Duration.ofSeconds(3);

    private final Clock clock = Clock.systemUTC();
    private final FakeWeb web = new FakeWeb();
    private final JobStore jobs = new InMemoryJobStore();
    private final InMemoryPageStore pages = new InMemoryPageStore();
    private final InMemoryContentStore contents = new InMemoryContentStore();
    private final UrlNormalizer normalizer = new UrlNormalizer(new ParamRules());
    private final HostSchedule schedule = new InMemoryHostSchedule();
    private final String topic = "frontier-" + UUID.randomUUID();
    private final List<CrawlEngine> engines = new ArrayList<>();
    private final List<KafkaFrontier> frontiers = new ArrayList<>();

    /** Whatever a test didn't close itself: engines first, then the frontiers they poll. */
    @AfterEach void stop() {
        engines.forEach(CrawlEngine::close);
        frontiers.forEach(KafkaFrontier::close);
    }

    private KafkaFrontier frontier(String id) {
        KafkaFrontier f = new KafkaFrontier(KafkaFrontierSettings.of(KAFKA.getBootstrapServers(), id)
                .withTopic(topic, PARTITIONS).withGroup("g-" + topic).withFetchLease(LEASE), schedule, clock);
        frontiers.add(f);
        return f;
    }

    private CrawlEngine engine(KafkaFrontier f, Fetcher fetcher, Duration politeness) {
        EngineConfig cfg = EngineConfig.defaults().withWorkers(4).withPoliteness(politeness)
                .withRetryBackoff(Duration.ofMillis(1));
        CrawlEngine e = new CrawlEngine(cfg, f, jobs, pages, contents, fetcher, normalizer, TrapDetector.defaults(),
                new InMemorySitemapGraphs(4), clock);
        engines.add(e);
        return e;
    }

    /** Every partition owned, by everyone given, and no rebalance in flight: balanced for a second without change. */
    private static void awaitBalanced(KafkaFrontier... fs) {
        await("the group settles").atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofMillis(100))
                .during(Duration.ofSeconds(1)).until(() -> balanced(fs));
    }

    private static boolean balanced(KafkaFrontier... fs) {
        Set<Integer> all = new HashSet<>();
        int total = 0;
        for (KafkaFrontier f : fs) {
            Set<Integer> p = f.partitions();
            if (p.isEmpty()) return false;
            all.addAll(p);
            total += p.size();
        }
        return all.size() == PARTITIONS && total == PARTITIONS;
    }

    private static String links(String... hrefs) {
        StringBuilder sb = new StringBuilder("<html><body>");
        for (String h : hrefs) sb.append("<a href=\"").append(h).append("\">x</a>");
        return sb.append("</body></html>").toString();
    }

    // ---------------------------------------------------------------- spread, politeness, exactly-once

    @Test void hostsAreSpreadOverNodesAndEachIsCrawledByOneNodeOnce() throws Exception {
        List<String> hosts = IntStream.range(0, 8).mapToObj(i -> "https://h" + i + ".com").toList();
        web.page("https://hub.com/", links(hosts.stream().map(h -> h + "/").toArray(String[]::new)));
        for (String h : hosts) {
            web.page(h + "/", links("/1", "/2", "/3"));
            for (int i = 1; i <= 3; i++) web.page(h + "/" + i, links("/", "/" + i));
        }
        Map<String, Set<String>> nodesPerHost = new ConcurrentHashMap<>();
        Map<String, AtomicInteger> active = new ConcurrentHashMap<>();
        AtomicBoolean overlap = new AtomicBoolean();

        Function<String, Fetcher> watched = id -> req -> {          // records which node fetched each host, and overlaps
            String host = req.url().getHost();
            nodesPerHost.computeIfAbsent(host, k -> ConcurrentHashMap.newKeySet()).add(id);
            AtomicInteger a = active.computeIfAbsent(host, k -> new AtomicInteger());
            if (a.incrementAndGet() > 1) overlap.set(true);
            try {
                return web.fetch(req);
            } finally {
                a.decrementAndGet();
            }
        };

        try (KafkaFrontier f0 = frontier("node-0"); KafkaFrontier f1 = frontier("node-1");
             KafkaFrontier f2 = frontier("node-2")) {
            KafkaFrontier[] fs = {f0, f1, f2};
            awaitBalanced(fs);
            Duration politeness = Duration.ofMillis(5);
            try (CrawlEngine e0 = engine(f0, watched.apply("node-0"), politeness);
                 CrawlEngine e1 = engine(f1, watched.apply("node-1"), politeness);
                 CrawlEngine e2 = engine(f2, watched.apply("node-2"), politeness)) {
                CrawlJob job = e0.submit(CrawlRequest.builder("t1", "https://hub.com/")
                        .scope(Scope.ANY).maxDepth(2).respectRobots(false).build(), null);
                CrawlJob done = e2.await(job.jobId(), Duration.ofSeconds(60));

                assertEquals(JobStatus.COMPLETED, done.status());
                assertEquals(1 + 8 * 4, e1.jobs().stats(job.jobId()).pages(), "read on a third node");
                assertEquals(1 + 8 * 4, web.totalHitsExcludingRobots(), "every page fetched exactly once, cluster-wide");
                nodesPerHost.forEach((h, ns) -> assertEquals(1, ns.size(), h + " was fetched by " + ns));
                assertTrue(nodesPerHost.values().stream().flatMap(Set::stream).distinct().count() > 1, "work was spread");
                assertFalse(overlap.get(), "no host had two requests in flight");
                for (String h : hosts) {
                    String host = h.substring("https://".length());
                    String owner = nodesPerHost.get(host).iterator().next();
                    int partition = f0.partitionOf(host);
                    assertTrue(fs[Integer.parseInt(owner.substring(5))].partitions().contains(partition),
                            host + " was crawled by the owner of its partition");
                }
            }
        }
    }

    // ---------------------------------------------------------------- failure

    @Test void aNodeDyingMidFetchLosesNoWorkAndCountsNothingTwice() throws Exception {
        try (KafkaFrontier fa = frontier("node-a"); KafkaFrontier fb = frontier("node-b")) {
            awaitBalanced(fa, fb);
            Set<Integer> bParts = fb.partitions();
            String seedHost = IntStream.range(0, 100).mapToObj(i -> "s" + i + ".com")
                    .filter(h -> bParts.contains(fb.partitionOf(h))).findFirst().orElseThrow();
            String seed = "https://" + seedHost + "/";
            web.page(seed, links("/x", "/y")).page(seed + "x", links("/", "/x")).page(seed + "y", links("/", "/y"));

            CountDownLatch bHolding = new CountDownLatch(1);
            CountDownLatch bResume = new CountDownLatch(1);
            Fetcher stuck = req -> {                       // node-b hangs on its first fetch (a GC pause, a partition)
                bHolding.countDown();
                try {
                    assertTrue(bResume.await(60, TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return FetchResult.status(503);
                }
                return web.fetch(req);
            };

            try (CrawlEngine a = engine(fa, web, Duration.ZERO); CrawlEngine b = engine(fb, stuck, Duration.ZERO)) {
                CrawlJob job = b.submit(CrawlRequest.builder("t1", seed).maxDepth(1).respectRobots(false).build(), null);
                assertTrue(bHolding.await(30, TimeUnit.SECONDS));
                fb.kill();                                 // leaves the group, its seed uncommitted

                CrawlJob done = a.await(job.jobId(), Duration.ofSeconds(60));
                assertEquals(JobStatus.COMPLETED, done.status(), "node-a finished the crawl from the replayed seed");
                assertEquals(3, jobs.stats(job.jobId()).pages());
                assertTrue(fa.partitions().contains(fa.partitionOf(seedHost)));

                bResume.countDown();                       // the zombie wakes up and finishes its copy of the seed
                b.close();
                Thread.sleep(200);
                assertEquals(0, jobs.stats(job.jobId()).pending(), "the duplicate completion was not counted");
                assertEquals(JobStatus.COMPLETED, jobs.get(job.jobId()).orElseThrow().status());
                assertEquals(1, web.hits(seed + "x"), "children admitted once, whoever expanded the seed");
                assertTrue(Duration.between(job.createdAt(), done.finishedAt()).compareTo(LEASE) >= 0,
                        "node-a waited out node-b's fetch lease before contacting the host the zombie was still fetching");
            }
        }
    }

    @Test void aHostsPolitenessDelaySurvivesItsPartitionMoving() throws Exception {
        try (KafkaFrontier fa = frontier("node-a"); KafkaFrontier fb = frontier("node-b")) {
            awaitBalanced(fa, fb);
            Set<Integer> aParts = fa.partitions();
            String host = IntStream.range(0, 100).mapToObj(i -> "p" + i + ".com")
                    .filter(h -> aParts.contains(fa.partitionOf(h))).findFirst().orElseThrow();
            String root = "https://" + host + "/";
            web.page(root, links("/1")).page(root + "1", links("/"));

            Map<String, Instant> fetchedAt = new ConcurrentHashMap<>();
            Fetcher timed = req -> {
                fetchedAt.put(req.url().toString(), clock.instant());
                return web.fetch(req);
            };
            Duration politeness = Duration.ofMillis(2500);

            try (CrawlEngine a = engine(fa, timed, politeness); CrawlEngine b = engine(fb, timed, politeness)) {
                CrawlJob job = a.submit(CrawlRequest.builder("t1", root).maxDepth(1).respectRobots(false).build(), null);
                // node-a fetched the root and released the host with its delay (not the 30 s fetch lease); then it dies
                await("node-a fetches the root").atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(20))
                        .until(() -> fetchedAt.containsKey(root) && schedule.notBefore(host)
                                .filter(nb -> nb.isAfter(clock.instant().plusMillis(1000))
                                        && nb.isBefore(clock.instant().plusSeconds(5))).isPresent());
                Instant notBefore = schedule.notBefore(host).orElseThrow();
                fa.kill();
                a.close();

                CrawlJob done = b.await(job.jobId(), Duration.ofSeconds(60));
                assertEquals(JobStatus.COMPLETED, done.status());
                Instant second = fetchedAt.get(root + "1");
                assertFalse(second.isBefore(notBefore.minusMillis(50)),
                        "node-b waited out node-a's delay: fetched at " + second + ", allowed from " + notBefore);
                assertEquals(1, web.hits(root));
            }
        }
    }
}
