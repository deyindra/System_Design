package com.salesforce.einstein.webcrawler;

import com.salesforce.einstein.webcrawler.engine.CrawlEngine;
import com.salesforce.einstein.webcrawler.engine.EngineConfig;
import com.salesforce.einstein.webcrawler.engine.IdempotencyConflictException;
import com.salesforce.einstein.webcrawler.model.CanonicalUrl;
import com.salesforce.einstein.webcrawler.model.CrawlJob;
import com.salesforce.einstein.webcrawler.model.CrawlRequest;
import com.salesforce.einstein.webcrawler.model.JobPage;
import com.salesforce.einstein.webcrawler.model.JobStatus;
import com.salesforce.einstein.webcrawler.model.LinkEdge;
import com.salesforce.einstein.webcrawler.model.PageStatus;
import com.salesforce.einstein.webcrawler.model.ResourceType;
import com.salesforce.einstein.webcrawler.model.Scope;
import com.salesforce.einstein.webcrawler.sitemap.InMemorySitemapGraphs;
import com.salesforce.einstein.webcrawler.sitemap.NavigationSitemapParser;
import com.salesforce.einstein.webcrawler.sitemap.SitemapGraphRecord;
import com.salesforce.einstein.webcrawler.sitemap.SitemapGraphService;
import com.salesforce.einstein.webcrawler.url.ParamRules;
import com.salesforce.einstein.webcrawler.url.UrlNormalizer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/** Sitemap crawls on one node: pages from the sitemap's graph, breadth-first; assets from the HTML. */
class SitemapCrawlTest {

    private static final String S = "https://site.com";
    private static final String NAV = S + "/nav.xml";
    private final FakeWeb web = new FakeWeb();
    private final InMemorySitemapGraphs sitemaps = new InMemorySitemapGraphs(4);
    private CrawlEngine engine;

    @BeforeEach void start() { engine = TestEngines.engine(web, TestEngines.config(4), sitemaps); }

    @AfterEach void stop() { engine.close(); }

    /** Counts down once a job has finished, after its {@code sitemapUrl} graph was dropped. */
    private CountDownLatch finished() {
        CountDownLatch finished = new CountDownLatch(1);
        engine.onFinished(j -> finished.countDown());
        return finished;
    }

    private CrawlJob crawl(CrawlRequest req) throws InterruptedException {
        CrawlJob job = engine.submit(req, null);
        CrawlJob done = engine.await(job.jobId(), Duration.ofSeconds(10));
        assertThat(done.status()).isEqualTo(JobStatus.COMPLETED);
        return done;
    }

    private static CrawlRequest.Builder fromSitemap(String... seeds) {
        return CrawlRequest.builder("t1", seeds).sitemapUrl(NAV).maxDepth(10).respectRobots(false);
    }

    /** One {@code <url>} per entry: its {@code loc}, then its navigation links. */
    private static String xml(Set<String> roots, String[]... urls) {
        StringBuilder sb = new StringBuilder("<urlset xmlns=\"http://www.sitemaps.org/schemas/sitemap/0.9\""
                + " xmlns:nav=\"urn:webcrawler:sitemap-nav\">");
        for (String[] url : urls) {
            sb.append("<url").append(roots.contains(url[0]) ? " nav:root=\"true\"" : "").append("><loc>")
                    .append(url[0]).append("</loc>");
            for (int i = 1; i < url.length; i++) sb.append("<nav:link href=\"").append(url[i]).append("\"/>");
            sb.append("</url>");
        }
        return sb.append("</urlset>").toString();
    }

    private void serveSitemap(String url, String xml) {
        web.asset(url, "application/xml", xml.getBytes(StandardCharsets.UTF_8));
    }

    private static String links(String... hrefs) {
        StringBuilder sb = new StringBuilder("<html><body>");
        for (String h : hrefs) sb.append("<a href=\"").append(h).append("\">x</a>");
        return sb.append("</body></html>").toString();
    }

    private Optional<JobPage> node(CrawlJob job, String url) {
        CanonicalUrl c = engine.normalizer().normalize(url).orElseThrow();
        return engine.jobs().page(job.jobId(), c.hash());
    }

    private int depth(CrawlJob job, String url) { return node(job, url).orElseThrow().depth(); }

    private List<String> outLinks(CrawlJob job, String url) {
        return engine.jobs().outLinks(job.jobId(), node(job, url).orElseThrow().urlHash()).stream()
                .map(LinkEdge::toUrl).toList();
    }

    private static String graphName(CrawlJob job) { return "sitemap_" + job.jobId().replace("-", ""); }

    // ---------------------------------------------------------------- pages come from the sitemap

    @Test void onlySitemapPagesAreFetchedAndHtmlLinksAreOnlyRecorded() throws Exception {
        serveSitemap(NAV, xml(Set.of(S + "/"), new String[]{S + "/", "/a", "/b"}, new String[]{S + "/a", "/c"}));
        web.page(S + "/", links("/a", "/outside"))
           .page(S + "/a", links("/"))
           .page(S + "/b", links("/also-outside"))
           .page(S + "/c", links())
           .page(S + "/outside", links()).page(S + "/also-outside", links());
        CrawlJob job = crawl(fromSitemap().build());

        assertThat(web.hits(S + "/outside")).isZero();
        assertThat(web.hits(S + "/also-outside")).isZero();
        assertThat(List.of(S + "/", S + "/a", S + "/b", S + "/c")).allSatisfy(u -> assertThat(web.hits(u)).isOne());
        assertThat(engine.jobs().stats(job.jobId()).pages()).isEqualTo(4);
        assertThat(depth(job, S + "/")).isZero();
        assertThat(depth(job, S + "/a")).isOne();
        assertThat(depth(job, S + "/b")).isOne();
        assertThat(depth(job, S + "/c")).isEqualTo(2);
        // the HTML link and the sitemap's edges, /a once although both name it
        assertThat(outLinks(job, S + "/")).containsExactlyInAnyOrder(S + "/a", S + "/outside", S + "/b");
        assertThat(node(job, S + "/outside")).isEmpty();
    }

    @Test void depthIsTheShortestPathAndMaxDepthCuts() throws Exception {
        serveSitemap(NAV, xml(Set.of(S + "/"), new String[]{S + "/", "/a", "/b", "/d"}, new String[]{S + "/a", "/c"},
                new String[]{S + "/b", "/c"}, new String[]{S + "/c", "/d", "/e"}, new String[]{S + "/e", "/f"}));
        for (String p : List.of("/", "/a", "/b", "/c", "/d", "/e", "/f")) web.page(S + p, links());
        CrawlJob job = crawl(fromSitemap().maxDepth(3).build());

        assertThat(depth(job, S + "/d")).isOne();                  // straight from the root, not via /c
        assertThat(depth(job, S + "/c")).isEqualTo(2);
        assertThat(depth(job, S + "/e")).isEqualTo(3);
        assertThat(web.hits(S + "/f")).isZero();                     // depth 4
        assertThat(node(job, S + "/f")).isEmpty();
        assertThat(outLinks(job, S + "/e")).containsExactly(S + "/f");   // the edge is kept
    }

    @Test void theSitemapIsTheScope() throws Exception {
        serveSitemap(NAV, xml(Set.of(S + "/"), new String[]{S + "/", "https://other.com/p"}));
        web.page(S + "/", links())
           .redirect("https://other.com/p", "https://other.com/q")   // a redirect within the page's own host
           .page("https://other.com/q", links());
        CrawlJob job = crawl(fromSitemap().scope(Scope.SAME_HOST).build());

        assertThat(web.hits("https://other.com/q")).isOne();
        assertThat(node(job, "https://other.com/p").orElseThrow().status()).isEqualTo(PageStatus.REDIRECT);
    }

    @Test void aRedirectedPageStillLeadsToItsSitemapSuccessors() throws Exception {
        serveSitemap(NAV, xml(Set.of(S + "/"), new String[]{S + "/", "/old"}, new String[]{S + "/old", "/kid"}));
        web.page(S + "/", links()).redirect(S + "/old", S + "/new").page(S + "/new", links()).page(S + "/kid", links());
        CrawlJob job = crawl(fromSitemap().build());

        assertThat(web.hits(S + "/new")).isOne();
        assertThat(depth(job, S + "/new")).isOne();
        assertThat(web.hits(S + "/kid")).isOne();
        assertThat(depth(job, S + "/kid")).isEqualTo(2);
    }

    @Test void pagesWithTheSameBytesBothExpand() throws Exception {
        serveSitemap(NAV, xml(Set.of(S + "/"), new String[]{S + "/", "/a", "/b"}, new String[]{S + "/a", "/x"},
                new String[]{S + "/b", "/y"}));
        String same = links();
        web.page(S + "/", links()).page(S + "/a", same).page(S + "/b", same).page(S + "/x", links("/x1"))
           .page(S + "/y", links("/y1"));
        CrawlJob job = crawl(fromSitemap().build());

        assertThat(node(job, S + "/b").orElseThrow().status()).isEqualTo(PageStatus.FETCHED);
        assertThat(web.hits(S + "/x")).isOne();
        assertThat(web.hits(S + "/y")).isOne();
    }

    @Test void withoutMarkedRootsTheCrawlStartsAtThePagesNothingLinksTo() throws Exception {
        serveSitemap(NAV, xml(Set.of(), new String[]{S + "/a", "/b"}, new String[]{S + "/p", "/q"},
                new String[]{S + "/q", "/p"}));
        for (String p : List.of("/a", "/b", "/p", "/q")) web.page(S + p, links());
        CrawlJob job = crawl(fromSitemap().build());

        assertThat(depth(job, S + "/a")).isZero();
        assertThat(depth(job, S + "/b")).isOne();
        assertThat(depth(job, S + "/p")).isZero();                  // a cycle nothing enters: its first page
        assertThat(depth(job, S + "/q")).isOne();
    }

    @Test void seedsAreTheRoots() throws Exception {
        serveSitemap(NAV, xml(Set.of(S + "/"), new String[]{S + "/", "/a"}, new String[]{S + "/a", "/b"}));
        for (String p : List.of("/", "/a", "/b")) web.page(S + p, links());
        CrawlJob job = crawl(fromSitemap(S + "/a").build());

        assertThat(web.hits(S + "/")).isZero();
        assertThat(depth(job, S + "/a")).isZero();
        assertThat(depth(job, S + "/b")).isOne();
    }

    // ---------------------------------------------------------------- assets

    @Test void assetsAreFetchedAsLeavesFromTheHtmlTheCssAndTheSitemap() throws Exception {
        serveSitemap(NAV, xml(Set.of(S + "/"), new String[]{S + "/", "/logo.png", "/a"}));
        web.page(S + "/", "<html><head><link rel=\"stylesheet\" href=\"/s.css\"></head>"
                        + "<body><img src=\"/i.png\"><a href=\"/a\">a</a></body></html>")
           .page(S + "/a", links())
           .asset(S + "/s.css", "text/css", "@font-face{src:url(/f.woff2)} body{background:url(bg.png)}"
                   .getBytes(StandardCharsets.UTF_8))
           .asset(S + "/i.png", "image/png", new byte[]{1})
           .asset(S + "/bg.png", "image/png", new byte[]{2})
           .asset(S + "/logo.png", "image/png", new byte[]{3})
           .asset(S + "/f.woff2", "font/woff2", new byte[]{4});
        CrawlJob job = crawl(fromSitemap().maxDepth(1)
                .downloadAssets(EnumSet.of(ResourceType.IMAGE, ResourceType.CSS, ResourceType.FONT)).build());

        for (String a : List.of("/s.css", "/i.png", "/bg.png", "/logo.png", "/f.woff2")) {
            assertThat(web.hits(S + a)).as(a).isOne();
            assertThat(depth(job, S + a)).as(a).isZero();            // a leaf at its page's depth
        }
        assertThat(node(job, S + "/logo.png").orElseThrow().type()).isEqualTo(ResourceType.IMAGE);
    }

    // ---------------------------------------------------------------- limits

    @Test void maxPagesIsABudget() throws Exception {
        serveSitemap(NAV, xml(Set.of(S + "/"), new String[]{S + "/", "/1", "/2", "/3", "/4", "/5"}));
        for (String p : List.of("/", "/1", "/2", "/3", "/4", "/5")) web.page(S + p, links());
        CrawlJob job = crawl(fromSitemap().maxPages(3).build());

        assertThat(engine.jobs().stats(job.jobId()).pages()).isEqualTo(3);
        assertThat(engine.jobs().stats(job.jobId()).truncated()).isPositive();
    }

    @Test void maxPagesCannotExceedTheServiceLimit() {
        serveSitemap(NAV, xml(Set.of(S + "/"), new String[]{S + "/"}));
        try (CrawlEngine limited = TestEngines.engine(web, TestEngines.config(1).withMaxPagesPerJob(10), sitemaps)) {
            assertThatThrownBy(() -> limited.submit(fromSitemap().maxPages(11).build(), null))
                    .isInstanceOf(IllegalArgumentException.class).hasMessage("maxPages: 1..10");
            assertThatThrownBy(() -> limited.submit(CrawlRequest.builder("t1", S + "/").maxPages(11).build(), null))
                    .isInstanceOf(IllegalArgumentException.class);
        }
        assertThat(EngineConfig.defaults().maxPagesPerJob()).isEqualTo(1_000_000);
    }

    // ---------------------------------------------------------------- bad requests make no job

    @Test void aSitemapThatCannotBeUsedIsABadRequest() {
        serveSitemap(S + "/bad.xml", "<html>not a sitemap</html>");
        serveSitemap(NAV, xml(Set.of(S + "/"), new String[]{S + "/"}));
        assertThatThrownBy(() -> engine.submit(fromSitemap().sitemapUrl(S + "/missing.xml").build(), null))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("HTTP 404");
        assertThatThrownBy(() -> engine.submit(fromSitemap().sitemapUrl(S + "/bad.xml").build(), null))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("invalid sitemap");
        assertThatThrownBy(() -> engine.submit(fromSitemap(S + "/elsewhere").build(), null))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("seed not in the sitemap");
        assertThatThrownBy(() -> engine.submit(CrawlRequest.builder("t1", S + "/").sitemapGraph("no_such_graph")
                .build(), null)).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("unknown sitemapGraph");
        assertThatThrownBy(() -> engine.submit(fromSitemap(S + "/").sitemapGraph("both_set").build(), null))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("at most one");
        assertThatThrownBy(() -> engine.submit(CrawlRequest.builder("t1").sitemapGraph("nav_graph").build(), null))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("seeds: 1..100");
        assertThatThrownBy(() -> engine.submit(fromSitemap().sitemapUrl("ftp://site.com/nav.xml").build(), null))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("sitemapUrl");
        web.page(S + "/", links());
        assertThat(web.hits(S + "/")).isZero();                      // no job ever started
    }

    @Test void anIdempotentRetryMustNameTheSameSitemap() {
        serveSitemap(NAV, xml(Set.of(S + "/"), new String[]{S + "/"}));
        serveSitemap(S + "/nav2.xml", xml(Set.of(S + "/"), new String[]{S + "/"}));
        web.page(S + "/", links());
        CrawlJob first = engine.submit(fromSitemap().build(), "k1");
        assertThat(engine.submit(fromSitemap().build(), "k1").jobId()).isEqualTo(first.jobId());
        assertThatThrownBy(() -> engine.submit(fromSitemap().sitemapUrl(S + "/nav2.xml").build(), "k1"))
                .isInstanceOf(IdempotencyConflictException.class);
    }

    // ---------------------------------------------------------------- the graph's lifetime

    @Test void aSitemapUrlGraphIsDroppedWhenTheJobEnds() throws Exception {
        serveSitemap(NAV, xml(Set.of(S + "/"), new String[]{S + "/", "/a"}));
        web.page(S + "/", links()).page(S + "/a", links());
        CountDownLatch finished = finished();
        CrawlJob job = crawl(fromSitemap().build());

        assertThat(finished.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(sitemaps.exists(graphName(job))).isFalse();
    }

    @Test void aNamedGraphIsCrawledInSlicesAndKept() throws Exception {
        sitemaps.load("shop_nav", new NavigationSitemapParser(new UrlNormalizer(new ParamRules())).parse(
                xml(Set.of(), new String[]{S + "/", "/a", "/b"}, new String[]{S + "/a", "/a1"},
                        new String[]{S + "/b", "/b1"})).graph());
        for (String p : List.of("/", "/a", "/b", "/a1", "/b1")) web.page(S + p, links());
        CountDownLatch finished = finished();

        CrawlJob slice = crawl(CrawlRequest.builder("t1", S + "/a").sitemapGraph("shop_nav").respectRobots(false).build());

        assertThat(web.hits(S + "/a")).isOne();
        assertThat(web.hits(S + "/a1")).isOne();
        assertThat(web.hits(S + "/")).isZero();
        assertThat(web.hits(S + "/b1")).isZero();
        assertThat(depth(slice, S + "/a1")).isOne();
        assertThat(finished.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(sitemaps.exists("shop_nav")).isTrue();
    }

    // ---------------------------------------------------------------- graphs loaded through the API

    @Test void aLoadedGraphIsCrawledByItsTenantOnly() throws Exception {
        serveSitemap(NAV, xml(Set.of(S + "/"), new String[]{S + "/", "/a"}));
        web.page(S + "/", links()).page(S + "/a", links());
        try (SitemapGraphService service = new SitemapGraphService(sitemaps, web, engine.normalizer(), 1 << 20, 1,
                Clock.systemUTC())) {
            service.load("t1", "shop_nav", NAV);
            SitemapGraphRecord ready = await("the load ends").atMost(Duration.ofSeconds(10))
                    .pollInterval(Duration.ofMillis(10)).until(() -> service.get("t1", "shop_nav").orElseThrow(),
                            g -> g.status() == SitemapGraphRecord.Status.READY);

            CrawlJob job = crawl(CrawlRequest.builder("t1", ready.roots().toArray(String[]::new))
                    .sitemapGraph("shop_nav").respectRobots(false).build());
            assertThat(engine.jobs().stats(job.jobId()).pages()).isEqualTo(2);
            assertThatThrownBy(() -> engine.submit(CrawlRequest.builder("t2", S + "/").sitemapGraph("shop_nav")
                    .build(), null)).isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("unknown sitemapGraph");
        }
    }

    @Test void aGraphThatIsNotReadyCannotBeCrawled() {
        sitemaps.register(SitemapGraphRecord.loading("shop_nav", "l1", "t1", NAV, Instant.now()));
        assertThatThrownBy(() -> engine.submit(CrawlRequest.builder("t1", S + "/").sitemapGraph("shop_nav").build(),
                null)).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("LOADING");
        sitemaps.update(sitemaps.record("shop_nav").orElseThrow().failed("x", Instant.now()));
        assertThatThrownBy(() -> engine.submit(CrawlRequest.builder("t1", S + "/").sitemapGraph("shop_nav").build(),
                null)).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("FAILED");
    }

    @Test void anotherJobsGraphCannotBeNamed() {
        assertThatThrownBy(() -> engine.submit(CrawlRequest.builder("t1", S + "/").sitemapGraph("sitemap_x").build(),
                null)).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("reserved");
    }
}
