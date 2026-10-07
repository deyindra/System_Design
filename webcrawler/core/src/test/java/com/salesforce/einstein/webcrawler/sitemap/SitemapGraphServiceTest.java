package com.salesforce.einstein.webcrawler.sitemap;

import com.salesforce.einstein.webcrawler.FakeWeb;
import com.salesforce.einstein.webcrawler.fetch.FetchResult;
import com.salesforce.einstein.webcrawler.fetch.Fetcher;
import com.salesforce.einstein.webcrawler.url.ParamRules;
import com.salesforce.einstein.webcrawler.url.UrlNormalizer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/** Loading named sitemap graphs in the background, and who may see them. */
class SitemapGraphServiceTest {

    private static final String S = "https://site.com";
    private static final String NAV = S + "/nav.xml";
    private static final String NAV_XML = "<urlset xmlns=\"http://www.sitemaps.org/schemas/sitemap/0.9\""
            + " xmlns:nav=\"urn:webcrawler:sitemap-nav\">"
            + "<url nav:root=\"true\"><loc>" + S + "/</loc><nav:link href=\"/a\"/><nav:link href=\"/b\"/></url>"
            + "<url><loc>" + S + "/a</loc><nav:link href=\"/c\"/></url>"
            + "</urlset>";

    private final FakeWeb web = new FakeWeb();
    private final InMemorySitemapGraphs graphs = new InMemorySitemapGraphs(4);
    private final UrlNormalizer normalizer = new UrlNormalizer(new ParamRules());
    private SitemapGraphService service;

    @BeforeEach void start() {
        serve(NAV, NAV_XML);
        service = service(web);
    }

    @AfterEach void stop() { service.close(); }

    private SitemapGraphService service(Fetcher fetcher) {
        return new SitemapGraphService(graphs, fetcher, normalizer, 1 << 20, 2, Clock.systemUTC());
    }

    private void serve(String url, String xml) {
        web.asset(url, "application/xml", xml.getBytes(StandardCharsets.UTF_8));
    }

    /** {@code t1}'s entry once its load has ended. */
    private SitemapGraphRecord finished(String name) {
        return await("the load ends").atMost(Duration.ofSeconds(10)).pollInterval(Duration.ofMillis(10))
                .until(() -> service.get("t1", name).orElseThrow(),
                        g -> g.status() != SitemapGraphRecord.Status.LOADING);
    }

    @Test void aLoadEndsReadyWithTheSitemapsRoots() {
        SitemapGraphRecord started = service.load("t1", "shop_nav", NAV);
        assertThat(started.tenantId()).isEqualTo("t1");

        SitemapGraphRecord ready = finished("shop_nav");
        assertThat(ready.status()).isEqualTo(SitemapGraphRecord.Status.READY);
        assertThat(ready.loadId()).isEqualTo(started.loadId());
        assertThat(ready.pages()).isEqualTo(4);
        assertThat(ready.edges()).isEqualTo(3);
        assertThat(ready.roots()).containsExactly(S + "/");
        assertThat(ready.rootCount()).isOne();
        assertThat(ready.finishedAt()).isNotNull();
        assertThat(graphs.exists("shop_nav")).isTrue();
    }

    @Test void repeatingALoadIsHarmless() {
        SitemapGraphRecord first = service.load("t1", "shop_nav", NAV);
        assertThat(service.load("t1", "shop_nav", NAV).loadId()).isEqualTo(first.loadId());
        finished("shop_nav");
        assertThat(service.load("t1", "shop_nav", NAV).status()).isEqualTo(SitemapGraphRecord.Status.READY);
        assertThat(web.hits(NAV)).isOne();
    }

    @Test void aNameInUseIsAConflict() {
        service.load("t1", "shop_nav", NAV);
        assertThatThrownBy(() -> service.load("t1", "shop_nav", S + "/other.xml"))
                .isInstanceOf(SitemapGraphConflictException.class);
        assertThatThrownBy(() -> service.load("t2", "shop_nav", NAV)).isInstanceOf(SitemapGraphConflictException.class);

        graphs.load("bulk_nav", new NavigationSitemapParser(normalizer).parse(NAV_XML).graph());  // no entry
        assertThatThrownBy(() -> service.load("t1", "bulk_nav", NAV)).isInstanceOf(SitemapGraphConflictException.class);
    }

    @Test void anotherTenantsGraphDoesNotExist() {
        service.load("t1", "shop_nav", NAV);
        finished("shop_nav");
        assertThat(service.get("t2", "shop_nav")).isEmpty();
        assertThat(service.delete("t2", "shop_nav")).isFalse();
        assertThat(graphs.exists("shop_nav")).isTrue();
    }

    @Test void badNamesAndUrlsAreRejected() {
        assertThatThrownBy(() -> service.load("t1", "sitemap_x", NAV))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("reserved");
        assertThatThrownBy(() -> service.load("t1", "no-hyphens", NAV))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("name");
        assertThatThrownBy(() -> service.load("t1", "shop_nav", "ftp://site.com/nav.xml"))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("sitemapUrl");
        assertThat(graphs.record("sitemap_x")).isEmpty();
    }

    @Test void aSitemapThatCannotBeUsedEndsFailedAndLeavesNoGraph() {
        serve(S + "/bad.xml", "<html>not a sitemap</html>");
        serve(S + "/empty.xml", "<urlset xmlns=\"http://www.sitemaps.org/schemas/sitemap/0.9\"/>");
        service.load("t1", "missing_nav", S + "/missing.xml");
        service.load("t1", "bad_nav", S + "/bad.xml");
        service.load("t1", "empty_nav", S + "/empty.xml");

        assertThat(finished("missing_nav").error()).contains("HTTP 404");
        assertThat(finished("bad_nav").error()).contains("invalid sitemap");
        assertThat(finished("empty_nav").error()).isEqualTo("sitemap has no pages");
        assertThat(finished("bad_nav").status()).isEqualTo(SitemapGraphRecord.Status.FAILED);
        assertThat(graphs.exists("missing_nav") || graphs.exists("bad_nav") || graphs.exists("empty_nav")).isFalse();
    }

    @Test void aFailedLoadIsDeletedAndLoadedAgain() {
        service.load("t1", "shop_nav", S + "/missing.xml");
        finished("shop_nav");
        assertThat(service.delete("t1", "shop_nav")).isTrue();
        service.load("t1", "shop_nav", NAV);
        assertThat(finished("shop_nav").status()).isEqualTo(SitemapGraphRecord.Status.READY);
    }

    @Test void aGraphDeletedWhileItLoadsIsNotLeftBehind() throws Exception {
        CountDownLatch fetching = new CountDownLatch(1);
        CountDownLatch resume = new CountDownLatch(1);
        CountDownLatch fetched = new CountDownLatch(1);
        Fetcher gated = req -> {
            fetching.countDown();
            try {
                if (!resume.await(10, TimeUnit.SECONDS)) return FetchResult.status(504);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return FetchResult.status(503);
            }
            fetched.countDown();
            return web.fetch(req);
        };
        try (SitemapGraphService slow = service(gated)) {
            slow.load("t1", "shop_nav", NAV);
            assertThat(fetching.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(slow.delete("t1", "shop_nav")).isTrue();
            assertThat(slow.get("t1", "shop_nav")).isEmpty();

            resume.countDown();                                    // the load writes its graph, then finds no entry
            assertThat(fetched.await(10, TimeUnit.SECONDS)).isTrue();
            await("the orphan is dropped").atMost(Duration.ofSeconds(10)).pollInterval(Duration.ofMillis(10))
                    .during(Duration.ofMillis(200)).until(() -> !graphs.exists("shop_nav"));
            assertThat(graphs.record("shop_nav")).isEmpty();
        }
    }
}
