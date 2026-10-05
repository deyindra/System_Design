package com.salesforce.einstein.webcrawler;

import com.salesforce.einstein.webcrawler.engine.CrawlEngine;
import com.salesforce.einstein.webcrawler.model.CrawlJob;
import com.salesforce.einstein.webcrawler.model.CrawlRequest;
import com.salesforce.einstein.webcrawler.model.ResourceType;
import com.salesforce.einstein.webcrawler.render.CrawlResults;
import com.salesforce.einstein.webcrawler.render.LinkMode;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** "When a user views or downloads crawled pages, references point at the crawled copies." */
class CrawlResultsTest {

    private static final String S = "https://site.com";
    private static final byte[] PNG = {(byte) 0x89, 'P', 'N', 'G'};
    private final FakeWeb web = new FakeWeb();
    private CrawlEngine engine;
    private CrawlResults results;
    private CrawlJob job;

    @BeforeEach void crawl() throws Exception {
        web.page(S + "/docs/index.html", "<html><head><base href='/docs/'><link rel=stylesheet href='site.css'></head><body>"
                        + "<a id=l1 href='guide.html#install'>guide</a>"
                        + "<a id=l2 href='/old'>moved</a>"
                        + "<a id=l3 href='https://other-site.org/p'>out of scope</a>"
                        + "<a id=l4 href='https://elsewhere.com/x?utm_source=a'>external</a>"
                        + "<a id=l5 href='mailto:a@b.com'>mail</a>"
                        + "<a id=l6 href='#top'>top</a>"
                        + "<img id=i1 src='img/logo.png' srcset='img/logo.png 1x, img/logo-2x.png 2x'>"
                        + "<video id=v1 src='/media/intro.mp4'></video>"
                        + "<div id=d1 style=\"background:url('img/bg.png')\"></div></body></html>")
           .page(S + "/docs/guide.html", "<a href='index.html'>home</a><a href='index.html?sessionid=42'>home again</a>"
                        + "<a id=g1 href='/deep/2'>deep</a><img src='img/logo.png'>")
           .redirect(S + "/old", S + "/new")
           .page(S + "/new", "<a href='/docs/index.html'>back</a>")
           .page(S + "/deep/2", "<p>never fetched at maxDepth=1</p>")
           .asset(S + "/docs/site.css", "text/css", "h1{background:url(img/bg.png)} @font-face{src:url('/f.woff2')}".getBytes(StandardCharsets.UTF_8))
           .asset(S + "/docs/img/logo.png", "image/png", PNG)
           .asset(S + "/docs/img/logo-2x.png", "image/png", PNG)
           .asset(S + "/docs/img/bg.png", "image/png", new byte[]{1});
        engine = TestEngines.engine(web, 2);
        results = new CrawlResults(engine.jobs(), engine.contents(), engine.normalizer());
        job = engine.submit(CrawlRequest.builder("t1", S + "/docs/index.html").maxDepth(1)
                .downloadAssets(EnumSet.of(ResourceType.IMAGE, ResourceType.CSS)).build(), null);
        engine.await(job.jobId(), Duration.ofSeconds(10));
    }

    @AfterEach void stop() { engine.close(); }

    private String hash(String url) { return engine.normalizer().normalize(url).orElseThrow().hash(); }

    private Document view(String url, LinkMode mode) {
        byte[] html = results.content(job.jobId(), hash(url), mode, "/v1/crawls/" + job.jobId()).orElseThrow().bytes();
        return Jsoup.parse(new String(html, StandardCharsets.UTF_8));
    }

    @Test void snapshotModePointsCrawledReferencesAtOurCopies() {
        String api = "/v1/crawls/" + job.jobId() + "/pages/";
        Document d = view(S + "/docs/index.html", LinkMode.SNAPSHOT);

        assertEquals(api + hash(S + "/docs/guide.html") + "/content?links=snapshot#install", d.expectFirst("#l1").attr("href"));
        assertEquals(api + hash(S + "/new") + "/content?links=snapshot", d.expectFirst("#l2").attr("href"),
                "a link to a redirect opens our copy of the redirect target");
        assertEquals(api + hash(S + "/docs/img/logo.png") + "/content", d.expectFirst("#i1").attr("src"));
        assertTrue(d.expectFirst("#i1").attr("srcset").contains(api + hash(S + "/docs/img/logo-2x.png") + "/content 2x"));
        assertTrue(d.expectFirst("#d1").attr("style").contains(api + hash(S + "/docs/img/bg.png") + "/content"));
        assertEquals(api + hash(S + "/docs/site.css") + "/content?links=snapshot", d.expectFirst("link[rel=stylesheet]").attr("href"));
        assertTrue(d.select("base").isEmpty(), "<base> is removed once every reference is rewritten");
    }

    @Test void referencesWeDoNotHoldBecomeAbsoluteLinksToTheLiveSite() {
        Document d = view(S + "/docs/index.html", LinkMode.SNAPSHOT);
        assertEquals("https://other-site.org/p", d.expectFirst("#l3").attr("href"), "out of scope: live site");
        assertEquals(S + "/deep/2", view(S + "/docs/guide.html", LinkMode.SNAPSHOT).expectFirst("#g1").attr("href"),
                "beyond maxDepth: live site");
        assertEquals("https://elsewhere.com/x?utm_source=a", d.expectFirst("#l4").attr("href"), "author's URL kept, not our canonical form");
        assertEquals(S + "/media/intro.mp4", d.expectFirst("#v1").attr("src"), "video was reference-only");
        assertEquals("mailto:a@b.com", d.expectFirst("#l5").attr("href"));
        assertEquals("#top", d.expectFirst("#l6").attr("href"));
    }

    @Test void duplicateUrlVariantsResolveToTheOneStoredCopy() {
        Document guide = view(S + "/docs/guide.html", LinkMode.SNAPSHOT);
        String target = "/v1/crawls/" + job.jobId() + "/pages/" + hash(S + "/docs/index.html") + "/content?links=snapshot";
        assertEquals(2, guide.select("a[href='" + target + "']").size(), "index.html and index.html?sessionid=42 are one node");
    }

    @Test void stylesheetsAreRewrittenToo() {
        String css = new String(results.content(job.jobId(), hash(S + "/docs/site.css"), LinkMode.SNAPSHOT,
                "/v1/crawls/" + job.jobId()).orElseThrow().bytes(), StandardCharsets.UTF_8);
        assertTrue(css.contains("/pages/" + hash(S + "/docs/img/bg.png") + "/content"));
        assertTrue(css.contains("url('" + S + "/f.woff2')"), "font was not downloaded: live URL");
    }

    @Test void rawAndAbsoluteModes() {
        byte[] raw = results.content(job.jobId(), hash(S + "/docs/img/logo.png"), LinkMode.RAW, "").orElseThrow().bytes();
        assertArrayEquals(PNG, raw);
        Document abs = view(S + "/docs/index.html", LinkMode.ABSOLUTE);
        assertEquals(S + "/docs/guide.html#install", abs.expectFirst("#l1").attr("href"));
        assertEquals(S + "/docs/img/logo.png", abs.expectFirst("#i1").attr("src"));
    }

    @Test void exportIsAnOfflineSiteWithRelativeLinks() throws Exception {
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        results.export(engine.jobs().get(job.jobId()).orElseThrow(), buf);
        Map<String, byte[]> files = new HashMap<>();
        try (ZipInputStream zip = new ZipInputStream(new ByteArrayInputStream(buf.toByteArray()))) {
            for (ZipEntry e; (e = zip.getNextEntry()) != null; ) files.put(e.getName(), zip.readAllBytes());
        }
        String index = "pages/" + hash(S + "/docs/index.html") + ".html";
        String logo = "assets/" + engine.jobs().page(job.jobId(), hash(S + "/docs/img/logo.png")).orElseThrow().contentHash() + ".png";
        assertTrue(files.containsKey("index.html") && files.containsKey("manifest.json") && files.containsKey(index));
        assertTrue(files.containsKey(logo));
        assertEquals(1, files.keySet().stream().filter(f -> f.endsWith(".png") && f.contains(
                engine.jobs().page(job.jobId(), hash(S + "/docs/img/logo.png")).orElseThrow().contentHash())).count(),
                "logo.png and logo-2x.png have the same bytes: one file");

        Document d = Jsoup.parse(new String(files.get(index), StandardCharsets.UTF_8));
        assertEquals(hash(S + "/docs/guide.html") + ".html#install", d.expectFirst("#l1").attr("href"));
        assertEquals(hash(S + "/new") + ".html", d.expectFirst("#l2").attr("href"));
        assertEquals("../" + logo, d.expectFirst("#i1").attr("src"));
        assertEquals("https://other-site.org/p", d.expectFirst("#l3").attr("href"));
        assertTrue(files.keySet().stream().noneMatch(f -> f.contains(hash(S + "/old"))), "a redirect is a pointer, not a file");
    }
}
