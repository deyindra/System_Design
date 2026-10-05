package com.salesforce.einstein.webcrawler;

import com.salesforce.einstein.webcrawler.engine.CrawlEngine;
import com.salesforce.einstein.webcrawler.model.CrawlJob;
import com.salesforce.einstein.webcrawler.model.CrawlRequest;
import com.salesforce.einstein.webcrawler.model.JobStatus;
import com.salesforce.einstein.webcrawler.model.Scope;
import com.salesforce.einstein.webcrawler.render.CrawlResults;
import com.salesforce.einstein.webcrawler.render.LinkMode;
import com.salesforce.einstein.webcrawler.url.ParamRules;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Link rewriting where the page, the base or the rules are unusual. */
class RewriteEdgeCasesTest {

    private static final String S = "https://site.com";
    private final FakeWeb web = new FakeWeb();
    private CrawlEngine engine;
    private CrawlResults results;

    @AfterEach void stop() { if (engine != null) engine.close(); }

    private CrawlJob crawl(CrawlRequest req) throws InterruptedException {
        engine = TestEngines.engine(web, 2);
        results = new CrawlResults(engine.jobs(), engine.contents(), engine.normalizer());
        CrawlJob job = engine.submit(req, null);
        assertEquals(JobStatus.COMPLETED, engine.await(job.jobId(), Duration.ofSeconds(10)).status());
        return job;
    }

    private String hash(String url) { return engine.normalizer().normalize(url).orElseThrow().hash(); }

    private Document view(CrawlJob job, String url) {
        byte[] html = results.content(job.jobId(), hash(url), LinkMode.SNAPSHOT, "/api").orElseThrow().bytes();
        return Jsoup.parse(new String(html, StandardCharsets.UTF_8));
    }

    @Test void renderingUsesTheCrawlTimeResolutionEvenAfterParamRulesChange() throws Exception {
        web.page(S + "/", "<a id=a href='/p?x=1'>p</a>").page(S + "/p?x=1", "<p>one</p>");
        CrawlJob job = crawl(CrawlRequest.builder("t1", S + "/").build());
        String crawled = hash(S + "/p?x=1");

        ParamRules rules = engine.normalizer().params();               // later, another job learns x is irrelevant
        for (int i = 0; i < ParamRules.LEARN_THRESHOLD; i++) rules.observe("site.com", "x", "v" + i, true);
        assertNotEquals(crawled, hash(S + "/p?x=1"), "today's normalizer would pick another node");

        assertEquals("/api/pages/" + crawled + "/content?links=snapshot", view(job, S + "/").expectFirst("#a").attr("href"));
    }

    @Test void malformedBaseFallsBackToThePageUrl() throws Exception {
        //noinspection HttpUrlsUsage: a malformed <base href> is the input under test
        web.page(S + "/d/", "<base href='http://[broken'><a id=a href='x.html'>x</a>").page(S + "/d/x.html", "<p>x</p>");
        CrawlJob job = crawl(CrawlRequest.builder("t1", S + "/d/").build());
        assertEquals("/api/pages/" + hash(S + "/d/x.html") + "/content?links=snapshot",
                view(job, S + "/d/").expectFirst("#a").attr("href"));
    }

    @Test void baseWithoutAPathResolvesUnderTheHost() throws Exception {
        web.page(S + "/", "<base href='https://cdn.site.com'><img id=i src='logo.png'>")
           .asset("https://cdn.site.com/logo.png", "image/png", new byte[]{1, 2});
        CrawlJob job = crawl(CrawlRequest.builder("t1", S + "/").scope(Scope.SAME_HOST).build());
        assertEquals(1, web.hits("https://cdn.site.com/logo.png"));
        assertEquals("/api/pages/" + hash("https://cdn.site.com/logo.png") + "/content",
                view(job, S + "/").expectFirst("#i").attr("src"));
    }

    @Test void identicalStylesheetsAtDifferentUrlsExportSeparately() throws Exception {
        byte[] css = "body{background:url(bg.png)}".getBytes(StandardCharsets.UTF_8);   // relative: differs per dir
        web.page(S + "/", "<link rel=stylesheet href=/a/s.css><link rel=stylesheet href=/b/s.css>")
           .asset(S + "/a/s.css", "text/css", css).asset(S + "/b/s.css", "text/css", css)
           .asset(S + "/a/bg.png", "image/png", new byte[]{1}).asset(S + "/b/bg.png", "image/png", new byte[]{2});
        CrawlJob job = crawl(CrawlRequest.builder("t1", S + "/").build());

        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        results.export(engine.jobs().get(job.jobId()).orElseThrow(), buf);
        Map<String, String> files = new HashMap<>();
        try (ZipInputStream zip = new ZipInputStream(new ByteArrayInputStream(buf.toByteArray()))) {
            for (ZipEntry e; (e = zip.getNextEntry()) != null; ) files.put(e.getName(), new String(zip.readAllBytes(), StandardCharsets.ISO_8859_1));
        }
        String a = files.get("assets/" + hash(S + "/a/s.css") + ".css"), b = files.get("assets/" + hash(S + "/b/s.css") + ".css");
        String bgA = engine.jobs().page(job.jobId(), hash(S + "/a/bg.png")).orElseThrow().contentHash();
        String bgB = engine.jobs().page(job.jobId(), hash(S + "/b/bg.png")).orElseThrow().contentHash();
        assertTrue(a.contains(bgA) && !a.contains(bgB), a);
        assertTrue(b.contains(bgB) && !b.contains(bgA), b);
    }
}
