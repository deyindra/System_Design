package com.salesforce.einstein.webcrawler;

import com.salesforce.einstein.webcrawler.model.CanonicalUrl;
import com.salesforce.einstein.webcrawler.url.ParamRules;
import com.salesforce.einstein.webcrawler.url.TrapDetector;
import com.salesforce.einstein.webcrawler.url.UrlNormalizer;
import org.junit.jupiter.api.Test;

import java.net.URI;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class UrlNormalizerTest {

    private final ParamRules rules = new ParamRules();
    private final UrlNormalizer n = new UrlNormalizer(rules);

    private String norm(String raw) { return n.normalize(raw).map(CanonicalUrl::value).orElse(null); }

    @SuppressWarnings("HttpUrlsUsage")   // http is a scheme the normalizer accepts; its default port is the point
    @Test void caseDefaultPortFragmentAndDotSegments() {
        assertEquals("https://example.com/a/c", norm("HTTPS://Example.COM:443/a/b/../c#top"));
        assertEquals("http://example.com/", norm("http://example.com"));
        assertEquals("http://example.com:8080/", norm("http://example.com:8080"));
        assertEquals("https://example.com/~user", norm("https://example.com/%7euser"));
        assertEquals("https://example.com/a%2Fb", norm("https://example.com/a%2fb"));
    }

    @Test void pathCaseAndTrailingSlashAreKept() {
        assertNotEquals(norm("https://example.com/A"), norm("https://example.com/a"));
        assertNotEquals(norm("https://example.com/a/"), norm("https://example.com/a"));
    }

    @Test void queryIsSortedAndTrackingParamsDropped() {
        assertEquals("https://shop.com/p?color=red&id=7",
                norm("https://shop.com/p?id=7&utm_source=mail&color=red&gclid=x&sessionid=abc"));
        assertEquals(norm("https://shop.com/p?b=2&a=1"), norm("https://shop.com/p?a=1&b=2"));
        assertEquals("https://shop.com/p", norm("https://shop.com/p?utm_campaign=x"));
        assertNotEquals(norm("https://shop.com/item?id=1"), norm("https://shop.com/item?id=2"));
    }

    @Test void resolvesRelativeAgainstBase() {
        URI base = URI.create("https://example.com/docs/guide/intro.html");
        assertEquals("https://example.com/docs/guide/setup.html", n.normalize(base, "setup.html").orElseThrow().value());
        assertEquals("https://example.com/img/x.png", n.normalize(base, "../../img/x.png").orElseThrow().value());
        assertEquals("https://cdn.com/x.js", n.normalize(base, "//cdn.com/x.js").orElseThrow().value());
        assertEquals("https://example.com/a%20b", n.normalize(base, "/a b").orElseThrow().value());
    }

    @Test void nonHttpSchemesAreNotNodes() {
        assertTrue(n.normalize("mailto:a@b.com").isEmpty());
        assertTrue(n.normalize("javascript:void(0)").isEmpty());
        assertTrue(n.normalize("data:image/png;base64,AAAA").isEmpty());
        assertTrue(n.normalize("https://example.com/" + "x".repeat(UrlNormalizer.MAX_URL_LENGTH)).isEmpty());
    }

    @Test void learnedParamIsDroppedAfterThreshold() {
        assertEquals("https://news.com/a?ref=home", norm("https://news.com/a?ref=home"));
        for (int i = 0; i < ParamRules.LEARN_THRESHOLD; i++) rules.observe("news.com", "ref", "v" + i, true);
        assertEquals("https://news.com/a", norm("https://news.com/a?ref=home"));

        for (int i = 0; i < ParamRules.LEARN_THRESHOLD; i++) rules.observe("news.com", "page", "v" + i, true);
        rules.observe("news.com", "page", "9", false);   // one counter-example: relevant forever
        assertEquals("https://news.com/a?page=2", norm("https://news.com/a?page=2"));
    }

    @Test void oneValueSeenManyTimesIsNotEnoughToLearn() {
        // ?page=1 is often the same as no page at all; re-crawling it must not condemn "page" for the whole host
        for (int i = 0; i < 10 * ParamRules.LEARN_THRESHOLD; i++) rules.observe("news.com", "page", "1", true);
        assertEquals("https://news.com/list?page=2", norm("https://news.com/list?page=2"));
    }

    @Test void emptyBasePathDoesNotGlueOntoTheHost() {
        URI base = URI.create("https://cdn.example.com");
        assertEquals("https://cdn.example.com/p.html", n.normalize(base, "p.html").orElseThrow().value());
    }

    @Test void trapDetector() {
        TrapDetector t = TrapDetector.defaults();
        assertTrue(t.check(n.normalize("https://x.com/a/b/a/b/a/b/a/b").orElseThrow()).isPresent());
        assertTrue(t.check(n.normalize("https://x.com/docs/page").orElseThrow()).isEmpty());
    }
}
