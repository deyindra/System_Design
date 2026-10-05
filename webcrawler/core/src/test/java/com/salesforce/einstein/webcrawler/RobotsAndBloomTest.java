package com.salesforce.einstein.webcrawler;

import com.salesforce.einstein.webcrawler.fetch.RobotsRules;
import com.salesforce.einstein.webcrawler.url.BloomFilter;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RobotsAndBloomTest {

    @Test void robotsLongestMatchWinsAndAgentGroupBeatsStar() {
        RobotsRules r = RobotsRules.parse("""
                User-agent: *
                Disallow: /

                User-agent: EinsteinCrawler
                Disallow: /private
                Allow: /private/public
                Disallow: /*.pdf$
                Crawl-delay: 2.5
                Sitemap: https://x.com/sitemap.xml
                """, "EinsteinCrawler");
        assertTrue(r.isAllowed("/docs"));
        assertFalse(r.isAllowed("/private/x"));
        assertTrue(r.isAllowed("/private/public/y"));
        assertFalse(r.isAllowed("/a/b.pdf"));
        assertTrue(r.isAllowed("/a/b.pdf?x=1"));
        assertEquals(Duration.ofMillis(2500), r.crawlDelay().orElseThrow());
        assertEquals(1, r.sitemaps().size());

        RobotsRules other = RobotsRules.parse("User-agent: *\nDisallow: /\n", "OtherBot");
        assertFalse(other.isAllowed("/anything"));
    }

    @Test void bloomFilterHasNoFalseNegativesAndRoughlyTheConfiguredFalsePositiveRate() {
        BloomFilter b = new BloomFilter(100_000, 0.01);
        for (int i = 0; i < 100_000; i++) b.put("https://x.com/" + i);
        for (int i = 0; i < 100_000; i++) assertTrue(b.mightContain("https://x.com/" + i));
        int fp = 0;
        for (int i = 0; i < 100_000; i++) if (b.mightContain("https://y.com/" + i)) fp++;
        assertTrue(fp < 2_000, "false positives: " + fp);
        assertEquals(7, b.hashFunctions());
    }
}
