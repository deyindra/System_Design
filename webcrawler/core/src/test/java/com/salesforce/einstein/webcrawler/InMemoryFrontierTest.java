package com.salesforce.einstein.webcrawler;

import com.salesforce.einstein.webcrawler.frontier.InMemoryFrontier;
import com.salesforce.einstein.webcrawler.model.CanonicalUrl;
import com.salesforce.einstein.webcrawler.model.CrawlTask;
import com.salesforce.einstein.webcrawler.model.ResourceType;
import com.salesforce.einstein.webcrawler.url.ParamRules;
import com.salesforce.einstein.webcrawler.url.UrlNormalizer;
import org.junit.jupiter.api.Test;
import org.springframework.lang.NonNull;

import java.time.Clock;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class InMemoryFrontierTest {

    private final Clock clock = Clock.systemUTC();
    private final InMemoryFrontier f = new InMemoryFrontier(clock);
    private final UrlNormalizer n = new UrlNormalizer(new ParamRules());

    private CrawlTask task(String url, int depth) {
        CanonicalUrl c = n.normalize(url).orElseThrow();
        return new CrawlTask("j", c, depth, null, ResourceType.PAGE, 0, 0);
    }

    /** {@code poll} that must return a task; {@code null} there means the frontier timed out. */
    @NonNull
    private CrawlTask take(long timeoutMillis) throws InterruptedException {
        CrawlTask t = f.poll(timeoutMillis);
        assertNotNull(t, "no task within " + timeoutMillis + " ms");
        return t;
    }

    @Test void oneInFlightPerHostOthersStillFlow() throws Exception {
        f.push(task("https://a.com/1", 0));
        f.push(task("https://a.com/2", 0));
        f.push(task("https://b.com/1", 0));
        CrawlTask first = take(100);
        CrawlTask second = take(100);
        assertNotEquals(first.host(), second.host(), "the busy host is skipped, not waited on");
        assertNull(f.poll(50), "a.com is busy, b.com is busy");
        f.release("a.com", clock.instant());
        assertEquals("https://a.com/2", take(100).url().value());
    }

    @Test void politenessDelayIsEnforced() throws Exception {
        f.push(task("https://a.com/1", 0));
        f.push(task("https://a.com/2", 0));
        take(100);
        long t0 = System.nanoTime();
        f.release("a.com", clock.instant().plus(Duration.ofMillis(150)));
        assertEquals("https://a.com/2", take(1000).url().value());
        assertTrue(Duration.ofNanos(System.nanoTime() - t0).toMillis() >= 140);
    }

    @Test void breadthFirstWithinAHost() throws Exception {
        f.push(task("https://a.com/deep", 3));
        f.push(task("https://a.com/shallow", 1));
        f.push(task("https://a.com/seed", 0));
        for (String expected : new String[]{"/seed", "/shallow", "/deep"}) {
            assertEquals("https://a.com" + expected, take(100).url().value());
            f.release("a.com", clock.instant());
        }
        assertEquals(0, f.size());
    }
}
