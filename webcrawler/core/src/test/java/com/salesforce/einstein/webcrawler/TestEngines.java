package com.salesforce.einstein.webcrawler;

import com.salesforce.einstein.webcrawler.engine.CrawlEngine;
import com.salesforce.einstein.webcrawler.engine.EngineConfig;
import com.salesforce.einstein.webcrawler.fetch.Fetcher;
import com.salesforce.einstein.webcrawler.frontier.InMemoryFrontier;
import com.salesforce.einstein.webcrawler.store.InMemoryContentStore;
import com.salesforce.einstein.webcrawler.store.InMemoryJobStore;
import com.salesforce.einstein.webcrawler.store.InMemoryPageStore;
import com.salesforce.einstein.webcrawler.url.ParamRules;
import com.salesforce.einstein.webcrawler.url.TrapDetector;
import com.salesforce.einstein.webcrawler.url.UrlNormalizer;

import java.time.Clock;
import java.time.Duration;

public final class TestEngines {
    private TestEngines() { }

    /** No politeness delay, 1 ms retry backoff. */
    public static CrawlEngine engine(FakeWeb web, int workers) { return engine((Fetcher) web, workers); }

    public static CrawlEngine engine(Fetcher web, int workers) {
        Clock clock = Clock.systemUTC();
        EngineConfig cfg = EngineConfig.defaults().withWorkers(workers)
                .withPoliteness(Duration.ZERO).withRetryBackoff(Duration.ofMillis(1));
        return new CrawlEngine(cfg, new InMemoryFrontier(clock), new InMemoryJobStore(), new InMemoryPageStore(),
                new InMemoryContentStore(), web, new UrlNormalizer(new ParamRules()), TrapDetector.defaults(), clock);
    }
}
