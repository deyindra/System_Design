package com.salesforce.einstein.webcrawler.fetch;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * {@code robots.txt} per scheme + host, fetched on first use and cached for {@link #TTL}
 * (in production: the {@code hosts} table + a shared cache read by all workers).
 * Redirects are followed up to {@link #MAX_REDIRECTS} hops (RFC 9309 §2.3.1.2), e.g. {@code http → https} or
 * {@code example.com → www.example.com}. 4xx or too many redirects → allow all; 5xx, network error or a refused
 * destination → disallow all, re-checked after {@link #ERROR_TTL}.
 */
public final class RobotsCache {

    public static final Duration TTL = Duration.ofHours(24);
    public static final Duration ERROR_TTL = Duration.ofMinutes(10);
    public static final int MAX_REDIRECTS = 5;
    private static final long MAX_ROBOTS_BYTES = 500 * 1024;   // RFC 9309 asks crawlers to read at least 500 KiB

    private record Entry(RobotsRules rules, Instant expiresAt) { }

    private final Fetcher fetcher;
    private final Clock clock;
    private final String agentToken;
    private final Map<String, Entry> cache = new ConcurrentHashMap<>();

    public RobotsCache(Fetcher fetcher, Clock clock, String agentToken) {
        this.fetcher = fetcher;
        this.clock = clock;
        this.agentToken = agentToken;
    }

    public RobotsRules rulesFor(URI url) {
        String origin = url.getScheme() + "://" + url.getRawAuthority();
        Instant now = clock.instant();
        Entry e = cache.get(origin);
        if (e != null && now.isBefore(e.expiresAt)) return e.rules;
        Entry fresh = load(origin, now);
        cache.put(origin, fresh);
        return fresh.rules;
    }

    private Entry load(String origin, Instant now) {
        URI url = URI.create(origin + "/robots.txt");
        FetchResult r = fetcher.fetch(FetchRequest.of(url, MAX_ROBOTS_BYTES));
        for (int hop = 0; r.isRedirect() && hop < MAX_REDIRECTS; hop++) {
            try {
                url = url.resolve(r.location());
            } catch (IllegalArgumentException e) {
                return new Entry(RobotsRules.DISALLOW_ALL, now.plus(ERROR_TTL));
            }
            r = fetcher.fetch(FetchRequest.of(url, MAX_ROBOTS_BYTES));
        }
        if (r.isRedirect()) return new Entry(RobotsRules.ALLOW_ALL, now.plus(TTL));   // RFC 9309: treat as unavailable
        if (r.status() == 200)
            return new Entry(RobotsRules.parse(new String(r.body(), StandardCharsets.UTF_8), agentToken), now.plus(TTL));
        if (r.status() >= 400 && r.status() < 500) return new Entry(RobotsRules.ALLOW_ALL, now.plus(TTL));
        return new Entry(RobotsRules.DISALLOW_ALL, now.plus(ERROR_TTL));
    }
}
