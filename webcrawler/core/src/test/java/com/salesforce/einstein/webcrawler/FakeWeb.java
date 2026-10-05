package com.salesforce.einstein.webcrawler;

import com.salesforce.einstein.webcrawler.fetch.FetchRequest;
import com.salesforce.einstein.webcrawler.fetch.FetchResult;
import com.salesforce.einstein.webcrawler.fetch.Fetcher;
import com.salesforce.einstein.webcrawler.url.Hashing;

import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/** An in-memory internet. Counts every request, answers conditional GETs with 304. Unknown URLs are 404. */
public final class FakeWeb implements Fetcher {

    private final Map<String, FetchResult> responses = new ConcurrentHashMap<>();
    private final Map<String, Deque<FetchResult>> scripted = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> hits = new ConcurrentHashMap<>();

    public FakeWeb page(String url, String html) {
        byte[] b = html.getBytes(StandardCharsets.UTF_8);
        responses.put(url, new FetchResult(200, "text/html; charset=utf-8", b, null,
                "\"" + Hashing.sha256Hex(b).substring(0, 16) + "\"", null, false, null));
        return this;
    }

    public FakeWeb asset(String url, String contentType, byte[] bytes) {
        responses.put(url, new FetchResult(200, contentType, bytes, null, null, null, false, null));
        return this;
    }

    public FakeWeb redirect(String url, String location) {
        responses.put(url, FetchResult.redirect(301, location));
        return this;
    }

    public FakeWeb robots(String origin, String text) {
        return asset(origin + "/robots.txt", "text/plain", text.getBytes(StandardCharsets.UTF_8));
    }

    /** Successive responses for one URL; the last one repeats. */
    public void sequence(String url, FetchResult... results) {
        scripted.put(url, new ArrayDeque<>(List.of(results)));
    }

    public int hits(String url) { return hits.getOrDefault(url, new AtomicInteger()).get(); }

    public int totalHitsExcludingRobots() {
        return hits.entrySet().stream().filter(e -> !e.getKey().endsWith("/robots.txt"))
                .mapToInt(e -> e.getValue().get()).sum();
    }

    @Override public FetchResult fetch(FetchRequest req) {
        String url = req.url().toString();
        hits.computeIfAbsent(url, k -> new AtomicInteger()).incrementAndGet();
        Deque<FetchResult> seq = scripted.get(url);
        if (seq != null) {
            synchronized (seq) { return seq.size() > 1 ? seq.poll() : seq.peek(); }
        }
        FetchResult r = responses.get(url);
        if (r == null) return FetchResult.status(404);
        if (r.etag() != null && r.etag().equals(req.etag())) return FetchResult.status(304);
        if (r.body().length > req.maxBytes())
            return new FetchResult(200, r.contentType(), new byte[0], null, null, null, true, null);
        return r;
    }
}
