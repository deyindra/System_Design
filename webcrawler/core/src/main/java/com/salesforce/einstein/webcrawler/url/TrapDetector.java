package com.salesforce.einstein.webcrawler.url;

import com.salesforce.einstein.webcrawler.model.CanonicalUrl;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Spider-trap heuristics. The seen-set stops true cycles; traps are <i>infinite acyclic</i> graphs
 * (calendars with a "next month" link, {@code /a/a/a/a/...} relative-link bugs, faceted search with
 * millions of filter combinations) where every URL is new.
 *
 * <p>Stateless, so every node runs its own copy. The per-path variant counter is job state and lives in the
 * {@code JobStore} ({@code variants:{job}}), so the cap holds across nodes.
 */
public final class TrapDetector {

    private final int maxPathSegments;
    private final int maxSegmentRepeats;
    private final int maxQueryParams;
    private final int maxVariantsPerPath;

    public TrapDetector(int maxPathSegments, int maxSegmentRepeats, int maxQueryParams, int maxVariantsPerPath) {
        this.maxPathSegments = maxPathSegments;
        this.maxSegmentRepeats = maxSegmentRepeats;
        this.maxQueryParams = maxQueryParams;
        this.maxVariantsPerPath = maxVariantsPerPath;
    }

    public static TrapDetector defaults() { return new TrapDetector(20, 3, 10, 50); }

    /** The reason this URL looks like a trap, if it does. Stateless checks only. */
    public Optional<String> check(CanonicalUrl url) {
        String[] segs = url.path().split("/");
        if (segs.length > maxPathSegments) return Optional.of("path too deep");
        Map<String, Integer> seen = new HashMap<>();
        for (String s : segs) {
            if (!s.isEmpty() && seen.merge(s, 1, Integer::sum) > maxSegmentRepeats)
                return Optional.of("repeating path segment '" + s + "'");
        }
        if (UrlNormalizer.paramNames(url).size() > maxQueryParams) return Optional.of("too many query parameters");
        return Optional.empty();
    }

    /** The counter key for the per-path variant cap (host + path), or empty when the URL has no query string. */
    public Optional<String> variantKey(CanonicalUrl url) {
        return url.uri().getRawQuery() == null ? Optional.empty() : Optional.of(url.host() + url.path());
    }

    /** How many query-string variants of one path a job may admit. */
    public int maxVariantsPerPath() { return maxVariantsPerPath; }
}
