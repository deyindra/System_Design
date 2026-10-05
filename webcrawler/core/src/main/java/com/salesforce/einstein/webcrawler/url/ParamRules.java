package com.salesforce.einstein.webcrawler.url;

import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Decides which query parameters are part of a page's identity.
 *
 * <p>The web gives no signal for this: {@code ?id=7} selects different content while {@code ?utm_source=x}
 * or {@code ?sessionid=abc} does not. Three layers, cheapest first:
 * <ol>
 *   <li><b>Static list</b>: tracking and session parameters, dropped everywhere.</li>
 *   <li><b>Learned per host</b>: when a URL with parameter {@code p} returns the same bytes as the same URL
 *       without {@code p}, that is one vote for "irrelevant". After {@link #LEARN_THRESHOLD} votes from
 *       <i>distinct values</i> of {@code p} and no counter-example, {@code p} is dropped for the host. One
 *       counter-example makes it relevant forever. Distinct values matter: {@code ?page=1} is often the same as
 *       no {@code page} at all, and seeing it on every crawl must not hide {@code ?page=2}.</li>
 *   <li><b>{@code <link rel=canonical>}</b> and content hashing (in the worker) catch everything else.</li>
 * </ol>
 * In production the learned rules live in {@code url_param_rules} and are cached in every worker.
 */
public final class ParamRules {

    public static final int LEARN_THRESHOLD = 3;

    private static final Set<String> TRACKING = Set.of(
            "gclid", "fbclid", "msclkid", "dclid", "yclid", "mc_cid", "mc_eid", "_ga", "_gl", "igshid",
            "ref_src", "sessionid", "sid", "jsessionid", "phpsessid", "aspsessionid", "cfid", "cftoken");

    /** Votes for one parameter on one host. Workers observe concurrently, so every access holds its monitor. */
    private static final class Evidence {
        private final Set<String> sameValues = new HashSet<>();     // capped at LEARN_THRESHOLD
        private boolean differs;

        synchronized void record(String value, boolean sameContent) {
            if (!sameContent) differs = true;
            else if (sameValues.size() < LEARN_THRESHOLD) sameValues.add(value);
        }

        synchronized boolean irrelevant() { return !differs && sameValues.size() >= LEARN_THRESHOLD; }
    }

    private final Map<String, Map<String, Evidence>> learned = new ConcurrentHashMap<>();

    public boolean shouldDrop(String host, String param) {
        String p = param.toLowerCase(Locale.ROOT);
        if (p.startsWith("utm_") || TRACKING.contains(p)) return true;
        Map<String, Evidence> byParam = learned.get(host);
        if (byParam == null) return false;
        Evidence e = byParam.get(p);
        return e != null && e.irrelevant();
    }

    /** {@code sameContent}: the URL with {@code param=value} had the same content hash as the URL without it. */
    public void observe(String host, String param, String value, boolean sameContent) {
        learned.computeIfAbsent(host, h -> new ConcurrentHashMap<>())
                .computeIfAbsent(param.toLowerCase(Locale.ROOT), p -> new Evidence())
                .record(value, sameContent);
    }

    /** Parameters learned to be irrelevant on this host. */
    public Set<String> learnedIrrelevant(String host) {
        Set<String> out = new TreeSet<>();
        learned.getOrDefault(host, Map.of()).forEach((p, e) -> { if (e.irrelevant()) out.add(p); });
        return out;
    }
}
