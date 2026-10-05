package com.salesforce.einstein.webcrawler.fetch;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

/**
 * Parsed {@code robots.txt} for our user agent: Allow/Disallow with longest-match-wins (RFC 9309),
 * {@code *} and {@code $} wildcards, {@code Crawl-delay}, {@code Sitemap}.
 */
public final class RobotsRules {

    private record Rule(String pattern, boolean allow) { }

    private final List<Rule> rules;
    private final Duration crawlDelay;
    private final List<String> sitemaps;

    private RobotsRules(List<Rule> rules, Duration crawlDelay, List<String> sitemaps) {
        this.rules = rules;
        this.crawlDelay = crawlDelay;
        this.sitemaps = sitemaps;
    }

    public static final RobotsRules ALLOW_ALL = new RobotsRules(List.of(), null, List.of());
    /** RFC 9309: a 5xx on robots.txt means "assume complete disallow" until it can be fetched. */
    public static final RobotsRules DISALLOW_ALL = new RobotsRules(List.of(new Rule("/", false)), null, List.of());

    /** Groups for {@code agentToken} win over {@code *}; within the chosen groups, rules are merged. */
    public static RobotsRules parse(String text, String agentToken) {
        String agent = agentToken.toLowerCase(Locale.ROOT);
        List<Rule> specific = new ArrayList<>(), star = new ArrayList<>();
        Duration delaySpecific = null, delayStar = null;
        List<String> sitemaps = new ArrayList<>();
        boolean inSpecific = false, inStar = false, lastWasAgent = false;
        for (String line : text.split("\\R")) {
            int hash = line.indexOf('#');
            if (hash >= 0) line = line.substring(0, hash);
            int colon = line.indexOf(':');
            if (colon < 0) continue;
            String key = line.substring(0, colon).strip().toLowerCase(Locale.ROOT);
            String val = line.substring(colon + 1).strip();
            switch (key) {
                case "user-agent" -> {
                    if (!lastWasAgent) { inSpecific = false; inStar = false; }
                    String ua = val.toLowerCase(Locale.ROOT);
                    if (ua.equals("*")) inStar = true;
                    else if (agent.contains(ua)) inSpecific = true;
                    lastWasAgent = true;
                    continue;
                }
                case "allow", "disallow" -> {
                    if (!val.isEmpty()) {
                        Rule r = new Rule(val, key.equals("allow"));
                        if (inSpecific) specific.add(r);
                        if (inStar) star.add(r);
                    }
                }
                case "crawl-delay" -> {
                    try {
                        Duration d = Duration.ofMillis((long) (Double.parseDouble(val) * 1000));
                        if (inSpecific) delaySpecific = d;
                        if (inStar) delayStar = d;
                    } catch (NumberFormatException ignored) { }
                }
                case "sitemap" -> sitemaps.add(val);
                default -> { }
            }
            lastWasAgent = false;
        }
        boolean useSpecific = !specific.isEmpty() || delaySpecific != null;
        return new RobotsRules(useSpecific ? specific : star, useSpecific ? delaySpecific : delayStar, sitemaps);
    }

    public boolean isAllowed(String pathAndQuery) {
        Rule best = null;
        for (Rule r : rules) {
            if (matches(r.pattern, pathAndQuery)
                    && (best == null || r.pattern.length() > best.pattern.length()
                        || r.pattern.length() == best.pattern.length() && r.allow)) best = r;
        }
        return best == null || best.allow;
    }

    public Optional<Duration> crawlDelay() { return Optional.ofNullable(crawlDelay); }

    public List<String> sitemaps() { return List.copyOf(sitemaps); }

    private static boolean matches(String pattern, String path) {
        boolean anchored = pattern.endsWith("$");
        String p = anchored ? pattern.substring(0, pattern.length() - 1) : pattern;
        return glob(p, 0, path, 0, anchored);
    }

    private static boolean glob(String p, int pi, String s, int si, boolean anchored) {
        while (pi < p.length()) {
            char c = p.charAt(pi);
            if (c == '*') {
                for (int k = si; k <= s.length(); k++) if (glob(p, pi + 1, s, k, anchored)) return true;
                return false;
            }
            if (si >= s.length() || s.charAt(si) != c) return false;
            pi++;
            si++;
        }
        return !anchored || si == s.length();
    }
}
