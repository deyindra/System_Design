package com.salesforce.einstein.webcrawler.url;

import com.salesforce.einstein.webcrawler.model.CanonicalUrl;

import java.net.IDN;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

/**
 * Raw href → {@link CanonicalUrl}. This is the first line of defense against cycles and duplicates:
 * every spelling of the same resource must map to one node <i>before</i> the seen-test.
 *
 * <ul>
 *   <li>resolve against the base (the page URL or its {@code <base href>})</li>
 *   <li>only {@code http}/{@code https} (drops {@code mailto:}, {@code javascript:}, {@code data:}, {@code tel:})</li>
 *   <li>lower-case scheme and host, IDN → punycode, drop trailing dot and default port</li>
 *   <li>remove dot segments, empty path → {@code /}, upper-case percent escapes, decode unreserved ones</li>
 *   <li>drop the fragment (never sent to the server)</li>
 *   <li>drop parameters {@link ParamRules} says are irrelevant, then sort the rest</li>
 * </ul>
 * The path's case and trailing slash are kept: servers may treat {@code /A} and {@code /a/} as different pages.
 *
 * @param params which query parameters are part of a page's identity (static list plus rules learned per host)
 */
public record UrlNormalizer(ParamRules params) {

    public static final int MAX_URL_LENGTH = 2048;

    public Optional<CanonicalUrl> normalize(String raw) { return normalize(null, raw); }

    public Optional<CanonicalUrl> normalize(URI base, String raw) {
        if (raw == null || raw.isBlank()) return Optional.empty();
        try {
            URI u = new URI(escapeIllegal(raw.strip()));
            if (base != null) u = withPath(base).resolve(u);
            String scheme = u.getScheme() == null ? null : u.getScheme().toLowerCase(Locale.ROOT);
            if (!"http".equals(scheme) && !"https".equals(scheme)) return Optional.empty();
            String host = u.getHost();
            if (host == null || host.isEmpty()) return Optional.empty();
            host = IDN.toASCII(host.toLowerCase(Locale.ROOT));
            if (host.endsWith(".")) host = host.substring(0, host.length() - 1);
            int port = u.getPort();
            if (port == 80 && scheme.equals("http") || port == 443 && scheme.equals("https")) port = -1;

            String path = u.normalize().getRawPath();
            if (path == null || path.isEmpty()) path = "/";
            while (path.startsWith("/../")) path = path.substring(3);   // RFC 3986: a parent segment above the root is dropped
            path = normalizePercent(path);

            String query = canonicalQuery(host, u.getRawQuery());
            StringBuilder sb = new StringBuilder(scheme).append("://").append(host);
            if (port != -1) sb.append(':').append(port);
            sb.append(path);
            if (!query.isEmpty()) sb.append('?').append(query);
            String value = sb.toString();
            if (value.length() > MAX_URL_LENGTH) return Optional.empty();
            return Optional.of(new CanonicalUrl(value, host, Hashing.urlHash(value)));
        } catch (URISyntaxException | IllegalArgumentException e) {
            return Optional.empty();
        }
    }

    /**
     * {@code https://cdn.example.com} → {@code https://cdn.example.com/}. {@link URI#resolve(URI)} on a base with an
     * authority and an empty path glues the reference onto the host ({@code https://cdn.example.comp.html}).
     */
    public static URI withPath(URI base) {
        if (base.getRawAuthority() == null || (base.getRawPath() != null && !base.getRawPath().isEmpty())) return base;
        return URI.create(base.getScheme() + "://" + base.getRawAuthority() + "/"
                + (base.getRawQuery() == null ? "" : "?" + base.getRawQuery()));
    }

    /** The same URL with one query parameter removed (for parameter learning). */
    public Optional<CanonicalUrl> without(CanonicalUrl url, String param) {
        URI u = url.uri();
        String q = u.getRawQuery();
        if (q == null) return Optional.empty();
        List<String> kept = new ArrayList<>();
        for (String kv : q.split("&")) if (!paramName(kv).equalsIgnoreCase(param)) kept.add(kv);
        String base = url.value().substring(0, url.value().indexOf('?'));
        return normalize(kept.isEmpty() ? base : base + "?" + String.join("&", kept));
    }

    /** Query parameters as (name, raw value) pairs, in canonical order. */
    public static List<Map.Entry<String, String>> params(CanonicalUrl url) {
        String q = url.uri().getRawQuery();
        List<Map.Entry<String, String>> out = new ArrayList<>();
        if (q != null) for (String kv : q.split("&")) {
            int eq = kv.indexOf('=');
            out.add(Map.entry(paramName(kv), eq < 0 ? "" : kv.substring(eq + 1)));
        }
        return out;
    }

    public static List<String> paramNames(CanonicalUrl url) {
        String q = url.uri().getRawQuery();
        List<String> out = new ArrayList<>();
        if (q != null) for (String kv : q.split("&")) out.add(paramName(kv));
        return out;
    }

    private String canonicalQuery(String host, String rawQuery) {
        if (rawQuery == null || rawQuery.isEmpty()) return "";
        List<String> kept = new ArrayList<>();
        for (String kv : rawQuery.split("&")) {
            if (kv.isEmpty()) continue;
            if (params.shouldDrop(host, paramName(kv))) continue;
            kept.add(normalizePercent(kv));
        }
        kept.sort(Comparator.comparing(UrlNormalizer::paramName).thenComparing(Comparator.naturalOrder()));
        return String.join("&", kept);
    }

    private static String paramName(String kv) {
        int eq = kv.indexOf('=');
        return eq < 0 ? kv : kv.substring(0, eq);
    }

    /** {@code %7e} → {@code ~}, {@code %2f} → {@code %2F}. Reserved characters stay escaped. */
    private static String normalizePercent(String s) {
        if (s.indexOf('%') < 0) return s;
        StringBuilder sb = new StringBuilder(s.length());
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '%' && i + 2 < s.length() && isHex(s.charAt(i + 1)) && isHex(s.charAt(i + 2))) {
                int v = Integer.parseInt(s.substring(i + 1, i + 3), 16);
                if (isUnreserved(v)) sb.append((char) v);
                else sb.append('%').append(s.substring(i + 1, i + 3).toUpperCase(Locale.ROOT));
                i += 2;
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    /** Real-world hrefs contain spaces, {@code |}, non-ASCII… which {@link URI} rejects. */
    private static String escapeIllegal(String s) {
        StringBuilder sb = new StringBuilder(s.length());
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c > 127 || c <= 32 || "<>\"{}|\\^`".indexOf(c) >= 0) {
                int end = Character.isHighSurrogate(c) && i + 1 < s.length() ? i + 2 : i + 1;
                for (byte b : s.substring(i, end).getBytes(StandardCharsets.UTF_8))
                    sb.append('%').append(String.format("%02X", b & 0xff));
                i = end - 1;
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    private static boolean isHex(char c) { return Character.digit(c, 16) >= 0; }

    private static boolean isUnreserved(int c) {
        return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9'
                || c == '-' || c == '.' || c == '_' || c == '~';
    }
}
