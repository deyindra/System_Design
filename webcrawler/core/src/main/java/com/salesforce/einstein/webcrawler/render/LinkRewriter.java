package com.salesforce.einstein.webcrawler.render;

import com.salesforce.einstein.webcrawler.model.CanonicalUrl;
import com.salesforce.einstein.webcrawler.url.UrlNormalizer;
import org.jsoup.Jsoup;
import org.jsoup.nodes.DataNode;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Rewrites every reference in an HTML page or stylesheet: {@code href}, {@code src}, {@code srcset},
 * {@code poster}, {@code data}, inline {@code style} and {@code <style>} {@code url()}s.
 *
 * <p>For each reference: find the node it points to, and ask {@code local} whether this job holds a copy. If yes, use
 * the local reference (keeping any {@code #fragment}); if no, write the absolute live URL. Runs at <b>read time</b>,
 * never on the stored bytes: see {@code CrawlResults}.
 *
 * <p>The node comes from the edge the crawler recorded for that exact {@code raw} reference ({@code recorded}), so a
 * page renders the way it was crawled even if normalization rules (learned parameters) have changed since. Only a
 * reference the crawler never recorded is canonicalized now.
 */
public final class LinkRewriter {

    private static final String[][] ATTRS = {
            {"a[href], area[href], link[href]", "href"},
            {"img[src], script[src], iframe[src], frame[src], video[src], audio[src], source[src], track[src], embed[src], input[src]", "src"},
            {"video[poster]", "poster"},
            {"object[data]", "data"},
    };
    private static final Pattern CSS_URL = Pattern.compile("url\\(\\s*(['\"]?)([^'\")]+)\\1\\s*\\)", Pattern.CASE_INSENSITIVE);
    private static final Pattern CSS_IMPORT = Pattern.compile("@import\\s+(['\"])([^'\"]+)\\1", Pattern.CASE_INSENSITIVE);

    private final UrlNormalizer normalizer;

    public LinkRewriter(UrlNormalizer normalizer) { this.normalizer = normalizer; }

    /**
     * @param recorded raw reference → url hash, from the page's recorded edges
     * @param local    url hash → our reference to it, if this job holds a copy
     */
    public byte[] rewriteHtml(byte[] html, String pageUrl, Map<String, String> recorded,
                              Function<String, Optional<String>> local) {
        Document doc;
        try {
            doc = Jsoup.parse(new ByteArrayInputStream(html), null, pageUrl);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        URI base = baseOf(doc.baseUri(), pageUrl);
        Refs refs = new Refs(recorded, local);
        doc.select("base").remove();                 // every reference is rewritten, so <base> would now be wrong

        for (String[] sel : ATTRS) {
            for (Element e : doc.select(sel[0])) {
                String raw = e.attr(sel[1]);
                Rewritten r = rewrite(base, raw, refs);
                e.attr(sel[1], r.value);
                //noinspection: the HTML attribute name
                if (r.local) { e.removeAttr("integrity"); e.removeAttr("crossorigin"); }   // our copy may differ (rewritten CSS)
            }
        }
        for (Element e : doc.select("img[srcset], source[srcset]")) e.attr("srcset", rewriteSrcset(base, e.attr("srcset"), refs));
        for (Element e : doc.select("[style]")) e.attr("style", rewriteCss(e.attr("style"), base, refs));
        for (Element e : doc.select("style")) {
            String css = rewriteCss(e.data(), base, refs);
            e.empty();
            e.appendChild(new DataNode(css));
        }
        doc.charset(StandardCharsets.UTF_8);         // we re-encode, so declare what we wrote
        return doc.outerHtml().getBytes(StandardCharsets.UTF_8);
    }

    public String rewriteCss(String css, URI base, Map<String, String> recorded, Function<String, Optional<String>> local) {
        return rewriteCss(css, UrlNormalizer.withPath(base), new Refs(recorded, local));
    }

    private String rewriteCss(String css, URI base, Refs refs) {
        String out = replace(CSS_URL, css, m -> "url(" + m.group(1) + rewrite(base, m.group(2).strip(), refs).value + m.group(1) + ")");
        return replace(CSS_IMPORT, out, m -> "@import " + m.group(1) + rewrite(base, m.group(2).strip(), refs).value + m.group(1));
    }

    private record Refs(Map<String, String> recorded, Function<String, Optional<String>> local) { }

    private record Rewritten(String value, boolean local) { }

    private Rewritten rewrite(URI base, String raw, Refs refs) {
        if (raw == null || raw.isBlank() || raw.startsWith("#") || raw.startsWith("data:")) return new Rewritten(raw, false);
        String target = refs.recorded().get(raw);
        if (target == null) {
            Optional<CanonicalUrl> c = normalizer.normalize(base, raw);
            if (c.isEmpty()) return new Rewritten(raw, false);   // mailto:, javascript:, tel: …: leave alone
            target = c.get().hash();
        }
        int hash = raw.indexOf('#');
        String fragment = hash >= 0 ? raw.substring(hash) : "";
        Optional<String> mine = refs.local().apply(target);
        return mine.map(s ->
                new Rewritten(s + fragment, true))
                .orElseGet(() -> new Rewritten(absolute(base, raw), false));
    }

    /** Each candidate is {@code url [descriptor]}; only the URL changes. */
    private String rewriteSrcset(URI base, String srcset, Refs refs) {
        return Arrays.stream(srcset.split(","))
                .map(String::strip)
                .filter(c -> !c.isEmpty())
                .map(c -> {
                    int sp = c.indexOf(' ');
                    return sp < 0 ? rewrite(base, c, refs).value : rewrite(base, c.substring(0, sp), refs).value + c.substring(sp);
                })
                .collect(Collectors.joining(", "));
    }

    /** {@code <base href>} if it is a usable absolute URI, else the page URL. */
    private static URI baseOf(String docBase, String pageUrl) {
        if (!docBase.isBlank()) {
            try {
                URI u = new URI(docBase);
                if (u.isAbsolute()) return UrlNormalizer.withPath(u);
            } catch (URISyntaxException ignored) { }
        }
        return UrlNormalizer.withPath(URI.create(pageUrl));
    }

    /** The author's URL made absolute, keeping its own query and fragment (not our canonical form). */
    private static String absolute(URI base, String raw) {
        try {
            return base.resolve(raw).toString();
        } catch (IllegalArgumentException e) {
            try { return base.resolve(raw.replace(" ", "%20")).toString(); } catch (IllegalArgumentException e2) { return raw; }
        }
    }

    private static String replace(Pattern p, String s, Function<Matcher, String> f) {
        Matcher m = p.matcher(s);
        StringBuilder sb = new StringBuilder();
        while (m.find()) m.appendReplacement(sb, Matcher.quoteReplacement(f.apply(m)));
        m.appendTail(sb);
        return sb.toString();
    }

}
