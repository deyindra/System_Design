package com.salesforce.einstein.webcrawler.parse;

import com.salesforce.einstein.webcrawler.model.ResourceType;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Extracts every outgoing reference of an HTML page and classifies it.
 *
 * <p>Navigational links ({@code a}, {@code area}, {@code iframe}) become graph edges to pages. Embedded
 * resources ({@code img}, {@code srcset}, {@code video}, {@code audio}, {@code source}, stylesheets, scripts,
 * fonts, inline-style {@code url()}) become edges to <b>leaf</b> assets. The element tells us the type before
 * we fetch; the response Content-Type confirms it.
 */
public final class HtmlParser {

    public ParsedPage parse(byte[] body, String url) {
        Document doc;
        try {
            doc = Jsoup.parse(new ByteArrayInputStream(body), null, url);   // charset from BOM / <meta>
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        List<ExtractedLink> out = new ArrayList<>();

        for (Element e : doc.select("a[href], area[href]"))
            out.add(new ExtractedLink(e.attr("href"), ResourceType.guessFromPath(pathOf(e.attr("href")))));
        for (Element e : doc.select("iframe[src], frame[src]"))
            out.add(new ExtractedLink(e.attr("src"), ResourceType.PAGE));

        for (Element e : doc.select("img[src], input[type=image][src]")) out.add(new ExtractedLink(e.attr("src"), ResourceType.IMAGE));
        for (Element e : doc.select("img[srcset], source[srcset]"))
            for (String s : srcset(e.attr("srcset"))) out.add(new ExtractedLink(s, ResourceType.IMAGE));
        for (Element e : doc.select("video[poster]")) out.add(new ExtractedLink(e.attr("poster"), ResourceType.IMAGE));
        for (Element e : doc.select("video[src], video source[src]")) out.add(new ExtractedLink(e.attr("src"), ResourceType.VIDEO));
        for (Element e : doc.select("audio[src], audio source[src]")) out.add(new ExtractedLink(e.attr("src"), ResourceType.AUDIO));
        for (Element e : doc.select("track[src]")) out.add(new ExtractedLink(e.attr("src"), ResourceType.OTHER));
        for (Element e : doc.select("embed[src]")) out.add(new ExtractedLink(e.attr("src"), assetGuess(e.attr("src"))));
        for (Element e : doc.select("object[data]")) out.add(new ExtractedLink(e.attr("data"), assetGuess(e.attr("data"))));
        for (Element e : doc.select("script[src]")) out.add(new ExtractedLink(e.attr("src"), ResourceType.SCRIPT));

        String canonical = null;
        for (Element e : doc.select("link[href]")) {
            String rel = e.attr("rel").toLowerCase(Locale.ROOT);
            String href = e.attr("href");
            if (rel.contains("canonical")) canonical = href;
            else if (rel.contains("stylesheet")) out.add(new ExtractedLink(href, ResourceType.CSS));
            else if (rel.contains("icon")) out.add(new ExtractedLink(href, ResourceType.IMAGE));
            else if (rel.contains("preload") || rel.contains("prefetch")) out.add(new ExtractedLink(href, preloadType(e.attr("as"), href)));
            // rel=alternate/next/prev are navigational
            else if (rel.contains("alternate") || rel.contains("next") || rel.contains("prev")) out.add(new ExtractedLink(href, ResourceType.PAGE));
        }

        for (Element e : doc.select("[style]"))
            for (String u : CssParser.urls(e.attr("style"))) out.add(new ExtractedLink(u, assetGuess(u)));
        for (Element e : doc.select("style"))
            for (String u : CssParser.urls(e.data())) out.add(new ExtractedLink(u, assetGuess(u)));

        boolean noFollow = false, noIndex = false;
        for (Element m : doc.select("meta[name=robots], meta[name=googlebot]")) {
            String c = m.attr("content").toLowerCase(Locale.ROOT);
            if (c.contains("nofollow") || c.contains("none")) noFollow = true;
            if (c.contains("noindex") || c.contains("none")) noIndex = true;
        }
        return new ParsedPage(doc.baseUri(), doc.title(), canonical, noFollow, noIndex, out);
    }

    /** {@code "a.jpg 1x, b.jpg 2x"} → {@code [a.jpg, b.jpg]}. */
    static List<String> srcset(String srcset) {
        List<String> out = new ArrayList<>();
        for (String candidate : srcset.split(",")) {
            String c = candidate.strip();
            if (c.isEmpty()) continue;
            int sp = c.indexOf(' ');
            out.add(sp < 0 ? c : c.substring(0, sp));
        }
        return out;
    }

    /** Embedded resources are never pages; an unknown extension is OTHER. */
    static ResourceType assetGuess(String raw) {
        ResourceType t = ResourceType.guessFromPath(pathOf(raw));
        return t == ResourceType.PAGE ? ResourceType.OTHER : t;
    }

    private static ResourceType preloadType(String as, String href) {
        return switch (as.toLowerCase(Locale.ROOT)) {
            case "font" -> ResourceType.FONT;
            case "image" -> ResourceType.IMAGE;
            case "style" -> ResourceType.CSS;
            case "script" -> ResourceType.SCRIPT;
            case "video" -> ResourceType.VIDEO;
            case "audio" -> ResourceType.AUDIO;
            default -> assetGuess(href);
        };
    }

    private static String pathOf(String raw) {
        int cut = raw.length();
        for (char c : new char[]{'?', '#'}) { int i = raw.indexOf(c); if (i >= 0) cut = Math.min(cut, i); }
        return raw.substring(0, cut);
    }
}
