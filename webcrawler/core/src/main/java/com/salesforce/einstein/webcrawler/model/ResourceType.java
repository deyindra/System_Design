package com.salesforce.einstein.webcrawler.model;

import java.util.Locale;

/**
 * What a URL points at. {@link #PAGE} nodes are expanded (parsed for links); every other type is a
 * <b>leaf</b> of the crawl graph: it is fetched (or only referenced) but never followed, and it does not
 * consume crawl depth.
 */
public enum ResourceType {
    PAGE, IMAGE, VIDEO, AUDIO, CSS, SCRIPT, FONT, DOCUMENT, OTHER;

    public boolean isAsset() { return this != PAGE; }

    /** Authoritative type, from the response {@code Content-Type}. */
    public static ResourceType fromContentType(String contentType) {
        if (contentType == null || contentType.isBlank()) return OTHER;
        String ct = contentType.toLowerCase(Locale.ROOT);
        if (ct.startsWith("text/html") || ct.startsWith("application/xhtml")) return PAGE;
        if (ct.startsWith("image/")) return IMAGE;
        if (ct.startsWith("video/") || ct.contains("mpegurl") || ct.contains("dash+xml")) return VIDEO;
        if (ct.startsWith("audio/")) return AUDIO;
        if (ct.startsWith("text/css")) return CSS;
        if (ct.contains("javascript") || ct.contains("ecmascript")) return SCRIPT;
        if (ct.startsWith("font/") || ct.contains("font-woff")) return FONT;
        if (ct.startsWith("application/pdf") || ct.contains("msword") || ct.contains("officedocument")) return DOCUMENT;
        return OTHER;
    }

    /** Best guess before fetching, from the file extension. Used for {@code <a href>} targets. */
    public static ResourceType guessFromPath(String path) {
        String p = path == null ? "" : path.toLowerCase(Locale.ROOT);
        int dot = p.lastIndexOf('.');
        if (dot < 0 || dot < p.lastIndexOf('/')) return PAGE;
        return switch (p.substring(dot + 1)) {
            case "jpg", "jpeg", "png", "gif", "webp", "svg", "ico", "avif", "bmp" -> IMAGE;
            case "mp4", "webm", "mov", "m3u8", "mpd", "avi", "mkv" -> VIDEO;
            case "mp3", "wav", "ogg", "m4a", "flac", "aac" -> AUDIO;
            case "css" -> CSS;
            case "js", "mjs" -> SCRIPT;
            case "woff", "woff2", "ttf", "otf", "eot" -> FONT;
            case "pdf", "doc", "docx", "xls", "xlsx", "ppt", "pptx" -> DOCUMENT;
            case "zip", "gz", "tar", "exe", "dmg", "bin" -> OTHER;
            default -> PAGE;   // .html, .php, .aspx, ...
        };
    }
}
