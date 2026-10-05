package com.salesforce.einstein.webcrawler.render;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.salesforce.einstein.webcrawler.model.CrawlJob;
import com.salesforce.einstein.webcrawler.model.JobPage;
import com.salesforce.einstein.webcrawler.model.LinkEdge;
import com.salesforce.einstein.webcrawler.model.PageStatus;
import com.salesforce.einstein.webcrawler.model.ResourceType;
import com.salesforce.einstein.webcrawler.store.ContentStore;
import com.salesforce.einstein.webcrawler.store.JobStore;
import com.salesforce.einstein.webcrawler.store.Slice;
import com.salesforce.einstein.webcrawler.url.UrlNormalizer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * Serving a finished (or running) crawl back to the user.
 *
 * <h2>Why links are rewritten at read time, not when the page is stored</h2>
 * <ol>
 *   <li><b>Children don't exist yet.</b> BFS stores a page before its links are crawled, so at write time we can't
 *       know which references will have a local copy.</li>
 *   <li><b>Bytes are shared.</b> One content-addressed blob serves every job and tenant that crawled that page; each
 *       job has a different set of crawled children, so there is no single correct rewritten version.</li>
 *   <li><b>Fidelity.</b> The stored bytes stay exactly what the origin sent (evidence, reparsing, diffing).</li>
 * </ol>
 * Rewriting is a pure function of {@code (contentHash, jobId, mode)}, so its output is cached at the CDN.
 *
 * <h2>Resolution</h2>
 * A reference resolves through the job's graph: {@code REDIRECT → redirectTo}, {@code DUPLICATE → duplicateOf},
 * until a node with content. A link to {@code /old} that redirected to {@code /new} therefore opens our copy of
 * {@code /new}, and a link to {@code /p?sessionid=9} opens the single stored copy of {@code /p}.
 */
public final class CrawlResults {

    public record Content(byte[] bytes, String contentType) { }

    private static final int MAX_HOPS = 10;

    private final JobStore jobs;
    private final ContentStore contents;
    private final LinkRewriter rewriter;
    private final ObjectMapper json = new ObjectMapper();

    public CrawlResults(JobStore jobs, ContentStore contents, UrlNormalizer normalizer) {
        this.jobs = jobs;
        this.contents = contents;
        this.rewriter = new LinkRewriter(normalizer);
    }

    /** The node that actually holds bytes for {@code urlHash} in this job, following redirects and duplicates. */
    public Optional<JobPage> resolve(String jobId, String urlHash) {
        String h = urlHash;
        for (int i = 0; i < MAX_HOPS && h != null; i++) {
            Optional<JobPage> p = jobs.page(jobId, h);
            if (p.isEmpty()) return Optional.empty();
            JobPage page = p.get();
            if (page.status().hasContent()) return p;
            h = page.status() == PageStatus.REDIRECT ? page.redirectTo()
                    : page.status() == PageStatus.DUPLICATE ? page.duplicateOf() : null;
        }
        return Optional.empty();
    }

    /**
     * One node's bytes. In {@link LinkMode#SNAPSHOT} each reference this job holds points at
     * {@code {apiPrefix}/pages/{urlHash}/content}, so the browser stays inside the snapshot.
     */
    public Optional<Content> content(String jobId, String urlHash, LinkMode mode, String apiPrefix) {
        Optional<JobPage> node = resolve(jobId, urlHash);
        if (node.isEmpty()) return Optional.empty();
        JobPage p = node.get();
        byte[] bytes = contents.get(p.contentHash()).orElseThrow();
        if (mode == LinkMode.RAW || !(p.type() == ResourceType.PAGE || p.type() == ResourceType.CSS))
            return Optional.of(new Content(bytes, p.contentType()));

        Function<String, Optional<String>> local = mode == LinkMode.ABSOLUTE
                ? h -> Optional.empty()
                : h -> resolve(jobId, h).map(t -> apiPrefix + "/pages/" + t.urlHash() + "/content"
                        + (t.type() == ResourceType.PAGE || t.type() == ResourceType.CSS ? "?links=snapshot" : ""));
        return Optional.of(render(p, bytes, local));
    }

    /**
     * An offline, browsable copy of the whole crawl as a ZIP:
     * <pre>
     * index.html                 links to every seed and page
     * manifest.json              original URL → file, status, type, depth
     * pages/{urlHash}.html       one per page, links rewritten to relative paths
     * assets/{urlHash}.css       stylesheets, rewritten relative to their own URL
     * assets/{contentHash}.{ext} images, fonts, media … stored once per distinct content
     * </pre>
     * Links between pages become {@code other.html}, page → asset {@code ../assets/x.png}, CSS → asset {@code x.woff2}.
     * Anything not in the crawl stays an absolute link to the live site.
     */
    public void export(CrawlJob job, OutputStream out) throws IOException {
        String jobId = job.jobId();
        List<JobPage> all = new ArrayList<>();
        String cursor = null;
        do {
            Slice<JobPage> s = jobs.pages(jobId, null, cursor, 1000);
            all.addAll(s.items());
            cursor = s.nextCursor();
        } while (cursor != null);

        Map<String, Object> manifest = new LinkedHashMap<>();
        Set<String> written = new HashSet<>();
        try (ZipOutputStream zip = new ZipOutputStream(out)) {
            for (JobPage p : all) {
                Optional<JobPage> owner = resolve(jobId, p.urlHash());
                manifest.put(p.url(), Map.of("status", p.status().name(), "type", p.type().name(), "depth", p.depth(),
                        "file", owner.map(CrawlResults::exportPath).orElse("")));
                if (owner.isEmpty() || !owner.get().urlHash().equals(p.urlHash())) continue;   // pointer, not a file
                String path = exportPath(p);
                if (!written.add(path)) continue;                                            // same asset bytes, other URL
                String dir = path.substring(0, path.indexOf('/') + 1);
                byte[] bytes = contents.get(p.contentHash()).orElseThrow();
                Content c = (p.type() == ResourceType.PAGE || p.type() == ResourceType.CSS)
                        ? render(p, bytes, h -> resolve(jobId, h).map(o -> relative(dir, exportPath(o))))
                        : new Content(bytes, p.contentType());
                zip.putNextEntry(new ZipEntry(path));
                zip.write(c.bytes());
                zip.closeEntry();
            }
            zip.putNextEntry(new ZipEntry("manifest.json"));
            zip.write(json.writerWithDefaultPrettyPrinter().writeValueAsBytes(manifest));
            zip.closeEntry();
            zip.putNextEntry(new ZipEntry("index.html"));
            zip.write(index(job, all).getBytes(StandardCharsets.UTF_8));
            zip.closeEntry();
        }
    }

    private Content render(JobPage p, byte[] bytes, Function<String, Optional<String>> local) {
        Map<String, String> recorded = new HashMap<>();             // raw reference → node, as the crawler resolved it
        for (LinkEdge e : jobs.outLinks(p.jobId(), p.urlHash())) recorded.putIfAbsent(e.rawHref(), e.toHash());
        if (p.type() == ResourceType.CSS) {
            String css = rewriter.rewriteCss(new String(bytes, StandardCharsets.UTF_8), URI.create(p.url()), recorded, local);
            return new Content(css.getBytes(StandardCharsets.UTF_8), "text/css; charset=utf-8");
        }
        return new Content(rewriter.rewriteHtml(bytes, p.url(), recorded, local), "text/html; charset=utf-8");
    }

    /**
     * Pages and stylesheets are rewritten relative to their own URL, so they get one file per URL. Other assets are
     * byte-identical wherever they came from, so one file per distinct content.
     */
    static String exportPath(JobPage p) {
        if (p.type() == ResourceType.PAGE) return "pages/" + p.urlHash() + ".html";
        if (p.type() == ResourceType.CSS) return "assets/" + p.urlHash() + ".css";
        return "assets/" + p.contentHash() + extension(p);
    }

    private static String relative(String fromDir, String to) {
        return to.startsWith(fromDir) ? to.substring(fromDir.length()) : "../" + to;
    }

    private static String extension(JobPage p) {
        String path = URI.create(p.url()).getPath();
        if (path != null) {
            int dot = path.lastIndexOf('.');
            if (dot > path.lastIndexOf('/') && path.length() - dot <= 6) return path.substring(dot).toLowerCase(Locale.ROOT);
        }
        String ct = p.contentType() == null ? "" : p.contentType().toLowerCase(Locale.ROOT);
        if (ct.contains("png")) return ".png";
        if (ct.contains("jpeg")) return ".jpg";
        if (ct.contains("gif")) return ".gif";
        if (ct.contains("svg")) return ".svg";
        if (ct.contains("webp")) return ".webp";
        if (ct.contains("css")) return ".css";
        if (ct.contains("javascript")) return ".js";
        return ".bin";
    }

    private String index(CrawlJob job, List<JobPage> all) {
        StringBuilder sb = new StringBuilder("<!doctype html><meta charset=utf-8><title>Crawl ")
                .append(job.jobId()).append("</title><h1>Crawl ").append(job.jobId()).append("</h1><ul>");
        for (JobPage p : all) {
            if (p.type() != ResourceType.PAGE) continue;
            Optional<JobPage> o = resolve(job.jobId(), p.urlHash());
            String label = escape(p.url()) + " (depth " + p.depth() + ", " + p.status() + ")";
            sb.append("<li>").append(o.map(x -> "<a href=\"" + exportPath(x) + "\">" + label + "</a>").orElse(label)).append("</li>");
        }
        return sb.append("</ul>").toString();
    }

    private static String escape(String s) {
        return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace("\"", "&quot;");
    }
}
