package com.salesforce.einstein.webcrawler.api;

import com.salesforce.einstein.webcrawler.config.CrawlerProperties;
import com.salesforce.einstein.webcrawler.engine.CrawlEngine;
import com.salesforce.einstein.webcrawler.model.CrawlJob;
import com.salesforce.einstein.webcrawler.model.CrawlMode;
import com.salesforce.einstein.webcrawler.model.CrawlRequest;
import com.salesforce.einstein.webcrawler.model.JobPage;
import com.salesforce.einstein.webcrawler.model.LinkEdge;
import com.salesforce.einstein.webcrawler.model.ResourceType;
import com.salesforce.einstein.webcrawler.render.CrawlResults;
import com.salesforce.einstein.webcrawler.render.LinkMode;
import com.salesforce.einstein.webcrawler.store.JobStore;
import com.salesforce.einstein.webcrawler.store.Slice;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Crawl jobs and their results. The tenant comes from {@code X-Tenant-Id}, which the gateway sets from the
 * verified token. Another tenant's job is a 404, never a 403, so job ids can't be probed.
 */
@RestController
@Validated
@RequestMapping("/v1/crawls")
@Tag(name = "Crawls")
public class CrawlController {

    static final String TENANT = "X-Tenant-Id";

    private final CrawlEngine engine;
    private final JobStore jobs;
    private final CrawlResults results;
    private final Duration maxSyncWait;

    public CrawlController(CrawlEngine engine, CrawlResults results, CrawlerProperties props) {
        this.engine = engine;
        this.jobs = engine.jobs();
        this.results = results;
        this.maxSyncWait = props.maxSyncWait();
    }

    @Operation(summary = "Start a crawl",
            description = "ASYNC → 202 + Location. SYNC (maxDepth ≤ 1, maxPages ≤ 25) → 200 with every node if it "
                    + "finishes within syncTimeoutMs, otherwise 202 and the crawl continues in the background.")
    @PostMapping(consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Object> create(@RequestHeader(TENANT) String tenant,
                                         @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
                                         @Valid @RequestBody ApiModels.CreateCrawl body) throws InterruptedException {
        CrawlRequest req = body.toDomain(tenant);
        CrawlJob job = engine.submit(req, idempotencyKey);
        URI location = URI.create("/v1/crawls/" + job.jobId());

        if (req.mode() == CrawlMode.ASYNC) return ResponseEntity.accepted().location(location).body(view(job));

        long waitMs = Math.min(body.syncTimeoutMs() == null ? 10_000 : body.syncTimeoutMs(), maxSyncWait.toMillis());
        CrawlJob done = engine.await(job.jobId(), Duration.ofMillis(waitMs));
        if (!done.status().isTerminal())   // deadline passed: same contract as ASYNC from here on
            return ResponseEntity.accepted().location(location).header(HttpHeaders.RETRY_AFTER, "2").body(view(done));

        List<ApiModels.PageView> pages = new ArrayList<>();
        String cursor = null;
        do {
            Slice<JobPage> s = jobs.pages(done.jobId(), null, cursor, 500);
            s.items().forEach(p -> pages.add(ApiModels.PageView.of(p)));
            cursor = s.nextCursor();
        } while (cursor != null);
        return ResponseEntity.ok().location(location).body(new ApiModels.SyncResult(view(done), pages));
    }

    @Operation(summary = "Status and live counters")
    @GetMapping("/{jobId}")
    public ApiModels.JobView get(@RequestHeader(TENANT) String tenant, @PathVariable String jobId) {
        return view(owned(tenant, jobId));
    }

    @Operation(summary = "Cancel; queued URLs are dropped without contacting their hosts")
    @PostMapping("/{jobId}/cancel")
    public ApiModels.JobView cancel(@RequestHeader(TENANT) String tenant, @PathVariable String jobId) {
        engine.cancel(owned(tenant, jobId).jobId());
        return view(jobs.get(jobId).orElseThrow());
    }

    @Operation(summary = "Nodes of the crawl graph in BFS (discovery) order, keyset-paginated")
    @GetMapping("/{jobId}/pages")
    public ApiModels.PageList pages(@RequestHeader(TENANT) String tenant, @PathVariable String jobId,
                               @Parameter(description = "PAGE, IMAGE, VIDEO, AUDIO, CSS, SCRIPT, FONT, DOCUMENT, OTHER")
                               @RequestParam(required = false) String type,
                               @RequestParam(required = false) String cursor,
                               @RequestParam(defaultValue = "100") @Min(1) @Max(1000) int limit) {
        owned(tenant, jobId);
        ResourceType t = type == null ? null : ResourceType.valueOf(type.toUpperCase(Locale.ROOT));
        Slice<JobPage> s = jobs.pages(jobId, t, cursor, limit);
        return new ApiModels.PageList(s.items().stream().map(ApiModels.PageView::of).toList(), s.nextCursor());
    }

    @Operation(summary = "One node and its out-links (edges are kept even to visited or unfollowed targets)")
    @GetMapping("/{jobId}/pages/{urlHash}")
    public ApiModels.PageDetail page(@RequestHeader(TENANT) String tenant, @PathVariable String jobId, @PathVariable String urlHash) {
        owned(tenant, jobId);
        JobPage p = jobs.page(jobId, urlHash).orElseThrow(() -> new NotFoundException("no such page in this crawl"));
        List<ApiModels.LinkView> links = new ArrayList<>();
        for (LinkEdge e : jobs.outLinks(jobId, urlHash))
            links.add(ApiModels.LinkView.of(e, jobs.page(jobId, e.toHash()).isPresent()));
        return new ApiModels.PageDetail(ApiModels.PageView.of(p), links);
    }

    @Operation(summary = "Stored bytes of a node",
            description = "links=snapshot (default): references to anything this crawl stored point at our copy, "
                    + "the rest are absolute to the origin. links=absolute: all absolute. links=raw: bytes as fetched. "
                    + "Redirects and duplicates resolve to the node that holds the content.")
    @GetMapping("/{jobId}/pages/{urlHash}/content")
    public ResponseEntity<byte[]> content(@RequestHeader(TENANT) String tenant, @PathVariable String jobId,
                                          @PathVariable String urlHash,
                                          @RequestParam(defaultValue = "snapshot") String links) {
        CrawlJob job = owned(tenant, jobId);
        LinkMode mode = LinkMode.valueOf(links.toUpperCase(Locale.ROOT));
        CrawlResults.Content c = results.content(jobId, urlHash, mode, "/v1/crawls/" + jobId)
                .orElseThrow(() -> new NotFoundException("this node has no stored content"));
        return ResponseEntity.ok()
                .header(HttpHeaders.CONTENT_TYPE, c.contentType() == null ? MediaType.APPLICATION_OCTET_STREAM_VALUE : c.contentType())
                // A pure function of (content, job, mode) once the job is done; while it runs, children keep arriving
                // and links switch from absolute to local, so nothing may be cached yet.
                .header(HttpHeaders.CACHE_CONTROL, job.status().isTerminal() ? "private, max-age=3600" : "no-store")
                .header("Content-Security-Policy", "sandbox")                    // never run crawled scripts on our origin
                .header("X-Content-Type-Options", "nosniff")
                .body(c.bytes());
    }

    @Operation(summary = "Offline, browsable ZIP of the whole crawl; links rewritten to relative paths")
    @GetMapping(value = "/{jobId}/export", produces = "application/zip")
    public ResponseEntity<StreamingResponseBody> export(@RequestHeader(TENANT) String tenant, @PathVariable String jobId) {
        CrawlJob job = owned(tenant, jobId);
        if (!job.status().isTerminal())
            return ResponseEntity.status(HttpStatus.CONFLICT).build();   // export a consistent graph, not a moving one
        return ResponseEntity.ok()
                .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"crawl-" + jobId + ".zip\"")
                .body(out -> results.export(job, out));
    }

    private CrawlJob owned(String tenant, String jobId) {
        return jobs.get(jobId).filter(j -> j.tenantId().equals(tenant))
                .orElseThrow(() -> new NotFoundException("no such crawl"));
    }

    private ApiModels.JobView view(CrawlJob j) { return ApiModels.JobView.of(j, jobs.stats(j.jobId())); }
}
