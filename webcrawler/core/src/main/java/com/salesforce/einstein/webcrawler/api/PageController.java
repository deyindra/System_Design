package com.salesforce.einstein.webcrawler.api;

import com.salesforce.einstein.webcrawler.engine.CrawlEngine;
import com.salesforce.einstein.webcrawler.model.CanonicalUrl;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * "Have we already crawled this?": the tenant's latest copy of a URL across its own jobs (metadata and a link to the
 * content). Scoped to the tenant on purpose: the global snapshot cache is shared by all tenants for reuse, but
 * exposing it would tell one tenant what another has crawled.
 */
@RestController
@RequestMapping("/v1/pages")
@Tag(name = "Pages")
public class PageController {

    private final CrawlEngine engine;

    public PageController(CrawlEngine engine) { this.engine = engine; }

    @Operation(summary = "The calling tenant's latest copy of a URL, after canonicalization",
            description = "404 if none of this tenant's jobs stored it, even if another tenant did.")
    @GetMapping
    public ApiModels.SnapshotView latest(@RequestHeader(CrawlController.TENANT) String tenant, @RequestParam String url) {
        CanonicalUrl c = engine.normalizer().normalize(url)
                .orElseThrow(() -> new IllegalArgumentException("not an http(s) URL: " + url));
        return engine.jobs().latestForTenant(tenant, c.hash()).map(ApiModels.SnapshotView::of)
                .orElseThrow(() -> new NotFoundException("no snapshot of " + c.value()));
    }
}
