package com.salesforce.einstein.webcrawler.api;

import com.salesforce.einstein.webcrawler.sitemap.SitemapGraphRecord;
import com.salesforce.einstein.webcrawler.sitemap.SitemapGraphService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.net.URI;

import static com.salesforce.einstein.webcrawler.api.CrawlController.TENANT;

/**
 * Named sitemap graphs, for crawls that set {@code sitemapGraph}. A graph belongs to the tenant that loaded it;
 * another tenant's is a 404, as for crawls.
 */
@RestController
@Validated
@RequestMapping("/v1/sitemap-graphs")
@Tag(name = "Sitemap graphs")
public class SitemapGraphController {

    private final SitemapGraphService graphs;

    public SitemapGraphController(SitemapGraphService graphs) {
        this.graphs = graphs;
    }

    @Operation(summary = "Load a sitemap as a named graph",
            description = "202 + Location while it loads in the background; GET it until READY, then crawl with "
                    + "sitemapGraph and its roots as seeds. Repeating the same PUT is harmless (200 once it has "
                    + "finished); a name already in use is a 409.")
    @PutMapping(value = "/{name}", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<ApiModels.SitemapGraphView> load(@RequestHeader(TENANT) String tenant, @PathVariable String name,
                                                           @Valid @RequestBody ApiModels.LoadSitemapGraph body) {
        SitemapGraphRecord g = graphs.load(tenant, name, body.sitemapUrl());
        ApiModels.SitemapGraphView view = ApiModels.SitemapGraphView.of(g);
        if (g.status() != SitemapGraphRecord.Status.LOADING) return ResponseEntity.ok(view);
        return ResponseEntity.accepted().location(URI.create("/v1/sitemap-graphs/" + name)).body(view);
    }

    @Operation(summary = "A sitemap graph and its load: LOADING, READY (with its roots) or FAILED (with why)")
    @GetMapping(value = "/{name}", produces = MediaType.APPLICATION_JSON_VALUE)
    public ApiModels.SitemapGraphView get(@RequestHeader(TENANT) String tenant, @PathVariable String name) {
        return graphs.get(tenant, name).map(ApiModels.SitemapGraphView::of)
                .orElseThrow(() -> new NotFoundException("sitemap graph not found: " + name));
    }

    @Operation(summary = "Delete a sitemap graph, in any state",
            description = "Crawls still reading it stop finding successors.")
    @DeleteMapping("/{name}")
    public ResponseEntity<Void> delete(@RequestHeader(TENANT) String tenant, @PathVariable String name) {
        if (!graphs.delete(tenant, name)) throw new NotFoundException("sitemap graph not found: " + name);
        return ResponseEntity.noContent().build();
    }
}
