package com.salesforce.einstein.webcrawler.model;

/**
 * A directed edge of the crawl graph. Edges are recorded even when the target was already visited
 * (that is how cycles show up in the result) or was not followed (out of scope, too deep).
 *
 * @param rawHref the attribute value exactly as written in the page, used by the link rewriter
 */
public record LinkEdge(String jobId, String fromHash, String toHash, String toUrl,
                       ResourceType type, String rawHref) { }
