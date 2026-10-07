package com.salesforce.einstein.webcrawler.sitemap;

import java.io.Serial;

/** The name is taken: by another load, another tenant, or a graph that was not loaded through the API. */
public class SitemapGraphConflictException extends RuntimeException {
    @Serial
    private static final long serialVersionUID = 1L;

    public SitemapGraphConflictException(String message) { super(message); }
}
