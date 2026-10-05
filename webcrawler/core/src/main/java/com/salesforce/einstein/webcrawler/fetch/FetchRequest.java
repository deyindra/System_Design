package com.salesforce.einstein.webcrawler.fetch;

import java.net.URI;

/**
 * @param etag         from the previous snapshot; sent as {@code If-None-Match}
 * @param lastModified from the previous snapshot; sent as {@code If-Modified-Since}
 * @param maxBytes     the body is cut off (and the result marked truncated) beyond this
 */
public record FetchRequest(URI url, String etag, String lastModified, long maxBytes) {

    public static FetchRequest of(URI url, long maxBytes) { return new FetchRequest(url, null, null, maxBytes); }
}
