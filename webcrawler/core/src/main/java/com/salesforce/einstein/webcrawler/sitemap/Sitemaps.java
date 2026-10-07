package com.salesforce.einstein.webcrawler.sitemap;

import com.salesforce.einstein.webcrawler.fetch.FetchRequest;
import com.salesforce.einstein.webcrawler.fetch.FetchResult;
import com.salesforce.einstein.webcrawler.fetch.Fetcher;
import com.salesforce.einstein.webcrawler.url.UrlNormalizer;

import java.io.ByteArrayInputStream;
import java.net.URI;

/** Fetches and parses a navigation sitemap, for a crawl request and for the loaders. */
public final class Sitemaps {

    private Sitemaps() { }

    /**
     * One GET, no redirects followed and no robots.txt check (the sitemap is the requester's). Every failure is an
     * {@link IllegalArgumentException} saying what was wrong with the sitemap.
     *
     * @param maxBytes a larger file is refused
     */
    public static NavigationSitemap fetch(Fetcher fetcher, UrlNormalizer normalizer, String url, long maxBytes) {
        URI uri;
        try {
            uri = URI.create(url);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("sitemap URL is not a URI: " + url, e);
        }
        FetchResult result = fetcher.fetch(FetchRequest.of(uri, maxBytes));
        if (result.status() != 200)
            throw new IllegalArgumentException("sitemap " + url + " could not be fetched: "
                    + (result.error() != null ? result.error() : "HTTP " + result.status()));
        if (result.truncated()) throw new IllegalArgumentException("sitemap " + url + " is larger than " + maxBytes + " bytes");
        return new NavigationSitemapParser(normalizer).parse(new ByteArrayInputStream(result.body()));
    }
}
