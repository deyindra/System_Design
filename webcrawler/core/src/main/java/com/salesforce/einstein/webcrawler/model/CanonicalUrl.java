package com.salesforce.einstein.webcrawler.model;

import org.springframework.lang.NonNull;

import java.net.URI;
import java.util.Objects;

/**
 * A URL after normalization. Two raw URLs that normalize to the same {@code value} are the same graph node.
 *
 * @param value canonical form, e.g. {@code https://example.com/a?id=1&lang=en}
 * @param host  lower-case host, the politeness and sharding key
 * @param hash  128-bit hex hash of {@code value}: the node id in every table (64 bits would collide at ~10^10 URLs)
 */
public record CanonicalUrl(String value, String host, String hash) {

    public CanonicalUrl {
        Objects.requireNonNull(value, "value");
        Objects.requireNonNull(host, "host");
        Objects.requireNonNull(hash, "hash");
    }

    public URI uri() { return URI.create(value); }

    public String path() {
        String p = uri().getRawPath();
        return p == null || p.isEmpty() ? "/" : p;
    }

    @Override @NonNull public String toString() { return value; }
}
