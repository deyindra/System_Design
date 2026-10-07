package com.salesforce.einstein.webcrawler.sitemap;

import com.salesforce.einstein.graphexecutor.spi.GraphStore;
import com.salesforce.einstein.graphexecutor.spi.NodeCodec;
import com.salesforce.einstein.webcrawler.model.CanonicalUrl;
import com.salesforce.einstein.webcrawler.url.Hashing;

import java.net.URI;
import java.util.function.ToIntFunction;

/**
 * A {@link CanonicalUrl} as a graph-database key: its canonical {@code value}. The host and hash are derived from
 * the value (as {@code UrlNormalizer} derives them), so decoding needs no lookup and gives back an equal URL.
 */
public final class CanonicalUrlCodec implements NodeCodec<CanonicalUrl> {

    public static final CanonicalUrlCodec INSTANCE = new CanonicalUrlCodec();

    private CanonicalUrlCodec() { }

    /** The shard function a sitemap graph is stored with: every page of a host in one shard. */
    public static ToIntFunction<CanonicalUrl> shardByHost(int shards) {
        return GraphStore.byKey(CanonicalUrl::host, shards);
    }

    @Override public String encode(CanonicalUrl url) { return url.value(); }

    @Override public CanonicalUrl decode(String value) {
        return new CanonicalUrl(value, URI.create(value).getHost(), Hashing.urlHash(value));
    }
}
