package com.salesforce.einstein.webcrawler.sitemap;

import com.salesforce.einstein.ds.graph.Graph;
import com.salesforce.einstein.graphexecutor.spi.GraphStore;
import com.salesforce.einstein.webcrawler.model.CanonicalUrl;

import java.util.Optional;

/**
 * Port: named sitemap graphs in a graph store, read a page's successors at a time while a sitemap crawl runs, so a
 * graph far larger than one machine's memory (~100B pages) can be crawled in bounded slices from any node.
 *
 * <p><b>Store layout</b>, which a bulk import must produce for {@link #open} to read it:
 * <ul>
 *   <li>one vertex per page or asset, with {@code key} = {@link CanonicalUrl#value()} as the crawler's
 *       {@code UrlNormalizer} gives it (the {@link CanonicalUrlCodec} encoding), and {@code shard} =
 *       {@link CanonicalUrlCodec#shardByHost}{@code (shards)} of it;</li>
 *   <li>one edge per navigation link, from the linking page to the linked one.</li>
 * </ul>
 * How the vertices and edges are labelled is the adapter's (see its {@code open}).
 *
 * <p>Names match {@code [A-Za-z_][A-Za-z0-9_]{2,62}}, valid as an identifier in every store. Implementations are
 * thread-safe.
 */
public interface SitemapGraphs extends AutoCloseable {

    /**
     * The graph called {@code name}, read-only; the same instance for the same name. It belongs to this object, so
     * callers don't close it. Reading a graph that does not exist (or no longer does) may fail.
     */
    GraphStore<CanonicalUrl> open(String name);

    boolean exists(String name);

    /** Creates graph {@code name} and writes {@code graph} into it. The name must not be in use. */
    void load(String name, Graph<CanonicalUrl> graph);

    /** Deletes graph {@code name}, if it exists. Its catalog entry, if any, stays. */
    void drop(String name);

    // The catalog: an entry per graph loaded through the API. It is kept apart from the graphs, so a bulk import, or
    // a crawl's per-job graph, has none.

    /** Adds {@code entry}, atomically; false if its name already has one. */
    boolean register(SitemapGraphRecord entry);

    Optional<SitemapGraphRecord> record(String name);

    /** Replaces the entry of the same name and {@code loadId}; false if there is none (deleted, or loaded again). */
    boolean update(SitemapGraphRecord entry);

    /** Removes the entry of graph {@code name}, if any. The graph stays. */
    void unregister(String name);

    /** Releases connections. The default has none. */
    @Override
    default void close() { }
}
