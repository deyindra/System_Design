package com.salesforce.einstein.webcrawler.sitemap;

import com.salesforce.einstein.ds.graph.Graph;
import com.salesforce.einstein.graphexecutor.spi.GraphStore;
import com.salesforce.einstein.graphexecutor.spi.memory.InMemoryGraphStore;
import com.salesforce.einstein.webcrawler.model.CanonicalUrl;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Single-process {@link SitemapGraphs}: each graph is an {@link InMemoryGraphStore}. Nodes share it only when they
 * share the instance (tests). A graph that is not loaded reads as empty. Its stores hold nothing to release.
 */
public final class InMemorySitemapGraphs implements SitemapGraphs {

    private static final GraphStore<CanonicalUrl> EMPTY = InMemoryGraphStore.of(Graph.directed(), 1, url -> 0);

    private final int shards;
    private final Map<String, Loaded> graphs = new ConcurrentHashMap<>();
    private final Map<String, SitemapGraphRecord> catalog = new ConcurrentHashMap<>();

    /** A loaded graph's store; held in a record, so the map's values are not resources. */
    private record Loaded(GraphStore<CanonicalUrl> store) { }

    public InMemorySitemapGraphs(int shards) {
        if (shards < 1) throw new IllegalArgumentException("shards must be >= 1");
        this.shards = shards;
    }

    @Override
    public GraphStore<CanonicalUrl> open(String name) {
        Loaded loaded = graphs.get(name);
        return loaded == null ? EMPTY : loaded.store();
    }

    @Override
    public boolean exists(String name) { return graphs.containsKey(name); }

    @Override
    public void load(String name, Graph<CanonicalUrl> graph) {
        Loaded loaded = new Loaded(InMemoryGraphStore.of(graph, shards, CanonicalUrlCodec.shardByHost(shards)));
        if (graphs.putIfAbsent(name, loaded) != null) throw new IllegalStateException("sitemap graph exists: " + name);
    }

    @Override
    public void drop(String name) { graphs.remove(name); }

    @Override
    public boolean register(SitemapGraphRecord entry) { return catalog.putIfAbsent(entry.name(), entry) == null; }

    @Override
    public Optional<SitemapGraphRecord> record(String name) { return Optional.ofNullable(catalog.get(name)); }

    @Override
    public boolean update(SitemapGraphRecord entry) {
        while (true) {
            SitemapGraphRecord current = catalog.get(entry.name());
            if (current == null || !current.loadId().equals(entry.loadId())) return false;
            if (catalog.replace(entry.name(), current, entry)) return true;
        }
    }

    @Override
    public void unregister(String name) { catalog.remove(name); }
}
