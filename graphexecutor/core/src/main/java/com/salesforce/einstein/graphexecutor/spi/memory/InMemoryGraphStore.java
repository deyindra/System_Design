package com.salesforce.einstein.graphexecutor.spi.memory;

import com.salesforce.einstein.ds.graph.Graph;
import com.salesforce.einstein.graphexecutor.spi.GraphStore;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.ToIntFunction;

/**
 * An in-memory store over a copy of a {@link Graph}, its nodes listed per shard in insertion order. Used to test
 * code written against stores, and to run it on graphs that do fit in memory.
 */
public final class InMemoryGraphStore<T> implements GraphStore<T> {
    private final Graph<T> graph;
    private final int shards;
    private final ToIntFunction<? super T> shardOf;
    private final List<List<T>> byShard = new ArrayList<>();
    private final Map<T, Integer> position = new HashMap<>();   // index in its shard's list: the page order

    /** A store over a copy of {@code graph}; pages follow the graph's insertion order. */
    public static <T> InMemoryGraphStore<T> of(Graph<T> graph, int shards, ToIntFunction<? super T> shardOf) {
        return new InMemoryGraphStore<>(graph, shards, shardOf);
    }

    private InMemoryGraphStore(Graph<T> graph, int shards, ToIntFunction<? super T> shardOf) {
        if (!graph.isDirected()) {
            throw new IllegalArgumentException("needs a directed graph");
        }
        if (shards < 1) {
            throw new IllegalArgumentException("shards must be >= 1");
        }
        this.graph = graph.subgraph(graph.nodes());
        this.shards = shards;
        this.shardOf = Objects.requireNonNull(shardOf, "shardOf");
        for (int s = 0; s < shards; s++) {
            byShard.add(new ArrayList<>());
        }
        for (T node : this.graph.nodes()) {
            List<T> shard = byShard.get(shardOf(node));
            position.put(node, shard.size());
            shard.add(node);
        }
    }

    @Override
    public int shards() {
        return shards;
    }

    @Override
    public int shardOf(T node) {
        int shard = shardOf.applyAsInt(node);
        if (shard < 0 || shard >= shards) {
            throw new IllegalStateException("shardOf(" + node + ") = " + shard + ", not in [0, " + shards + ")");
        }
        return shard;
    }

    @Override
    public List<T> nodes(int shard, T after, int limit) {
        return page(shard, after, limit, node -> true);
    }

    @Override
    public List<T> sources(int shard, T after, int limit) {
        return page(shard, after, limit, node -> graph.inDegree(node) == 0);
    }

    private List<T> page(int shard, T after, int limit, Predicate<T> keep) {
        List<T> nodes = byShard.get(shard);
        List<T> page = new ArrayList<>();
        for (int i = after == null ? 0 : position.get(after) + 1; i < nodes.size() && page.size() < limit; i++) {
            if (keep.test(nodes.get(i))) {
                page.add(nodes.get(i));
            }
        }
        return page;
    }

    @Override
    public Map<T, List<T>> successors(Collection<? extends T> nodes) {
        return adjacency(nodes, graph::successors);
    }

    @Override
    public Map<T, List<T>> predecessors(Collection<? extends T> nodes) {
        return adjacency(nodes, graph::predecessors);
    }

    private Map<T, List<T>> adjacency(Collection<? extends T> nodes, Function<T, Collection<T>> edges) {
        Map<T, List<T>> adjacency = new LinkedHashMap<>();
        for (T node : nodes) {
            if (graph.containsNode(node)) {
                adjacency.put(node, List.copyOf(edges.apply(node)));
            }
        }
        return adjacency;
    }
}
