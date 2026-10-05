package com.salesforce.einstein.graphexecutor.graph;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * The one {@link Graph} implementation: an adjacency map whose behavior is set by two flags, so all four
 * kinds (directed or undirected, weighted or not) share every line of code. Create it through the
 * factories on {@link Graph}.
 *
 * <p><b>Direction.</b> Every edge {@code a -> b} is stored twice: {@code out[a][b]} and {@code in[b][a]}.
 * For a directed graph {@code in} is a separate reverse index, so removing a node costs O(degree)
 * instead of a scan of every node. For an undirected graph {@code in} <em>is</em> {@code out}: writing
 * {@code in[b][a]} is exactly the mirror entry an undirected edge needs.
 *
 * <p><b>Weight.</b> Each entry maps a neighbor to an {@code Optional<Double>}: always present in a
 * weighted graph, always {@code Optional.empty()} (a shared singleton, so free) in an unweighted one.
 * The public methods enforce that, so the stored value never disagrees with {@link #isWeighted()}.
 *
 * <p>Costs (expected, hash-based): add/remove/update/contains edge O(1); add node O(1);
 * remove node O(degree); update node O(degree).
 */
final class AdjacencyGraph<T> implements Graph<T> {
    // node -> (successor -> weight). LinkedHashMap keeps iteration, and so traversal, deterministic.
    private final Map<T, Map<T, Optional<Double>>> out = new LinkedHashMap<>();
    // node -> (predecessor -> weight); the same map as `out` when undirected
    private final Map<T, Map<T, Optional<Double>>> in;
    private final boolean weighted;
    private int edgeCount;
    // bumped on every structural or weight change; traversal iterators use it to fail fast
    private int modCount;

    AdjacencyGraph(boolean directed, boolean weighted) {
        this.in = directed ? new LinkedHashMap<>() : out;
        this.weighted = weighted;
    }

    @Override
    public boolean isDirected() {
        return in != out;
    }

    @Override
    public boolean isWeighted() {
        return weighted;
    }

    // ---- nodes ----

    @Override
    public boolean addNode(T node) {
        Objects.requireNonNull(node, "node");
        if (out.containsKey(node)) {
            return false;
        }
        out.put(node, new LinkedHashMap<>());
        if (isDirected()) {
            in.put(node, new LinkedHashMap<>());
        }
        modCount++;
        return true;
    }

    @Override
    public boolean removeNode(T node) {
        if (!containsNode(node)) {
            return false;
        }
        // copy first: removeEdge mutates these maps (and for a self-loop, the very map being read)
        for (T successor : List.copyOf(out.get(node).keySet())) {
            removeEdge(node, successor);
        }
        for (T predecessor : List.copyOf(in.get(node).keySet())) {   // empty here when undirected
            removeEdge(predecessor, node);
        }
        out.remove(node);
        in.remove(node);
        modCount++;
        return true;
    }

    @Override
    public boolean updateNode(T oldNode, T newNode) {
        Objects.requireNonNull(newNode, "newNode");
        if (!containsNode(oldNode)) {
            return false;
        }
        if (oldNode.equals(newNode)) {
            return true;
        }
        if (containsNode(newNode)) {
            throw new IllegalArgumentException("node already exists: " + newNode);
        }
        addNode(newNode);
        new LinkedHashMap<>(out.get(oldNode)).forEach((successor, weight) ->
                putEdge(newNode, successor.equals(oldNode) ? newNode : successor, weight.orElse(null)));
        new LinkedHashMap<>(in.get(oldNode)).forEach((predecessor, weight) -> {
            if (!predecessor.equals(oldNode)) {   // the self-loop was moved above
                putEdge(predecessor, newNode, weight.orElse(null));   // no-op when undirected: already added above
            }
        });
        removeNode(oldNode);
        return true;
    }

    @Override
    public boolean containsNode(T node) {
        return node != null && out.containsKey(node);
    }

    @Override
    public Set<T> nodes() {
        return Collections.unmodifiableSet(out.keySet());
    }

    @Override
    public int nodeCount() {
        return out.size();
    }

    // ---- edges ----

    @Override
    public boolean addEdge(T from, T to) {
        if (weighted) {
            throw new UnsupportedOperationException("weighted graph: use addEdge(from, to, weight)");
        }
        return putEdge(from, to, null);
    }

    @Override
    public boolean addEdge(T from, T to, double weight) {
        requireWeighted();
        return putEdge(from, to, weight);
    }

    // the single writer of new edges; weight is null for an unweighted graph (callers guarantee it
    // matches `weighted`) and becomes the stored Optional here
    private boolean putEdge(T from, T to, Double weight) {
        Objects.requireNonNull(from, "from");
        Objects.requireNonNull(to, "to");
        addNode(from);
        addNode(to);
        Map<T, Optional<Double>> successors = out.get(from);
        if (successors.containsKey(to)) {
            return false;
        }
        Optional<Double> stored = Optional.ofNullable(weight);
        successors.put(to, stored);
        in.get(to).put(from, stored);
        edgeCount++;
        modCount++;
        return true;
    }

    @Override
    public boolean removeEdge(T from, T to) {
        Map<T, Optional<Double>> successors = from == null ? null : out.get(from);
        if (successors == null || !successors.containsKey(to)) {
            return false;
        }
        successors.remove(to);
        in.get(to).remove(from);   // already gone for an undirected self-loop; harmless
        edgeCount--;
        modCount++;
        return true;
    }

    @Override
    public boolean updateEdge(T from, T to, double weight) {
        requireWeighted();
        if (!containsEdge(from, to)) {
            return false;
        }
        out.get(from).put(to, Optional.of(weight));
        in.get(to).put(from, Optional.of(weight));
        modCount++;
        return true;
    }

    @Override
    public boolean containsEdge(T from, T to) {
        Map<T, Optional<Double>> successors = from == null ? null : out.get(from);
        return successors != null && to != null && successors.containsKey(to);
    }

    @Override
    public Optional<Double> edgeWeight(T from, T to) {
        return containsEdge(from, to) ? out.get(from).get(to) : Optional.empty();
    }

    @Override
    public int edgeCount() {
        return edgeCount;
    }

    private void requireWeighted() {
        if (!weighted) {
            throw new UnsupportedOperationException("unweighted graph: edges carry no weight");
        }
    }

    // ---- adjacency ----

    @Override
    public Set<T> successors(T node) {
        return Collections.unmodifiableSet(adjacency(out, node).keySet());
    }

    @Override
    public Set<T> predecessors(T node) {
        return Collections.unmodifiableSet(adjacency(in, node).keySet());
    }

    private Map<T, Optional<Double>> adjacency(Map<T, Map<T, Optional<Double>>> index, T node) {
        Map<T, Optional<Double>> adjacent = node == null ? null : index.get(node);
        if (adjacent == null) {
            throw new IllegalArgumentException("no such node: " + node);
        }
        return adjacent;
    }

    @Override
    public Graph<T> subgraph(Collection<? extends T> keep) {
        Set<? extends T> wanted = Set.copyOf(keep);
        AdjacencyGraph<T> copy = new AdjacencyGraph<>(isDirected(), weighted);
        for (T node : nodes()) {
            if (wanted.contains(node)) {
                copy.addNode(node);
            }
        }
        for (T from : copy.nodes()) {
            out.get(from).forEach((to, weight) -> {
                if (copy.containsNode(to)) {
                    copy.putEdge(from, to, weight.orElse(null));
                }
            });
        }
        return copy;
    }

    // ---- traversal ----

    @Override
    public Iterator<T> bfs(T start) {
        return new BfsIterator<>(List.of(requireNode(start)), this::successors, () -> modCount);
    }

    @Override
    public Iterator<T> dfs(T start) {
        return new DfsIterator<>(List.of(requireNode(start)), this::successors, () -> modCount);
    }

    @Override
    public Iterator<T> bfs() {
        return new BfsIterator<>(nodes(), this::successors, () -> modCount);
    }

    @Override
    public Iterator<T> dfs() {
        return new DfsIterator<>(nodes(), this::successors, () -> modCount);
    }

    private T requireNode(T node) {
        if (!containsNode(node)) {
            throw new IllegalArgumentException("no such node: " + node);
        }
        return node;
    }

    /**
     * Adjacency form, prefixed by the properties: {@code Graph[directed]{a=[b], b=[]}}, or with weights
     * {@code Graph[directed, weighted]{a={b=2.5}, b={}}}. Undirected edges appear under both ends.
     */
    @Override
    public String toString() {
        Map<T, Object> adjacency = new LinkedHashMap<>();
        out.forEach((node, successors) -> {
            if (weighted) {
                Map<T, Double> weights = new LinkedHashMap<>();
                successors.forEach((to, weight) -> weights.put(to, weight.orElseThrow()));
                adjacency.put(node, weights);
            } else {
                adjacency.put(node, successors.keySet());
            }
        });
        return "Graph[" + (isDirected() ? "directed" : "undirected") + (weighted ? ", weighted" : "") + "]"
                + adjacency;
    }
}
