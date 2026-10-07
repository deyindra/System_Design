package com.salesforce.einstein.ds.graph;

import java.util.Collection;
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;

/**
 * A simple graph over nodes of type {@code T}: at most one edge per ordered pair (directed) or unordered
 * pair (undirected). Self-loops are allowed.
 *
 * <p><b>Two independent properties</b>, fixed when the graph is created:
 * <ul>
 *   <li><b>directed</b> (unidirectional) or <b>undirected</b> (bidirectional): in an undirected graph
 *       {@code a-b} and {@code b-a} are the same edge, so every edge method accepts either order;</li>
 *   <li><b>weighted</b> or <b>unweighted</b>: a weighted graph requires a weight on every edge; an
 *       unweighted graph has none ({@link #edgeWeight} is always empty).</li>
 * </ul>
 * Calling the edge method of the other kind ({@code addEdge(a, b)} on a weighted graph,
 * {@code addEdge(a, b, w)} or {@code updateEdge} on an unweighted one) throws
 * {@link UnsupportedOperationException}.
 *
 * <p>Nodes are identified by {@code equals}/{@code hashCode}, which must not change while a node is in
 * the graph; use {@link #updateNode} to replace one. Null nodes are rejected.
 *
 * <p>Mutators return {@code false} when they change nothing (adding something already present,
 * removing or updating something absent), so callers never need a separate {@code contains} check.
 *
 * <p>Not thread-safe. Traversal iterators are fail-fast: they throw
 * {@link java.util.ConcurrentModificationException} if the graph changes underneath them.
 */
public interface Graph<T> {

    // ---- creation ----

    /** Unweighted, unidirectional. */
    static <T> Graph<T> directed() {
        return new AdjacencyGraph<>(true, false);
    }

    /** Unweighted, bidirectional. */
    static <T> Graph<T> undirected() {
        return new AdjacencyGraph<>(false, false);
    }

    /** Weighted, unidirectional. */
    static <T> Graph<T> weightedDirected() {
        return new AdjacencyGraph<>(true, true);
    }

    /** Weighted, bidirectional. */
    static <T> Graph<T> weightedUndirected() {
        return new AdjacencyGraph<>(false, true);
    }

    // ---- properties ----

    boolean isDirected();

    boolean isWeighted();

    // ---- nodes ----

    boolean addNode(T node);

    /** Removes the node and every edge touching it. */
    boolean removeNode(T node);

    /**
     * Replaces {@code oldNode} with {@code newNode}, moving all of its edges (and their weights) over.
     *
     * @return false if {@code oldNode} is absent
     * @throws IllegalArgumentException if {@code newNode} is already a different node in the graph
     */
    boolean updateNode(T oldNode, T newNode);

    boolean containsNode(T node);

    /** Unmodifiable live view, in insertion order. */
    Set<T> nodes();

    int nodeCount();

    // ---- edges ----

    /**
     * Adds an unweighted edge, adding missing endpoints first. Returns false if it already exists.
     *
     * @throws UnsupportedOperationException if this graph is weighted
     */
    boolean addEdge(T from, T to);

    /**
     * Adds a weighted edge, adding missing endpoints first. Returns false (weight untouched) if it exists.
     *
     * @throws UnsupportedOperationException if this graph is unweighted
     */
    boolean addEdge(T from, T to, double weight);

    boolean removeEdge(T from, T to);

    /**
     * Changes the weight of an existing edge; returns false if there is no such edge.
     *
     * @throws UnsupportedOperationException if this graph is unweighted
     */
    boolean updateEdge(T from, T to, double weight);

    boolean containsEdge(T from, T to);

    /** The edge's weight; empty if the graph is unweighted or there is no such edge. */
    Optional<Double> edgeWeight(T from, T to);

    int edgeCount();

    // ---- adjacency (each throws IllegalArgumentException if the node is absent) ----

    /** Nodes one edge away, following direction. Undirected: all neighbors. Unmodifiable live view. */
    Set<T> successors(T node);

    /** Nodes with an edge into {@code node}. Undirected: same as {@link #successors}. Unmodifiable live view. */
    Set<T> predecessors(T node);

    /** Undirected: the degree (a self-loop counts once). */
    default int outDegree(T node) {
        return successors(node).size();
    }

    /** Undirected: the degree (a self-loop counts once). */
    default int inDegree(T node) {
        return predecessors(node).size();
    }

    /**
     * An independent copy, with the same properties, of the subgraph induced by {@code keep}: those nodes
     * (in this graph's insertion order) and every edge, with its weight, between two of them. Nodes not in
     * this graph are ignored.
     */
    Graph<T> subgraph(Collection<? extends T> keep);

    // ---- traversal ----

    /** Breadth-first over the nodes reachable from {@code start}; each node once, even with cycles. */
    Iterator<T> bfs(T start);

    /** Depth-first preorder over the nodes reachable from {@code start}; each node once, even with cycles. */
    Iterator<T> dfs(T start);

    /** Breadth-first over every node, starting a new tree at each node not yet reached (insertion order). */
    Iterator<T> bfs();

    /** Depth-first over every node, starting a new tree at each node not yet reached (insertion order). */
    Iterator<T> dfs();
}
