package com.salesforce.einstein.graphexecutor.executor;

import com.salesforce.einstein.ds.graph.Graph;

import java.util.Set;

/**
 * One set of inter-connected nodes, independent of every other group: no edge, in either direction,
 * joins it to a node outside it.
 *
 * <p>{@code graph} is a private copy of this group's induced subgraph, taken before processing starts.
 * Workers read only their own copy, so groups never share mutable state and the caller may keep
 * changing the original graph.
 *
 * @param id    0-based, in order of each group's first node in the source graph
 * @param graph this group's nodes and the edges between them; treat as read-only
 */
public record TaskGroup<T>(int id, Graph<T> graph) {

    public Set<T> nodes() {
        return graph.nodes();
    }

    public int size() {
        return graph.nodeCount();
    }
}
