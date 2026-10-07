package com.salesforce.einstein.graphexecutor.spi;

import com.salesforce.einstein.ds.graph.Graph;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.ToIntFunction;

/**
 * A read-only directed graph too large for one JVM, kept in a store (a graph database) and read in pieces.
 * It is split into {@link #shards()} shards: every node belongs to exactly one, given by {@link #shardOf}, a
 * pure function that every machine computes the same way, so no machine needs a directory of all nodes.
 *
 * <p>The opposite of {@link Graph}, which is whole, mutable and in memory: here there is no "all nodes" or
 * "node count" call, only pages of one shard and the edges of a batch of nodes, so a reader's memory
 * depends on what it asks for, not on the size of the graph. Reads are batched (one query per batch of
 * nodes, not one per node) because each one is a round-trip to the store.
 *
 * <p>Implementations are thread-safe. Nodes are compared by {@code equals}; pages are ordered by a key
 * that the store chooses and that does not change while it is read.
 *
 * @param <T> node type
 */
public interface GraphStore<T> extends AutoCloseable {

    /** How many shards; shards are numbered 0..shards()-1. */
    int shards();

    /** The node's shard, in [0, shards()). The same answer on every machine, without reading the store. */
    int shardOf(T node);

    /**
     * Up to {@code limit} nodes of {@code shard}, the ones that come after {@code after} in the store's
     * order (null: from the start). An empty page means the shard has no more nodes.
     */
    List<T> nodes(int shard, T after, int limit);

    /** As {@link #nodes}, but only nodes without predecessors (in-degree 0). */
    List<T> sources(int shard, T after, int limit);

    /**
     * The successors of every given node that is in the store (nodes it does not have are left out).
     * Successors may be in any shard.
     */
    Map<T, List<T>> successors(Collection<? extends T> nodes);

    /** As {@link #successors}, in the other direction. */
    Map<T, List<T>> predecessors(Collection<? extends T> nodes);

    /** Releases connections. The default has none. */
    @Override
    default void close() {
    }

    /**
     * A shard function every machine computes identically: the hash of a string key, which (unlike an
     * object's hash) is specified by the JDK, modulo {@code shards}.
     */
    static <T> ToIntFunction<T> byKey(Function<? super T, String> key, int shards) {
        Objects.requireNonNull(key, "key");
        if (shards < 1) {
            throw new IllegalArgumentException("shards must be >= 1");
        }
        return node -> Math.floorMod(key.apply(node).hashCode(), shards);
    }

    /**
     * Reads a whole store into a {@link Graph}, for the one-VM executors when a stored graph turns out to
     * be small enough. O(nodes + edges) memory, so only for graphs that fit.
     */
    static <T> Graph<T> load(GraphStore<T> store, int pageSize) {
        Graph<T> graph = Graph.directed();
        for (int shard = 0; shard < store.shards(); shard++) {
            for (List<T> page = store.nodes(shard, null, pageSize); !page.isEmpty();
                 page = store.nodes(shard, page.get(page.size() - 1), pageSize)) {
                page.forEach(graph::addNode);
                store.successors(page).forEach((from, successors) -> successors.forEach(to -> graph.addEdge(from, to)));
            }
        }
        return graph;
    }
}
