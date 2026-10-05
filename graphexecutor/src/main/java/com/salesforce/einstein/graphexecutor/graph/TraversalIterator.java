package com.salesforce.einstein.graphexecutor.graph;

import java.util.ConcurrentModificationException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.IntSupplier;

/**
 * Shared skeleton of {@link BfsIterator} and {@link DfsIterator}.
 *
 * <p><b>Cycles:</b> a {@code visited} set guarantees each node is returned at most once, so the
 * traversal terminates on any graph, cyclic or not, in O(V + E) time and O(V) extra space.
 *
 * <p><b>Several roots:</b> when the current tree is exhausted the next root that has not been reached
 * yet starts a new one. Passing every node as a root therefore covers disconnected graphs too.
 *
 * <p><b>Lazy:</b> work happens in {@code hasNext}/{@code next}, one node at a time, so a caller can stop
 * early without paying for the rest of the graph.
 *
 * <p>The neighbour function is plain {@code node -> neighbours}, so the same iterators work on any
 * graph-like structure, or on a different <em>view</em> of a graph (e.g. a directed graph's edges
 * followed in both directions).
 */
public abstract class TraversalIterator<T> implements Iterator<T> {
    protected final Function<? super T, ? extends Iterable<? extends T>> neighbours;
    protected final Set<T> visited = new HashSet<>();
    private final Iterator<? extends T> roots;
    private final IntSupplier modCount;   // null = no fail-fast check
    private final int expectedModCount;
    private T lookahead;                  // next node to return; null = not computed yet

    protected TraversalIterator(Iterable<? extends T> roots,
                                Function<? super T, ? extends Iterable<? extends T>> neighbours,
                                IntSupplier modCount) {
        this.roots = roots.iterator();
        this.neighbours = Objects.requireNonNull(neighbours, "neighbours");
        this.modCount = modCount;
        this.expectedModCount = modCount == null ? 0 : modCount.getAsInt();
    }

    /** Begins a new tree at {@code root}, which has not been visited. */
    protected abstract void start(T root);

    /** Next node of the current tree, marking it visited; null once the tree is exhausted. */
    protected abstract T nextInTree();

    @Override
    public final boolean hasNext() {
        if (modCount != null && modCount.getAsInt() != expectedModCount) {
            throw new ConcurrentModificationException("graph changed during traversal");
        }
        if (lookahead == null) {
            lookahead = advance();
        }
        return lookahead != null;
    }

    @Override
    public final T next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        T node = lookahead;
        lookahead = null;
        return node;
    }

    private T advance() {
        T node;
        while ((node = nextInTree()) == null) {
            T root = nextUnvisitedRoot();
            if (root == null) {
                return null;
            }
            start(root);
        }
        return node;
    }

    private T nextUnvisitedRoot() {
        while (roots.hasNext()) {
            T root = Objects.requireNonNull(roots.next(), "root");
            if (!visited.contains(root)) {
                return root;
            }
        }
        return null;
    }
}
