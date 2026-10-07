package com.salesforce.einstein.ds.graph;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.function.Function;
import java.util.function.IntSupplier;

/**
 * Breadth-first traversal: nodes come out in order of distance (edge count) from their root.
 *
 * <p>A node is marked visited when it is <em>enqueued</em>, not when it is dequeued, so a node with
 * many in-edges sits in the queue once. The queue never holds more than V nodes.
 */
public final class BfsIterator<T> extends TraversalIterator<T> {
    private final Deque<T> queue = new ArrayDeque<>();

    public BfsIterator(Iterable<? extends T> roots, Function<? super T, ? extends Iterable<? extends T>> neighbours) {
        this(roots, neighbours, null);
    }

    BfsIterator(Iterable<? extends T> roots, Function<? super T, ? extends Iterable<? extends T>> neighbours,
                IntSupplier modCount) {
        super(roots, neighbours, modCount);
    }

    @Override
    protected void start(T root) {
        visited.add(root);
        queue.add(root);
    }

    @Override
    protected T nextInTree() {
        T node = queue.poll();
        if (node != null) {
            for (T neighbour : neighbours.apply(node)) {
                if (visited.add(neighbour)) {
                    queue.add(neighbour);
                }
            }
        }
        return node;
    }
}
