package com.salesforce.einstein.graphexecutor.graph;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.function.IntSupplier;

/**
 * Depth-first preorder traversal, iterative so deep graphs cannot overflow the call stack.
 *
 * <p>The stack holds one <em>neighbour iterator</em> per node on the current path, not the neighbours
 * themselves. That gives exactly the order recursive DFS would produce, and the stack is bounded by
 * the path depth rather than by E. (Pushing all neighbours at once, the usual shortcut, visits
 * siblings in reverse and can hold the same node many times.)
 *
 * <p>A node is marked visited when it is <em>returned</em>; that is what lets the path go deep before
 * going wide.
 */
public final class DfsIterator<T> extends TraversalIterator<T> {
    private final Deque<Iterator<? extends T>> stack = new ArrayDeque<>();

    public DfsIterator(Iterable<? extends T> roots, Function<? super T, ? extends Iterable<? extends T>> neighbours) {
        this(roots, neighbours, null);
    }

    DfsIterator(Iterable<? extends T> roots, Function<? super T, ? extends Iterable<? extends T>> neighbours,
                IntSupplier modCount) {
        super(roots, neighbours, modCount);
    }

    @Override
    protected void start(T root) {
        stack.push(List.of(root).iterator());
    }

    @Override
    protected T nextInTree() {
        while (!stack.isEmpty()) {
            Iterator<? extends T> candidates = stack.peek();
            if (!candidates.hasNext()) {
                stack.pop();   // every neighbour of this node is done: backtrack
                continue;
            }
            T node = candidates.next();
            if (visited.add(node)) {   // already seen = a cycle or a cross edge: skip it
                stack.push(neighbours.apply(node).iterator());
                return node;
            }
        }
        return null;
    }
}
