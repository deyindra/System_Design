package com.salesforce.einstein.ds.queue;

import org.jetbrains.annotations.NotNull;
import java.util.AbstractQueue;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * A binary min-heap that also indexes each element's heap position, giving
 * O(log N) {@link #remove(Object)}, O(1) {@link #contains(Object)} and an
 * {@link #update(Object)} operation for re-keying an element in place.
 *
 * <p>Each heap slot holds a small node that records its own index, so sifting is pure array
 * work. The {@code HashMap} index (element -> nodes) is touched only when an element enters or
 * leaves the queue, once per operation rather than once per sift step.
 *
 * <p>Duplicates (per {@code equals}) are allowed; equal elements share one node set (a
 * {@link LinkedHashSet}, so adding, removing and picking a node are O(1)). Costs, where
 * "+ hash" is one expected-O(1) {@code HashMap} operation given a reasonable {@code hashCode}:
 * <ul>
 *   <li>{@code offer}, {@code poll}, {@code remove(Object)}: O(log N) + hash, regardless of duplicates</li>
 *   <li>{@code peek}: O(1); {@code contains}: hash</li>
 *   <li>{@code update}: O(log N) + hash for a unique element; O(min(k log N, N)) + hash with k
 *       equal copies (it rebuilds the heap once re-sifting every copy would cost more)</li>
 * </ul>
 *
 * <p><b>Key-stability contract:</b> {@code equals}/{@code hashCode} of an element must not
 * change while it is in the queue. Its priority (what the comparator looks at) may change,
 * as long as {@link #update(Object)} is called afterward.
 *
 * <p>Not thread-safe. Null elements are not permitted.
 */
public class IndexedPriorityQueue<T> extends AbstractQueue<T> {
    /** One heap slot. Identity equality, so equal elements still get distinct nodes. */
    private static final class Node<T> {
        final T value;
        int index;
        boolean pending;   // only during a multi-copy update: orders before every other node

        Node(T value, int index) {
            this.value = value;
            this.index = index;
        }
    }

    private final ArrayList<Node<T>> heap = new ArrayList<>();
    // element -> the node of every equal element in the queue; a key is present iff its set is non-empty
    private final Map<T, Set<Node<T>>> nodesByValue = new HashMap<>();
    private final Comparator<? super T> comparator;

    // concurrent modification detector
    private int modCount = 0;

    public IndexedPriorityQueue() {
        this(null);
    }

    public IndexedPriorityQueue(Comparator<? super T> comparator) {
        this.comparator = comparator;
    }

    @Override
    public int size() {
        return heap.size();
    }

    @Override
    public boolean contains(Object o) {
        return nodesOf(o) != null;
    }

    @Override
    public boolean offer(T element) {
        if (element == null) throw new NullPointerException("Null elements are not permitted.");
        if (comparator == null && !(element instanceof Comparable)) {
            throw new ClassCastException(element.getClass().getName() + " is not Comparable and no comparator was supplied.");
        }

        modCount++;
        Node<T> node = new Node<>(element, heap.size());
        heap.add(node);
        nodesByValue.computeIfAbsent(element, k -> new LinkedHashSet<>()).add(node);
        siftUp(node.index);
        return true;
    }

    @Override
    public T peek() {
        return heap.isEmpty() ? null : heap.get(0).value;
    }

    @Override
    public T poll() {
        if (heap.isEmpty()) return null;
        T root = heap.get(0).value;
        removeAt(0);
        return root;
    }

    @Override
    public boolean remove(Object o) {
        Set<Node<T>> nodes = nodesOf(o);
        if (nodes == null) return false;
        // Any node will do since the copies are equal. LinkedHashSet hands back its oldest
        // entry in O(1) (a plain HashSet would scan buckets, which degrades after removals).
        removeAt(nodes.iterator().next().index);
        return true;
    }

    /**
     * Restores the heap ordering after an element's priority has changed (see the
     * key-stability contract on the class). Every equal copy is re-sifted, since each may
     * have a new priority: O(log N) for a unique element, O(min(k log N, N)) for k copies.
     *
     * @return {@code false} if no equal element is in the queue
     */
    public boolean update(T element) {
        Set<Node<T>> nodes = nodesByValue.get(element);
        if (nodes == null) return false;
        modCount++;
        int k = nodes.size();
        int n = heap.size();
        if (k == 1) {
            int index = nodes.iterator().next().index;
            if (siftDown(index) == index) siftUp(index);
        } else if ((long) k * log2(n) >= n) {
            heapify();
        } else {
            reinsert(nodes);
        }
        return true;
    }

    /**
     * Re-keys several changed nodes in O(k log N). Sifting them one by one is not enough: each
     * sift would compare against copies that are still out of place, and could settle relative
     * to a priority that is about to move. So no comparison reads a changed priority until
     * every changed node is out of the heap:
     * <ol>
     *   <li>mark each node pending (smaller than anything) and float it up; the pending nodes
     *       end up as a connected region around the root, and the rest is a valid heap</li>
     *   <li>pop the k pending nodes off the top</li>
     *   <li>offer them again with their new priorities</li>
     * </ol>
     * The heap array is touched, the element index is not: the same nodes stay in the queue.
     */
    private void reinsert(Set<Node<T>> nodes) {
        // Mark and float one node at a time. An unmarked copy is passed like any other node, so
        // each floated node ends up at the root or under another pending one. Marking them all
        // first can strand a pending node under a normal one, and the pops below would then
        // remove the wrong nodes.
        for (Node<T> node : nodes) {
            node.pending = true;
            siftUp(node.index);
        }
        int k = nodes.size();
        for (int i = 0; i < k; i++) {
            int last = heap.size() - 1;
            Node<T> tail = heap.remove(last);
            if (last > 0) {
                heap.set(0, tail);
                tail.index = 0;
                siftDown(0);
            }
        }
        for (Node<T> node : nodes) {
            node.pending = false;
            node.index = heap.size();
            heap.add(node);
            siftUp(node.index);
        }
    }

    @Override
    public void clear() {
        modCount++;
        heap.clear();
        nodesByValue.clear();
    }

    @Override
    public @NotNull Iterator<T> iterator() {
        return new Itr();
    }

    private final class Itr implements Iterator<T> {
        private int cursor = 0;
        private int lastRet = -1;
        private int expectedModCount = modCount;

        /**
         * Nodes that were relocated by a preceding {@code remove()} from a not-yet-visited
         * slot to an already-visited one; they would otherwise be skipped, so we replay them
         * after the array is exhausted (same technique as {@link java.util.PriorityQueue}).
         */
        private ArrayDeque<Node<T>> forgetMeNot = null;
        private Node<T> lastRetNode = null;

        @Override
        public boolean hasNext() {
            return cursor < heap.size() || (forgetMeNot != null && !forgetMeNot.isEmpty());
        }

        @Override
        public T next() {
            if (expectedModCount != modCount) throw new ConcurrentModificationException();
            if (cursor < heap.size()) {
                lastRetNode = null;
                lastRet = cursor;
                return heap.get(cursor++).value;
            }
            if (forgetMeNot != null && !forgetMeNot.isEmpty()) {
                lastRet = -1;
                lastRetNode = forgetMeNot.poll();
                return lastRetNode.value;
            }
            throw new NoSuchElementException();
        }

        @Override
        public void remove() {
            if (expectedModCount != modCount) throw new ConcurrentModificationException();
            if (lastRet >= 0) {
                Node<T> moved = removeAt(lastRet);
                lastRet = -1;
                if (moved == null) {
                    cursor--; // the tail element filled this slot; revisit it
                } else {
                    if (forgetMeNot == null) forgetMeNot = new ArrayDeque<>();
                    forgetMeNot.add(moved); // swam past the cursor; replay later
                }
            } else if (lastRetNode != null) {
                removeAt(lastRetNode.index);
                lastRetNode = null;
            } else {
                throw new IllegalStateException();
            }
            expectedModCount = modCount;
        }
    }

    /**
     * Removes the node at {@code index} by filling the hole with the tail node.
     *
     * @return the tail node if it ended up <em>above</em> {@code index} (so an in-order
     *         iterator would skip it), otherwise {@code null}
     */
    private Node<T> removeAt(int index) {
        modCount++;
        Node<T> removed = heap.get(index);
        Set<Node<T>> nodes = nodesByValue.get(removed.value);
        nodes.remove(removed);
        if (nodes.isEmpty()) nodesByValue.remove(removed.value);

        int last = heap.size() - 1;
        Node<T> moved = heap.remove(last);
        if (index == last) return null;

        heap.set(index, moved);
        moved.index = index;
        int newIndex = siftDown(index);
        if (newIndex == index) newIndex = siftUp(index);
        return newIndex < index ? moved : null;
    }

    /**
     * Node set for an arbitrary {@code Object}, or {@code null} if absent. The Collection
     * contract makes {@code contains}/{@code remove} take {@code Object}; looking it up in a
     * {@code T}-keyed map is safe because HashMap only uses {@code equals}/{@code hashCode}
     * (a non-{@code T} simply misses), whereas casting to {@code T} would be an unchecked lie.
     */
    @SuppressWarnings("SuspiciousMethodCalls")
    private Set<Node<T>> nodesOf(Object o) {
        return o == null ? null : nodesByValue.get(o);
    }

    /** Floyd's bottom-up heap construction, O(N). */
    private void heapify() {
        for (int i = (heap.size() >>> 1) - 1; i >= 0; i--) siftDown(i);
    }

    /** floor(log2(n)) + 1 for n > 0: the height of a heap of n elements. */
    private static int log2(int n) {
        return 32 - Integer.numberOfLeadingZeros(n);
    }

    /*
     * Both sifts move a "hole" rather than swapping: displaced nodes are shifted one level (each
     * recording its new index), and the sifted node is written once at its final slot.
     */

    /** @return the node's final index */
    private int siftUp(int index) {
        Node<T> node = heap.get(index);
        while (index > 0) {
            int parentIndex = (index - 1) >>> 1;
            Node<T> parent = heap.get(parentIndex);
            if (compare(node, parent) >= 0) break;
            heap.set(index, parent);
            parent.index = index;
            index = parentIndex;
        }
        heap.set(index, node);
        node.index = index;
        return index;
    }

    /** @return the node's final index */
    private int siftDown(int index) {
        int size = heap.size();
        int half = size >>> 1;
        Node<T> node = heap.get(index);
        while (index < half) {
            int childIndex = (index << 1) + 1;
            Node<T> child = heap.get(childIndex);
            int rightIndex = childIndex + 1;
            if (rightIndex < size && compare(heap.get(rightIndex), child) < 0) {
                childIndex = rightIndex;
                child = heap.get(rightIndex);
            }
            if (compare(node, child) <= 0) break;
            heap.set(index, child);
            child.index = index;
            index = childIndex;
        }
        heap.set(index, node);
        node.index = index;
        return index;
    }

    /** Pending nodes (see {@link #reinsert}) order before all others; otherwise by value. */
    @SuppressWarnings("unchecked")
    private int compare(Node<T> a, Node<T> b) {
        if (a.pending || b.pending) return a.pending == b.pending ? 0 : a.pending ? -1 : 1;
        if (comparator != null) {
            return comparator.compare(a.value, b.value);
        }
        return ((Comparable<? super T>) a.value).compareTo(b.value);
    }
}
