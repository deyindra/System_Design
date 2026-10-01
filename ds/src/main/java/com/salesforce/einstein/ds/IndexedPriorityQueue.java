package com.salesforce.einstein.ds;

import java.util.AbstractQueue;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * A binary min-heap that also indexes each element's heap position, giving
 * O(log N) {@link #remove(Object)}, O(1) {@link #contains(Object)} and an
 * {@link #update(Object)} operation for re-keying an element in place.
 *
 * <p>Duplicates (per {@code equals}) are allowed; equal elements share one position set
 * (a {@link LinkedHashSet}, so slot bookkeeping and picking a slot are O(1)). Costs are
 * expected-time given a reasonable {@code hashCode}:
 * <ul>
 *   <li>{@code offer}, {@code poll}, {@code remove(Object)}: O(log N), regardless of duplicates</li>
 *   <li>{@code peek}, {@code contains}: O(1)</li>
 *   <li>{@code update}: O(log N) for a unique element; O(k log N + k²) with k equal copies</li>
 * </ul>
 *
 * <p><b>Key-stability contract:</b> {@code equals}/{@code hashCode} of an element must not
 * change while it is in the queue. Its priority (what the comparator looks at) may change,
 * as long as {@link #update(Object)} is called after wards.
 *
 * <p>Not thread-safe. Null elements are not permitted.
 */
public class IndexedPriorityQueue<T> extends AbstractQueue<T> {
    private final ArrayList<T> heap = new ArrayList<>();
    // element -> every heap slot holding an equal element; a key is present iff its set is non-empty
    private final Map<T, Set<Integer>> positionMap = new HashMap<>();
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
        return positionsOf(o) != null;
    }

    @Override
    public boolean offer(T element) {
        if (element == null) throw new NullPointerException("Null elements are not permitted.");
        if (comparator == null && !(element instanceof Comparable)) {
            throw new ClassCastException(element.getClass().getName() + " is not Comparable and no comparator was supplied.");
        }

        modCount++;
        heap.add(element);
        int index = heap.size() - 1;
        addIndexMapping(element, index);
        siftUp(index);
        return true;
    }

    @Override
    public T peek() {
        return heap.isEmpty() ? null : heap.get(0);
    }

    @Override
    public T poll() {
        if (heap.isEmpty()) return null;
        T root = heap.get(0);
        removeAt(0);
        return root;
    }

    @Override
    public boolean remove(Object o) {
        Set<Integer> indices = positionsOf(o);
        if (indices == null) return false;
        // Any slot will do since the copies are equal. LinkedHashSet hands back its oldest
        // entry in O(1) (a plain HashSet would scan buckets, which degrades after removals).
        removeAt(indices.iterator().next());
        return true;
    }

    /**
     * O(log N) for a uniquely-keyed element. Restores the heap ordering after an
     * element's priority has changed (see the key-stability contract on the class).
     *
     * @return {@code false} if no equal element is in the queue
     */
    public boolean update(T element) {
        Set<Integer> indices = positionMap.get(element);
        if (indices == null) return false;
        modCount++;
        // Equal elements share one position set and sifting reshuffles it, so snapshot the
        // instances first and re-locate each one by identity before sifting it.
        List<T> instances = new ArrayList<>(indices.size());
        for (int i : indices) instances.add(heap.get(i));
        for (T instance : instances) {
            int index = indexOfInstance(instance);
            if (siftDown(index) == index) siftUp(index);
        }
        return true;
    }

    @Override
    public void clear() {
        modCount++;
        heap.clear();
        positionMap.clear();
    }

    @Override
    public Iterator<T> iterator() {
        return new Itr();
    }

    private final class Itr implements Iterator<T> {
        private int cursor = 0;
        private int lastRet = -1;
        private int expectedModCount = modCount;

        /**
         * Elements that were relocated by a preceding {@code remove()} from a not-yet-visited
         * slot to an already-visited one; they would otherwise be skipped, so we replay them
         * after the array is exhausted (same technique as {@link java.util.PriorityQueue}).
         */
        private ArrayDeque<T> forgetMeNot = null;
        private T lastRetElt = null;

        @Override
        public boolean hasNext() {
            return cursor < heap.size() || (forgetMeNot != null && !forgetMeNot.isEmpty());
        }

        @Override
        public T next() {
            if (expectedModCount != modCount) throw new ConcurrentModificationException();
            if (cursor < heap.size()) {
                lastRetElt = null;
                lastRet = cursor;
                return heap.get(cursor++);
            }
            if (forgetMeNot != null && !forgetMeNot.isEmpty()) {
                lastRet = -1;
                lastRetElt = forgetMeNot.poll();
                return lastRetElt;
            }
            throw new NoSuchElementException();
        }

        @Override
        public void remove() {
            if (expectedModCount != modCount) throw new ConcurrentModificationException();
            if (lastRet >= 0) {
                T moved = removeAt(lastRet);
                lastRet = -1;
                if (moved == null) {
                    cursor--; // the tail element filled this slot; revisit it
                } else {
                    if (forgetMeNot == null) forgetMeNot = new ArrayDeque<>();
                    forgetMeNot.add(moved); // swam past the cursor; replay later
                }
            } else if (lastRetElt != null) {
                removeAt(indexOfInstance(lastRetElt));
                lastRetElt = null;
            } else {
                throw new IllegalStateException();
            }
            expectedModCount = modCount;
        }
    }

    /**
     * Removes the element at {@code index} by filling the hole with the tail element.
     *
     * @return the tail element if it ended up <em>above</em> {@code index} (so an in-order
     *         iterator would skip it), otherwise {@code null}
     */
    private T removeAt(int index) {
        modCount++;
        int last = heap.size() - 1;
        removeIndexMapping(heap.get(index), index);
        T moved = heap.remove(last);
        if (index == last) return null;

        removeIndexMapping(moved, last);
        heap.set(index, moved);
        addIndexMapping(moved, index);

        int newIndex = siftDown(index);
        if (newIndex == index) newIndex = siftUp(index);
        return newIndex < index ? moved : null;
    }

    /**
     * Position set for an arbitrary {@code Object}, or {@code null} if absent. The Collection
     * contract makes {@code contains}/{@code remove} take {@code Object}; looking it up in a
     * {@code T}-keyed map is safe because HashMap only uses {@code equals}/{@code hashCode}
     * (a non-{@code T} simply misses), whereas casting to {@code T} would be an unchecked lie.
     */
    @SuppressWarnings("SuspiciousMethodCalls")
    private Set<Integer> positionsOf(Object o) {
        return o == null ? null : positionMap.get(o);
    }

    /** Index of this exact instance (by identity), or -1. */
    private int indexOfInstance(T target) {
        Set<Integer> indices = positionMap.get(target);
        if (indices != null) {
            for (int i : indices) {
                if (heap.get(i) == target) return i;
            }
        }
        return -1;
    }

    /*
     * Both sifts move a "hole" rather than swapping: the sifted element is detached from its
     * position set up front, displaced elements are shifted one level, and the element is
     * re-attached once at its final slot. This also stays correct when the element shares a
     * position set with an equal neighbor.
     *
     * The set may be momentarily empty while the element is detached; it is deliberately left
     * in positionMap (rather than via removeIndexMapping) to avoid a remove/re-create per sift.
     */

    /** @return the element's final index */
    private int siftUp(int index) {
        T element = heap.get(index);
        Set<Integer> slots = positionMap.get(element);
        slots.remove(index);
        while (index > 0) {
            int parentIndex = (index - 1) >>> 1;
            if (compare(element, heap.get(parentIndex)) >= 0) break;
            move(parentIndex, index);
            index = parentIndex;
        }
        heap.set(index, element);
        slots.add(index);
        return index;
    }

    /** @return the element's final index */
    private int siftDown(int index) {
        int size = heap.size();
        int half = size >>> 1;
        T element = heap.get(index);
        Set<Integer> slots = positionMap.get(element);
        slots.remove(index);
        while (index < half) {
            int childIndex = (index << 1) + 1;
            int rightIndex = childIndex + 1;
            if (rightIndex < size && compare(heap.get(rightIndex), heap.get(childIndex)) < 0) {
                childIndex = rightIndex;
            }
            if (compare(element, heap.get(childIndex)) <= 0) break;
            move(childIndex, index);
            index = childIndex;
        }
        heap.set(index, element);
        slots.add(index);
        return index;
    }

    private void move(int from, int to) {
        T element = heap.get(from);
        heap.set(to, element);
        Set<Integer> slots = positionMap.get(element);
        slots.remove(from);
        slots.add(to);
    }

    private void addIndexMapping(T element, int index) {
        positionMap.computeIfAbsent(element, k -> new LinkedHashSet<>()).add(index);
    }

    private void removeIndexMapping(T element, int index) {
        Set<Integer> indices = positionMap.get(element);
        if (indices != null) {
            indices.remove(index);
            if (indices.isEmpty()) {
                positionMap.remove(element);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private int compare(T a, T b) {
        if (comparator != null) {
            return comparator.compare(a, b);
        }
        return ((Comparable<? super T>) a).compareTo(b);
    }
}
