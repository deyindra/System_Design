package com.salesforce.einstein.ds;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IndexedPriorityQueueTest {

    /** Identity equality, mutable priority: satisfies the key-stability contract. */
    private static final class Task {
        final String name;
        int priority;

        Task(String name, int priority) {
            this.name = name;
            this.priority = priority;
        }

        @Override
        public String toString() {
            return name + ":" + priority;
        }
    }

    private static final Comparator<Task> BY_PRIORITY = Comparator.comparingInt(t -> t.priority);

    private static <T> List<T> drain(IndexedPriorityQueue<T> q) {
        List<T> out = new ArrayList<>();
        T t;
        while ((t = q.poll()) != null) out.add(t);
        return out;
    }

    private static <T extends Comparable<? super T>> void assertSorted(List<T> list) {
        List<T> sorted = new ArrayList<>(list);
        Collections.sort(sorted);
        assertEquals(sorted, list);
    }

    @SuppressWarnings("ConstantValue")
    @Test
    void emptyQueue() {
        // Empty it via add/poll rather than asserting on a fresh instance: that also covers the
        // drained state, and a fresh one only gives IntelliJ "always true/false" assertions.
        IndexedPriorityQueue<Integer> q = new IndexedPriorityQueue<>();
        q.add(1);
        assertEquals(1, q.poll());

        assertTrue(q.isEmpty());
        assertEquals(0, q.size());
        assertNull(q.peek());
        assertNull(q.poll());
        assertFalse(q.contains(1));
        assertFalse(q.remove(1));
        assertThrows(NoSuchElementException.class, q::element);
        assertThrows(NoSuchElementException.class, q::remove);
    }

    @Test
    void pollsInNaturalOrder() {
        IndexedPriorityQueue<Integer> q = new IndexedPriorityQueue<>();
        for (int v : new int[]{5, 3, 8, 1, 9, 2, 7}) q.offer(v);
        assertEquals(7, q.size());
        assertEquals(1, q.peek());
        assertEquals(List.of(1, 2, 3, 5, 7, 8, 9), drain(q));
        assertTrue(q.isEmpty());
    }

    @Test
    void honoursComparator() {
        IndexedPriorityQueue<Integer> q = new IndexedPriorityQueue<>(Comparator.reverseOrder());
        q.addAll(List.of(4, 1, 6, 3));
        assertEquals(List.of(6, 4, 3, 1), drain(q));
    }

    @Test
    void rejectsNull() {
        IndexedPriorityQueue<Integer> q = new IndexedPriorityQueue<>();
        assertThrows(NullPointerException.class, () -> q.offer(null));
        assertFalse(q.contains(null));
        assertFalse(q.remove(null));
    }

    @Test
    void rejectsNonComparableWithoutComparator() {
        IndexedPriorityQueue<Object> q = new IndexedPriorityQueue<>();
        q.add(1);
        assertThrows(ClassCastException.class, () -> q.offer(new Object()));
        assertEquals(List.of(1), drain(q)); // rejected before touching the heap
    }

    @Test
    void containsAndRemoveArbitraryElement() {
        IndexedPriorityQueue<Integer> q = new IndexedPriorityQueue<>();
        q.addAll(List.of(10, 20, 30, 40, 50));
        assertTrue(q.contains(30));
        assertTrue(q.remove(30));
        assertFalse(q.contains(30));
        assertFalse(q.remove(30));
        Object notAnInteger = "not an integer"; // contains(Object) must tolerate foreign types
        //noinspection SuspiciousMethodCalls
        assertFalse(q.contains(notAnInteger));
        assertEquals(List.of(10, 20, 40, 50), drain(q));
    }

    @Test
    void duplicatesAreTrackedIndividually() {
        IndexedPriorityQueue<String> q = new IndexedPriorityQueue<>();
        q.addAll(List.of("b", "a", "b", "c", "b"));
        assertEquals(5, q.size());

        assertTrue(q.remove("b"));
        assertTrue(q.contains("b"));
        assertTrue(q.remove("b"));
        assertTrue(q.contains("b"));
        assertTrue(q.remove("b"));
        assertFalse(q.contains("b"));

        assertEquals(List.of("a", "c"), drain(q));
    }

    @Test
    void updateAfterPriorityDecrease() {
        IndexedPriorityQueue<Task> q = new IndexedPriorityQueue<>(BY_PRIORITY);
        Task a = new Task("a", 1), b = new Task("b", 5), c = new Task("c", 10);
        q.addAll(List.of(a, b, c));

        c.priority = 0;
        assertTrue(q.update(c));
        assertSame(c, q.peek());
        assertEquals(List.of(c, a, b), drain(q));
    }

    @Test
    void updateAfterPriorityIncrease() {
        IndexedPriorityQueue<Task> q = new IndexedPriorityQueue<>(BY_PRIORITY);
        Task a = new Task("a", 1), b = new Task("b", 5), c = new Task("c", 10);
        q.addAll(List.of(a, b, c));

        a.priority = 100;
        assertTrue(q.update(a));
        assertEquals(List.of(b, c, a), drain(q));
    }

    @Test
    void updateOfAbsentElementReturnsFalse() {
        IndexedPriorityQueue<Task> q = new IndexedPriorityQueue<>(BY_PRIORITY);
        q.add(new Task("a", 1));
        assertFalse(q.update(new Task("ghost", 0)));
    }

    @Test
    void updateWithEqualButDistinctInstances() {
        // Equal by id, ordered by a mutable priority: the instances share one position set.
        final class Keyed {
            final int id;
            int priority;

            Keyed(int id, int priority) {
                this.id = id;
                this.priority = priority;
            }

            @Override
            public boolean equals(Object o) {
                return o instanceof Keyed && ((Keyed) o).id == id;
            }

            @Override
            public int hashCode() {
                return id;
            }
        }
        IndexedPriorityQueue<Keyed> q = new IndexedPriorityQueue<>(Comparator.comparingInt(k -> k.priority));
        Keyed low = new Keyed(1, 1);
        Keyed high = new Keyed(1, 50);
        q.add(low);
        for (int p = 2; p < 20; p++) q.add(new Keyed(100 + p, p));
        q.add(high);

        high.priority = 0;
        assertTrue(q.update(new Keyed(1, -1))); // any equal key locates the group
        assertSame(high, q.poll());
        assertSame(low, q.poll());
    }

    @Test
    void clearEmptiesQueueAndIndex() {
        IndexedPriorityQueue<Integer> q = new IndexedPriorityQueue<>();
        q.addAll(List.of(3, 1, 2));
        q.clear();
        q.add(7);
        assertFalse(q.contains(1)); // index was cleared, not just the heap
        assertEquals(List.of(7), drain(q));
    }

    @Test
    void iteratorVisitsEveryElement() {
        IndexedPriorityQueue<Integer> q = new IndexedPriorityQueue<>();
        List<Integer> values = List.of(9, 4, 7, 1, 8, 2, 2, 6);
        q.addAll(values);

        List<Integer> seen = new ArrayList<>(q); // copies via AbstractCollection.toArray -> iterator()
        Collections.sort(seen);
        List<Integer> expected = new ArrayList<>(values);
        Collections.sort(expected);
        assertEquals(expected, seen);
    }

    @Test
    void iteratorExhaustionAndIllegalRemove() {
        IndexedPriorityQueue<Integer> q = new IndexedPriorityQueue<>();
        q.add(1);
        Iterator<Integer> it = q.iterator();
        assertThrows(IllegalStateException.class, it::remove);
        it.next();
        it.remove();
        assertThrows(IllegalStateException.class, it::remove);
        assertFalse(it.hasNext());
        assertThrows(NoSuchElementException.class, it::next);
        assertTrue(q.isEmpty());
    }

    @Test
    void iteratorFailsFastOnExternalModification() {
        IndexedPriorityQueue<Integer> q = new IndexedPriorityQueue<>();
        q.addAll(List.of(1, 2, 3));
        Iterator<Integer> it = q.iterator();
        it.next();
        q.add(4);
        assertThrows(ConcurrentModificationException.class, it::next);
        assertThrows(ConcurrentModificationException.class, it::remove);
    }

    @Test
    void iteratorRemoveVisitsEveryElementExactlyOnce() {
        Random rnd = new Random(42);
        for (int round = 0; round < 200; round++) {
            IndexedPriorityQueue<Integer> q = new IndexedPriorityQueue<>();
            List<Integer> all = new ArrayList<>();
            int n = 1 + rnd.nextInt(60);
            for (int i = 0; i < n; i++) {
                int v = rnd.nextInt(30);
                q.add(v);
                all.add(v);
            }

            List<Integer> seen = new ArrayList<>();
            List<Integer> kept = new ArrayList<>();
            for (Iterator<Integer> it = q.iterator(); it.hasNext(); ) {
                Integer v = it.next();
                seen.add(v);
                if (rnd.nextBoolean()) it.remove();
                else kept.add(v);
            }

            Collections.sort(all);
            Collections.sort(seen);
            Collections.sort(kept);
            assertEquals(all, seen, "round " + round);
            assertEquals(kept, drain(q), "round " + round);
        }
    }

    @Test
    void randomizedOperationsMatchJavaUtilPriorityQueue() {
        Random rnd = new Random(7);
        IndexedPriorityQueue<Integer> q = new IndexedPriorityQueue<>();
        PriorityQueue<Integer> ref = new PriorityQueue<>();

        for (int step = 0; step < 20_000; step++) {
            int op = rnd.nextInt(10);
            int v = rnd.nextInt(100);
            if (op < 5) {
                q.offer(v);
                ref.offer(v);
            } else if (op < 7) {
                assertEquals(ref.poll(), q.poll());
            } else if (op < 9) {
                assertEquals(ref.remove(v), q.remove(v));
            } else {
                assertEquals(ref.contains(v), q.contains(v));
            }
            assertEquals(ref.size(), q.size());
            assertEquals(ref.peek(), q.peek());
        }

        List<Integer> drained = drain(q);
        assertSorted(drained);
        List<Integer> expected = new ArrayList<>();
        while (!ref.isEmpty()) expected.add(ref.poll());
        assertEquals(expected, drained);
    }

    @Test
    void randomizedUpdatesKeepHeapOrder() {
        Random rnd = new Random(99);
        IndexedPriorityQueue<Task> q = new IndexedPriorityQueue<>(BY_PRIORITY);
        List<Task> tasks = new ArrayList<>();
        for (int i = 0; i < 500; i++) {
            Task t = new Task("t" + i, rnd.nextInt(1000));
            tasks.add(t);
            q.add(t);
        }
        for (int i = 0; i < 5_000; i++) {
            Task t = tasks.get(rnd.nextInt(tasks.size()));
            t.priority = rnd.nextInt(1000);
            assertTrue(q.update(t));
        }

        List<Task> drained = drain(q);
        assertEquals(tasks.size(), drained.size());
        for (int i = 1; i < drained.size(); i++) {
            assertTrue(drained.get(i - 1).priority <= drained.get(i).priority,
                    "out of order at " + i + ": " + drained.get(i - 1) + " > " + drained.get(i));
        }
    }
}
