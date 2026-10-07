package com.salesforce.einstein.ds.graph;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TraversalTest {

    private static <T> List<T> toList(Iterator<T> it) {
        List<T> out = new ArrayList<>();
        it.forEachRemaining(out::add);
        return out;
    }

    /**
     * Rooted at {@code "a"}, with an edge from {@code "e"} back to the root closing a cycle:
     * <pre>{@code
     *   a -> b -> d
     *   |    ^    |
     *   v    |    v
     *   c ---+    e -> a
     * }</pre>
     */
    private static Graph<String> cyclic() {
        Graph<String> g = Graph.directed();
        g.addEdge("a", "b");
        g.addEdge("a", "c");
        g.addEdge("c", "b");
        g.addEdge("b", "d");
        g.addEdge("d", "e");
        g.addEdge("e", "a");
        return g;
    }

    @Test
    void bfsVisitsByDistanceAndSurvivesCycles() {
        assertEquals(List.of("a", "b", "c", "d", "e"), toList(cyclic().bfs("a")));
    }

    @Test
    void dfsIsRecursivePreorderAndSurvivesCycles() {
        // recursive DFS: the root, then b, d, e (its edge back to the root is skipped), backtrack, then c
        // (its edge to b is skipped too)
        assertEquals(List.of("a", "b", "d", "e", "c"), toList(cyclic().dfs("a")));
    }

    @Test
    void dfsMatchesRecursiveReferenceOnDenseCyclicGraph() {
        Graph<Integer> g = Graph.directed();
        java.util.Random random = new java.util.Random(42);
        for (int i = 0; i < 300; i++) {
            g.addEdge(random.nextInt(60), random.nextInt(60));
        }
        for (Integer start : g.nodes()) {
            List<Integer> expected = new ArrayList<>();
            recursiveDfs(g, start, new HashSet<>(), expected);
            assertEquals(expected, toList(g.dfs(start)));
        }
    }

    private static void recursiveDfs(Graph<Integer> g, Integer node, Set<Integer> seen, List<Integer> out) {
        if (!seen.add(node)) {
            return;
        }
        out.add(node);
        for (Integer next : g.successors(node)) {
            recursiveDfs(g, next, seen, out);
        }
    }

    @Test
    void onlyReachableNodesFromStartAndDirectionRespected() {
        Graph<String> g = Graph.directed();
        g.addEdge("a", "b");
        g.addEdge("c", "a");   // c points at the start node but cannot be reached from it
        assertEquals(List.of("a", "b"), toList(g.bfs("a")));
        assertEquals(List.of("a", "b"), toList(g.dfs("a")));
    }

    @Test
    void wholeGraphTraversalCoversEveryComponentOnce() {
        Graph<String> g = Graph.directed();
        g.addEdge("a", "b");
        g.addEdge("b", "a");
        g.addEdge("c", "d");
        g.addNode("lonely");
        g.addEdge("e", "c");   // e is inserted after c, so c's tree has already taken d

        assertEquals(List.of("a", "b", "c", "d", "lonely", "e"), toList(g.bfs()));
        assertEquals(List.of("a", "b", "c", "d", "lonely", "e"), toList(g.dfs()));
    }

    @Test
    void undirectedTraversalGoesBothWays() {
        Graph<Integer> g = Graph.undirected();
        g.addEdge(1, 2);
        g.addEdge(3, 2);
        g.addEdge(3, 1);   // triangle
        g.addEdge(3, 4);
        assertEquals(List.of(4, 3, 2, 1), toList(g.bfs(4)));
        assertEquals(List.of(4, 3, 2, 1), toList(g.dfs(4)));
    }

    @Test
    void selfLoopAndSingleNode() {
        Graph<String> g = Graph.directed();
        g.addEdge("x", "x");
        assertEquals(List.of("x"), toList(g.bfs("x")));
        assertEquals(List.of("x"), toList(g.dfs("x")));
    }

    @Test
    void deepChainDoesNotOverflowStack() {
        Graph<Integer> g = Graph.directed();
        int n = 200_000;
        for (int i = 0; i < n - 1; i++) {
            g.addEdge(i, i + 1);
        }
        g.addEdge(n - 1, 0);
        int count = 0;
        for (Iterator<Integer> it = g.dfs(0); it.hasNext(); it.next()) {
            count++;
        }
        assertEquals(n, count);
    }

    @Test
    void iteratorContract() {
        Iterator<String> it = cyclic().bfs("e");
        for (int i = 0; i < 3; i++) {
            assertTrue(it.hasNext());   // repeated calls must not advance the iterator...
        }
        assertEquals(List.of("e", "a", "b", "c", "d"), toList(it));   // ...so nothing was skipped
        assertFalse(it.hasNext());
        assertThrows(NoSuchElementException.class, it::next);
        assertThrows(UnsupportedOperationException.class, it::remove);
    }

    @Test
    void failFastOnModification() {
        Graph<String> g = cyclic();
        Iterator<String> bfs = g.bfs("a");
        Iterator<String> dfs = g.dfs();
        bfs.next();
        g.addEdge("e", "c");
        assertThrows(ConcurrentModificationException.class, bfs::hasNext);
        assertThrows(ConcurrentModificationException.class, dfs::next);
    }

    @Test
    void standaloneIteratorsWorkOnAnyNeighbourFunction() {
        Map<String, List<String>> adjacency = Map.of(
                "a", List.of("b", "c"), "b", List.of("a"), "c", List.of("c"));
        assertEquals(List.of("a", "b", "c"), toList(new BfsIterator<>(List.of("a"), adjacency::get)));
        assertEquals(List.of("a", "b", "c"), toList(new DfsIterator<>(List.of("a"), adjacency::get)));
    }
}
