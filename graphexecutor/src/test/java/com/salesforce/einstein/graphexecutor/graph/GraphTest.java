package com.salesforce.einstein.graphexecutor.graph;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GraphTest {

    private static <T> List<T> toList(Iterator<T> it) {
        List<T> out = new ArrayList<>();
        it.forEachRemaining(out::add);
        return out;
    }

    // ---- properties ----

    @Test
    void factoriesSetBothProperties() {
        assertTrue(Graph.directed().isDirected());
        assertFalse(Graph.directed().isWeighted());
        assertFalse(Graph.undirected().isDirected());
        assertFalse(Graph.undirected().isWeighted());
        assertTrue(Graph.weightedDirected().isDirected());
        assertTrue(Graph.weightedDirected().isWeighted());
        assertFalse(Graph.weightedUndirected().isDirected());
        assertTrue(Graph.weightedUndirected().isWeighted());
    }

    @Test
    void weightedGraphRequiresWeights() {
        Graph<String> g = Graph.weightedDirected();
        assertThrows(UnsupportedOperationException.class, () -> g.addEdge("a", "b"));
        assertEquals(0, g.nodeCount());   // rejected before touching anything
    }

    @Test
    void unweightedAddEdgeReportsWhetherItChangedTheGraph() {
        Graph<String> directed = Graph.directed();
        assertTrue(directed.addEdge("a", "b"));
        assertFalse(directed.addEdge("a", "b"));
        assertTrue(directed.addEdge("b", "a"));    // the reverse is a different edge

        Graph<String> undirected = Graph.undirected();
        assertTrue(undirected.addEdge("a", "b"));
        assertFalse(undirected.addEdge("b", "a"));   // same edge
        assertEquals(1, undirected.edgeCount());
    }

    @Test
    void unweightedGraphRejectsWeights() {
        Graph<String> g = Graph.directed();
        g.addEdge("a", "b");
        assertThrows(UnsupportedOperationException.class, () -> g.addEdge("b", "c", 2));
        assertThrows(UnsupportedOperationException.class, () -> g.updateEdge("a", "b", 2));
        assertEquals(Optional.empty(), g.edgeWeight("a", "b"));
        assertTrue(g.containsEdge("a", "b"));
        assertEquals(1, g.edgeCount());
    }

    @Test
    void toStringShowsPropertiesAndWeightsOnlyWhenWeighted() {
        Graph<String> plain = Graph.directed();
        plain.addEdge("a", "b");
        assertEquals("Graph[directed]{a=[b], b=[]}", plain.toString());

        Graph<String> weighted = Graph.weightedUndirected();
        weighted.addEdge("a", "b", 2.5);
        assertEquals("Graph[undirected, weighted]{a={b=2.5}, b={a=2.5}}", weighted.toString());
    }

    // ---- directed ----

    @Test
    void directedEdgeGoesOneWay() {
        Graph<String> g = Graph.weightedDirected();
        assertTrue(g.addEdge("a", "b", 2.5));

        assertTrue(g.containsEdge("a", "b"));
        assertFalse(g.containsEdge("b", "a"));
        assertEquals(Set.of("b"), g.successors("a"));
        assertEquals(Set.of("a"), g.predecessors("b"));
        assertEquals(Optional.of(2.5), g.edgeWeight("a", "b"));
        assertEquals(Optional.empty(), g.edgeWeight("b", "a"));
        assertEquals(2, g.nodeCount());   // endpoints added implicitly
        assertEquals(1, g.edgeCount());
    }

    @Test
    void inAndOutDegree() {
        Graph<String> g = Graph.directed();
        g.addEdge("a", "b");
        g.addEdge("a", "c");
        g.addEdge("c", "b");
        g.addEdge("c", "c");   // a self-loop counts once each way

        assertEquals(2, g.outDegree("a"));
        assertEquals(0, g.inDegree("a"));
        assertEquals(0, g.outDegree("b"));
        assertEquals(2, g.inDegree("b"));
        assertEquals(2, g.outDegree("c"));
        assertEquals(2, g.inDegree("c"));
        assertThrows(IllegalArgumentException.class, () -> g.outDegree("missing"));
    }

    @Test
    void addIsIdempotentAndDoesNotOverwriteWeight() {
        Graph<String> g = Graph.weightedDirected();
        assertTrue(g.addNode("a"));
        assertFalse(g.addNode("a"));
        assertTrue(g.addEdge("a", "b", 1));
        assertFalse(g.addEdge("a", "b", 9));
        assertEquals(Optional.of(1.0), g.edgeWeight("a", "b"));
        assertEquals(1, g.edgeCount());
    }

    @Test
    void updateEdgeChangesWeightOnlyIfPresent() {
        Graph<String> g = Graph.weightedDirected();
        g.addEdge("a", "b", 1);
        assertTrue(g.updateEdge("a", "b", 7));
        assertEquals(Optional.of(7.0), g.edgeWeight("a", "b"));
        assertFalse(g.updateEdge("b", "a", 7));
        assertFalse(g.containsEdge("b", "a"));
    }

    @Test
    void removeEdge() {
        Graph<String> g = Graph.directed();
        g.addEdge("a", "b");
        assertFalse(g.removeEdge("b", "a"));
        assertTrue(g.removeEdge("a", "b"));
        assertFalse(g.removeEdge("a", "b"));
        assertEquals(0, g.edgeCount());
        assertEquals(Set.of(), g.predecessors("b"));
        assertEquals(2, g.nodeCount());   // nodes stay
    }

    @Test
    void removeNodeDropsIncomingOutgoingAndSelfLoop() {
        Graph<String> g = Graph.directed();
        g.addEdge("a", "x");
        g.addEdge("x", "b");
        g.addEdge("x", "x");
        g.addEdge("a", "b");

        assertTrue(g.removeNode("x"));
        assertFalse(g.removeNode("x"));
        assertEquals(Set.of("a", "b"), g.nodes());
        assertEquals(1, g.edgeCount());
        assertEquals(Set.of("b"), g.successors("a"));
        assertEquals(Set.of("a"), g.predecessors("b"));
    }

    @Test
    void updateNodeMovesEveryEdgeWithItsWeight() {
        Graph<String> g = Graph.weightedDirected();
        g.addEdge("a", "x", 1);
        g.addEdge("x", "b", 2);
        g.addEdge("x", "x", 3);

        assertTrue(g.updateNode("x", "y"));

        assertFalse(g.containsNode("x"));
        assertEquals(Optional.of(1.0), g.edgeWeight("a", "y"));
        assertEquals(Optional.of(2.0), g.edgeWeight("y", "b"));
        assertEquals(Optional.of(3.0), g.edgeWeight("y", "y"));
        assertEquals(3, g.edgeCount());
        assertEquals(Set.of("a", "y"), g.predecessors("y"));
    }

    @Test
    void updateNodeOnUnweightedGraphKeepsEdgesUnweighted() {
        Graph<String> g = Graph.directed();
        g.addEdge("a", "x");
        assertTrue(g.updateNode("x", "y"));
        assertTrue(g.containsEdge("a", "y"));
        assertEquals(Optional.empty(), g.edgeWeight("a", "y"));
    }

    @Test
    void updateNodeEdgeCases() {
        Graph<String> g = Graph.directed();
        g.addEdge("a", "b");
        assertFalse(g.updateNode("missing", "z"));
        assertTrue(g.updateNode("a", "a"));
        assertThrows(IllegalArgumentException.class, () -> g.updateNode("a", "b"));
        assertEquals(1, g.edgeCount());
    }

    @Test
    void rejectsNullsAndUnknownNodes() {
        Graph<String> g = Graph.directed();
        assertThrows(NullPointerException.class, () -> g.addNode(null));
        assertThrows(NullPointerException.class, () -> g.addEdge("a", null));
        assertThrows(IllegalArgumentException.class, () -> g.successors("nope"));
        assertThrows(IllegalArgumentException.class, () -> g.bfs("nope"));
        assertFalse(g.containsNode(null));
        assertFalse(g.removeNode(null));
    }

    @Test
    void viewsAreUnmodifiable() {
        Graph<String> g = Graph.directed();
        g.addEdge("a", "b");
        assertThrows(UnsupportedOperationException.class, () -> g.nodes().remove("a"));
        assertThrows(UnsupportedOperationException.class, () -> g.successors("a").clear());
    }

    @Test
    void subgraphKeepsPropertiesWeightsAndOnlyInternalEdges() {
        Graph<String> g = Graph.weightedDirected();
        g.addEdge("a", "b", 4);
        g.addEdge("b", "c", 1);
        g.addEdge("c", "a", 1);

        Graph<String> sub = g.subgraph(List.of("a", "b", "zzz"));
        assertTrue(sub.isDirected());
        assertTrue(sub.isWeighted());
        assertEquals(Set.of("a", "b"), sub.nodes());
        assertEquals(1, sub.edgeCount());
        assertEquals(Optional.of(4.0), sub.edgeWeight("a", "b"));

        sub.removeNode("a");   // independent copy
        assertTrue(g.containsNode("a"));

        Graph<String> plain = Graph.undirected();
        plain.addEdge("a", "b");
        Graph<String> plainSub = plain.subgraph(List.of("a", "b"));
        assertFalse(plainSub.isDirected());
        assertFalse(plainSub.isWeighted());
        assertTrue(plainSub.containsEdge("b", "a"));
    }

    // ---- undirected ----

    @Test
    void undirectedEdgeIsAddressableFromEitherEnd() {
        Graph<String> g = Graph.weightedUndirected();
        assertTrue(g.addEdge("a", "b", 2));
        assertFalse(g.addEdge("b", "a", 5));   // same edge
        assertEquals(1, g.edgeCount());
        assertTrue(g.containsEdge("b", "a"));
        assertEquals(Set.of("b"), g.successors("a"));
        assertEquals(Set.of("a"), g.predecessors("b"));   // same as successors
        assertEquals(1, g.inDegree("b"));
        assertEquals(1, g.outDegree("b"));

        assertTrue(g.updateEdge("b", "a", 9));
        assertEquals(Optional.of(9.0), g.edgeWeight("a", "b"));

        assertTrue(g.removeEdge("b", "a"));
        assertFalse(g.containsEdge("a", "b"));
        assertEquals(0, g.edgeCount());
    }

    @Test
    void undirectedSelfLoopCountsOnce() {
        Graph<String> g = Graph.undirected();
        g.addEdge("a", "a");
        assertEquals(1, g.edgeCount());
        assertEquals(1, g.outDegree("a"));
        assertTrue(g.removeEdge("a", "a"));
        assertEquals(0, g.edgeCount());
    }

    @Test
    void undirectedRemoveAndUpdateNode() {
        Graph<String> g = Graph.weightedUndirected();
        g.addEdge("a", "x", 1);
        g.addEdge("x", "b", 2);
        g.addEdge("x", "x", 3);

        assertTrue(g.updateNode("x", "y"));
        assertEquals(3, g.edgeCount());
        assertEquals(Optional.of(1.0), g.edgeWeight("y", "a"));
        assertEquals(Optional.of(2.0), g.edgeWeight("b", "y"));
        assertEquals(Optional.of(3.0), g.edgeWeight("y", "y"));
        assertEquals(Set.of("y"), g.successors("a"));

        assertTrue(g.removeNode("y"));
        assertEquals(0, g.edgeCount());
        assertEquals(Set.of(), g.successors("a"));
        assertEquals(Set.of(), g.successors("b"));
    }

    @Test
    void traversalIgnoresWeights() {
        Graph<String> g = Graph.weightedUndirected();
        g.addEdge("a", "b", 5);
        g.addEdge("b", "c", 0.1);
        assertEquals(List.of("c", "b", "a"), toList(g.bfs("c")));
    }
}
