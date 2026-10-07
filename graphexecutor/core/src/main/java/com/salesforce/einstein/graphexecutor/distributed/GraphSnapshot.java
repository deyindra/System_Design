package com.salesforce.einstein.graphexecutor.distributed;

import com.salesforce.einstein.ds.graph.Graph;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

/**
 * What both distributed executors build first, on the calling thread: an independent copy of the graph,
 * its nodes in insertion order, each node's neighbors in both directions (for grouping and placement),
 * and the insertion order as a comparator.
 */
record GraphSnapshot<T>(Graph<T> graph, List<T> nodes, Map<T, List<T>> neighbors, Comparator<T> graphOrder) {

    /** Weakly connected components, in first-node order, and the label-propagation rounds it took. */
    record Groups<T>(List<List<T>> groups, int labelRounds) {
    }

    static <T> GraphSnapshot<T> of(Graph<T> graph) {
        if (!graph.isDirected()) {
            throw new IllegalArgumentException("needs a directed (unidirectional) graph");
        }
        Graph<T> snapshot = graph.subgraph(graph.nodes());
        List<T> nodes = List.copyOf(snapshot.nodes());
        Map<T, Integer> index = new HashMap<>();
        Map<T, List<T>> neighbors = new HashMap<>();
        for (T node : nodes) {
            index.put(node, index.size());
            List<T> either = new ArrayList<>(snapshot.successors(node));
            either.addAll(snapshot.predecessors(node));
            neighbors.put(node, either);
        }
        return new GraphSnapshot<>(snapshot, nodes, neighbors, Comparator.comparingInt(index::get));
    }

    /** Groups found by partitions that own nodes by hash: placement is not known yet. */
    Groups<T> groups(Executor pool, int partitions) {
        Map<T, Integer> hashOwner = Partitioner.<T>hash().assign(nodes, neighbors::get, partitions);
        LabelPropagation.Result<T> components = LabelPropagation.run(nodes, neighbors, hashOwner, partitions, pool);
        Map<Integer, List<T>> byLabel = new LinkedHashMap<>();   // labels are first-node indexes: group order
        for (T node : nodes) {
            byLabel.computeIfAbsent(components.labels().get(node), label -> new ArrayList<>()).add(node);
        }
        return new Groups<>(List.copyOf(byLabel.values()), components.rounds());
    }

    /** Edges whose ends have different owners. */
    long edgeCut(Map<T, Integer> ownerOf) {
        long cut = 0;
        for (T from : nodes) {
            for (T to : graph.successors(from)) {
                if (!ownerOf.get(from).equals(ownerOf.get(to))) {
                    cut++;
                }
            }
        }
        return cut;
    }
}
