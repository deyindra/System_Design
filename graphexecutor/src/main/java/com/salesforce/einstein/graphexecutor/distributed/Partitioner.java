package com.salesforce.einstein.graphexecutor.distributed;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Assigns nodes to partitions. The partition that owns a node is the only writer of that node's state
 * (its pending-dependency counter, its label), so no counter is ever shared between workers.
 *
 * <p>Two strategies:
 * <ul>
 *   <li>{@link #hash()}: stateless and perfectly balanced in expectation, but blind to edges: with k
 *       partitions about (k-1)/k of all edges cross partitions, and each crossing edge is a message;</li>
 *   <li>{@link #greedy(double)}: linear deterministic greedy (LDG), a one-pass streaming partitioner
 *       that puts each node where most of its already-placed neighbors are, with a penalty for full
 *       partitions. Fed nodes in BFS order it keeps neighborhoods together and cuts far fewer edges.</li>
 * </ul>
 */
@FunctionalInterface
public interface Partitioner<T> {

    /**
     * @param nodes      the nodes to place, in the order a streaming partitioner should see them
     * @param neighbors  each node's neighbors in either direction
     * @param partitions how many partitions, at least 1
     * @return every node of {@code nodes} mapped to a partition in [0, partitions)
     */
    Map<T, Integer> assign(List<T> nodes, Function<T, ? extends Collection<T>> neighbors, int partitions);

    static <T> Partitioner<T> hash() {
        return (nodes, neighbors, partitions) -> {
            Map<T, Integer> assignment = new LinkedHashMap<>();
            for (T node : nodes) {
                assignment.put(node, Math.floorMod(node.hashCode(), partitions));
            }
            return assignment;
        };
    }

    /**
     * @param slack how far a partition may exceed a perfectly even share, e.g. 0.1 for 10%
     */
    static <T> Partitioner<T> greedy(double slack) {
        if (slack < 0) {
            throw new IllegalArgumentException("slack must be >= 0");
        }
        return (nodes, neighbors, partitions) -> {
            int capacity = (int) Math.ceil(nodes.size() * (1 + slack) / partitions);
            int[] load = new int[partitions];
            Map<T, Integer> assignment = new LinkedHashMap<>();
            for (T node : nodes) {
                int[] placedNeighbors = new int[partitions];
                for (T neighbor : neighbors.apply(node)) {
                    Integer p = assignment.get(neighbor);
                    if (p != null) {
                        placedNeighbors[p]++;
                    }
                }
                int best = -1;
                double bestScore = -1;
                for (int p = 0; p < partitions; p++) {
                    if (load[p] >= capacity) {
                        continue;
                    }
                    // LDG score: neighbors already there, discounted by how full the partition is
                    double score = placedNeighbors[p] * (1 - (double) load[p] / capacity);
                    if (score > bestScore || (score == bestScore && load[p] < load[best])) {
                        best = p;
                        bestScore = score;
                    }
                }
                assignment.put(node, best);   // capacity * partitions >= nodes, so some partition has room
                load[best]++;
            }
            return assignment;
        };
    }
}
