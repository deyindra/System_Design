package com.salesforce.einstein.graphexecutor.distributed;

import com.salesforce.einstein.graphexecutor.graph.BfsIterator;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Decides which partition owns each node, group by group:
 * <ul>
 *   <li>a group that fits in one partition's fair share ({@code ceil(V / k)} nodes) is placed
 *       <em>whole</em>, largest first onto the least-loaded partition (LPT bin packing). Its tasks then
 *       never send a message to another partition;</li>
 *   <li>a bigger group (on real graphs, the one giant component) is <em>split by node</em> with the
 *       {@link Partitioner}, fed in BFS order so a locality-aware partitioner keeps neighborhoods together.</li>
 * </ul>
 */
final class GroupPlacement {
    private GroupPlacement() {
    }

    record Placement<T>(Map<T, Integer> partitionOf, int placedWhole, int split) {
    }

    static <T> Placement<T> place(List<List<T>> groups, Map<T, List<T>> neighbors, int partitions,
                                  Partitioner<T> partitioner) {
        int total = groups.stream().mapToInt(List::size).sum();
        int fairShare = Math.max(1, (total + partitions - 1) / partitions);
        int[] load = new int[partitions];
        Map<T, Integer> partitionOf = new HashMap<>();
        int split = 0;

        List<List<T>> small = new ArrayList<>();
        for (List<T> group : groups) {
            if (group.size() <= fairShare) {
                small.add(group);
                continue;
            }
            List<T> bfsOrder = new ArrayList<>(group.size());
            new BfsIterator<>(List.of(group.get(0)), neighbors::get).forEachRemaining(bfsOrder::add);
            Map<T, Integer> assignment = partitioner.assign(bfsOrder, neighbors::get, partitions);
            for (T node : group) {
                Integer p = assignment.get(node);
                if (p == null || p < 0 || p >= partitions) {
                    throw new IllegalStateException("partitioner put " + node + " in partition " + p);
                }
                partitionOf.put(node, p);
                load[p]++;
            }
            split++;
        }

        small.sort(Comparator.comparingInt((List<T> g) -> g.size()).reversed());   // stable: ties keep group order
        for (List<T> group : small) {
            int lightest = 0;
            for (int p = 1; p < partitions; p++) {
                if (load[p] < load[lightest]) {
                    lightest = p;
                }
            }
            for (T node : group) {
                partitionOf.put(node, lightest);
            }
            load[lightest] += group.size();
        }
        return new Placement<>(partitionOf, small.size(), split);
    }
}
