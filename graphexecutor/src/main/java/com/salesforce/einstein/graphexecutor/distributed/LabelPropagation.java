package com.salesforce.einstein.graphexecutor.distributed;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;

/**
 * Weakly connected components without a global BFS: every node starts with its own label (its index)
 * and, in rounds, adopts the smallest label among its neighbors (edges in either direction). When no
 * label changes, each component carries the index of its first node, so components come out in the same
 * order as {@code TaskExecutor.findGroups}.
 *
 * <p>Each partition owns its nodes' labels and is the only writer of them. Only nodes whose label changed
 * last round send, and messages are combined per receiving partition (one minimum per target node), so
 * a round costs one message per pair of partitions that share a changed edge. Rounds are O(diameter).
 *
 * <p>Distributed union-find converges in fewer rounds on long chains but needs pointer-jumping across
 * partitions; label propagation is the simpler fit for bulk-synchronous rounds.
 */
final class LabelPropagation {
    private LabelPropagation() {
    }

    record Result<T>(Map<T, Integer> labels, int rounds, long messages) {
    }

    static <T> Result<T> run(List<T> nodes, Map<T, List<T>> neighbors, Map<T, Integer> ownerOf,
                             int partitions, Executor pool) {
        List<Map<T, Integer>> labels = new ArrayList<>();   // per partition: owned node -> label
        List<Set<T>> changed = new ArrayList<>();
        for (int p = 0; p < partitions; p++) {
            labels.add(new HashMap<>());
            changed.add(new HashSet<>());
        }
        for (int i = 0; i < nodes.size(); i++) {
            int owner = ownerOf.get(nodes.get(i));
            labels.get(owner).put(nodes.get(i), i);
            changed.get(owner).add(nodes.get(i));
        }

        int rounds = 0;
        long messages = 0;
        while (changed.stream().anyMatch(set -> !set.isEmpty())) {
            // send: partition p writes only its own outboxes, one combined map per receiving partition
            List<List<Map<T, Integer>>> outboxes = Supersteps.onEveryPartition(pool, partitions, p -> {
                List<Map<T, Integer>> out = new ArrayList<>(partitions);
                for (int q = 0; q < partitions; q++) {
                    out.add(new HashMap<>());
                }
                for (T node : changed.get(p)) {
                    int label = labels.get(p).get(node);
                    for (T neighbor : neighbors.get(node)) {
                        out.get(ownerOf.get(neighbor)).merge(neighbor, label, Math::min);   // the combiner
                    }
                }
                changed.get(p).clear();
                return out;
            });
            for (List<Map<T, Integer>> out : outboxes) {
                messages += out.stream().filter(box -> !box.isEmpty()).count();
            }
            // receive: partition q is the only writer of its labels
            Supersteps.onEveryPartition(pool, partitions, q -> {
                for (List<Map<T, Integer>> out : outboxes) {
                    out.get(q).forEach((node, label) -> {
                        if (label < labels.get(q).get(node)) {
                            labels.get(q).put(node, label);
                            changed.get(q).add(node);
                        }
                    });
                }
                return null;
            });
            rounds++;
        }

        Map<T, Integer> merged = new HashMap<>();
        labels.forEach(merged::putAll);
        return new Result<>(merged, rounds, messages);
    }
}
