package com.salesforce.einstein.graphexecutor.distributed;

import com.salesforce.einstein.graphexecutor.spi.CompletionLog.Outcome;
import com.salesforce.einstein.graphexecutor.spi.CompletionLog.Status;
import com.salesforce.einstein.graphexecutor.spi.CompletionLog;
import com.salesforce.einstein.graphexecutor.spi.GraphStore;
import com.salesforce.einstein.graphexecutor.spi.NodeCodec;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A {@link DistributedTopologicalExecutor}'s rules for a {@link Worker}: Kahn's waves without counters.
 *
 * <p>A {@link Partition} keeps a pending count per node. Here nothing survives a round, so readiness is
 * asked of the log instead: a node reached in round r - 1 (some predecessor finished then) runs in round r
 * if <b>every</b> predecessor is {@code DONE} before r. Its last predecessor to finish is the one that sent
 * it, so it is checked exactly when it can first be ready. Only {@code DONE} expands: a failed node's
 * dependants never become ready, as in {@link Partition}.
 *
 * <p>Nodes on a cycle are never ready, so they never run; there is no dry run that rejects the graph
 * first (it would read the whole graph), and the report counts them as not run.
 */
final class TopologicalJob<T> extends ClusterJob<T> {
    private final DistributedTopologicalExecutor<T> executor;

    TopologicalJob(DistributedTopologicalExecutor<T> executor, GraphStore<T> store, CompletionLog<T> log,
                   NodeCodec<T> codec, int batchSize) {
        super(store, log, codec, batchSize);
        this.executor = executor;
    }

    @Override
    List<T> roots(int shard) {
        return allSources(shard);
    }

    @Override
    List<T> ready(List<T> reached, int round) {
        if (round == 0) {
            return reached;   // sources: no predecessors
        }
        Map<T, List<T>> predecessors = batched(reached, store::predecessors);
        Set<T> all = new HashSet<>();
        predecessors.values().forEach(all::addAll);
        Map<T, Outcome> outcomes = batched(all, log::outcomes);
        return reached.stream().filter(node -> predecessors.getOrDefault(node, List.of()).stream().allMatch(p -> {
            Outcome outcome = outcomes.get(p);
            return outcome != null && outcome.status() == Status.DONE && outcome.round() < round;
        })).toList();
    }

    @Override
    Status run(T node, int round, Lanes lanes) {
        return Tasks.runAndLog(log, node, round, () -> executor.executeTask(node)) == null
                ? Status.DONE : Status.FAILED;
    }

    @Override
    boolean expands(Status status) {
        return status == Status.DONE;
    }

    @Override
    void afterSend(int shard, int round, int attempt) {
        executor.afterSend(shard, round, attempt);
    }
}
