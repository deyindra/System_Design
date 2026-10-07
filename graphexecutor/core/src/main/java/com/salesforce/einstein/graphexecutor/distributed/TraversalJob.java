package com.salesforce.einstein.graphexecutor.distributed;

import com.salesforce.einstein.graphexecutor.spi.CompletionLog.Outcome;
import com.salesforce.einstein.graphexecutor.spi.CompletionLog.Status;
import com.salesforce.einstein.graphexecutor.spi.CompletionLog;
import com.salesforce.einstein.graphexecutor.spi.GraphStore;
import com.salesforce.einstein.graphexecutor.spi.NodeCodec;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * A {@link DistributedTraversalExecutor}'s rules for a {@link Worker}: round r visits depth r, a node runs
 * if admitted, everything visited expands (failed and too-deep nodes too, as in {@link TraversalPartition}),
 * and lanes are taken round-robin, {@link DistributedTraversalExecutor#laneDelay} apart.
 */
final class TraversalJob<T> extends ClusterJob<T> {
    private final DistributedTraversalExecutor<T> executor;
    private final Set<T> roots;
    private final int maxDepth;
    private final long delayNanos;

    /** @param roots where to start; empty: every node without predecessors */
    TraversalJob(DistributedTraversalExecutor<T> executor, Set<T> roots, GraphStore<T> store, CompletionLog<T> log,
                 NodeCodec<T> codec, int batchSize) {
        super(store, log, codec, batchSize);
        this.executor = executor;
        this.roots = Set.copyOf(roots);
        this.maxDepth = executor.maxDepth();
        Duration delay = Objects.requireNonNull(executor.laneDelay(), "laneDelay");
        if (delay.isNegative()) {
            throw new IllegalArgumentException("laneDelay must be >= 0");
        }
        this.delayNanos = delay.toNanos();
    }

    @Override
    List<T> roots(int shard) {
        return roots.isEmpty() ? allSources(shard)
                : byKey(roots.stream().filter(root -> store.shardOf(root) == shard).toList());
    }

    @Override
    List<T> ready(List<T> reached, int round) {
        return reached.stream().filter(executor::admit).toList();
    }

    @Override
    List<T> order(List<T> frontier) {
        return Lanes.roundRobin(frontier, node -> Objects.requireNonNull(executor.laneOf(node), "laneOf"));
    }

    @Override
    Status run(T node, int round, Lanes lanes) {
        if (round > maxDepth) {
            log.record(node, new Outcome(Status.SKIPPED, round, "deeper than maxDepth " + maxDepth));
            return Status.SKIPPED;
        }
        lanes.awaitTurn(executor.laneOf(node), delayNanos);
        return Tasks.runAndLog(log, node, round, () -> executor.executeTask(node, round)) == null
                ? Status.DONE : Status.FAILED;
    }

    @Override
    boolean expands(Status status) {
        return true;
    }

    @Override
    void afterSend(int shard, int round, int attempt) {
        executor.afterSend(shard, round, attempt);
    }
}
