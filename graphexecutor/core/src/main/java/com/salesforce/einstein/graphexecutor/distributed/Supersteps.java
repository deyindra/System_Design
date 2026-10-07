package com.salesforce.einstein.graphexecutor.distributed;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.function.IntConsumer;
import java.util.function.IntFunction;

/** The barrier of bulk-synchronous processing: run one step on every partition, wait for all of them. */
final class Supersteps {
    private Supersteps() {
    }

    /** Runs {@code step} for partitions 0..k-1 in parallel and returns their results in partition order. */
    static <R> List<R> onEveryPartition(Executor pool, int partitions, IntFunction<R> step) {
        List<CompletableFuture<R>> futures = new ArrayList<>(partitions);
        for (int p = 0; p < partitions; p++) {
            int partition = p;
            futures.add(CompletableFuture.supplyAsync(() -> step.apply(partition), pool));
        }
        try {
            return futures.stream().map(CompletableFuture::join).toList();   // the barrier
        } catch (CompletionException e) {
            if (e.getCause() instanceof RuntimeException cause) {
                throw cause;
            }
            if (e.getCause() instanceof Error cause) {
                throw cause;
            }
            throw e;
        }
    }

    /**
     * One round: {@code compute} on every partition, barrier, {@code apply} on every partition, barrier.
     * Returns what the computes completed, in {@code order}.
     */
    static <T> List<T> round(Executor pool, int partitions, IntFunction<List<T>> compute, IntConsumer apply,
                             Comparator<? super T> order) {
        List<List<T>> done = onEveryPartition(pool, partitions, compute);
        onEveryPartition(pool, partitions, p -> {
            apply.accept(p);
            return null;
        });
        List<T> completed = new ArrayList<>();
        done.forEach(completed::addAll);
        completed.sort(order);
        return List.copyOf(completed);
    }

    /**
     * One partition's compute with crash recovery: an unchecked exception is a crash, so {@code recover}
     * rebuilds that partition (its own state only; the others keep their progress) and the attempt is
     * replayed, up to {@code maxAttempts}. Task failures never get here: compute records them.
     *
     * @param recoverable false when there is no log to recover from: the first crash fails the run
     */
    static <R> R withRecovery(int partition, int round, int maxAttempts, boolean recoverable,
                              IntFunction<R> attempt, Runnable recover) {
        for (int a = 0; ; a++) {
            try {
                return attempt.apply(a);
            } catch (RuntimeException crash) {
                if (!recoverable || a + 1 >= maxAttempts) {
                    throw new IllegalStateException("partition " + partition + " crashed " + maxAttempts
                            + " times in round " + round, crash);
                }
                recover.run();
            }
        }
    }
}
