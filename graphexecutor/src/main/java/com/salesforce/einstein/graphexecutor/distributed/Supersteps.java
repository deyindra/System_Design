package com.salesforce.einstein.graphexecutor.distributed;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
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
}
