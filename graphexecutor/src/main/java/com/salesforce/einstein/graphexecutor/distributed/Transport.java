package com.salesforce.einstein.graphexecutor.distributed;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * At-least-once delivery between partitions, standing in for a network or a log. Every batch reaches
 * its receiver's inbox {@code copies} times (1 normally; more to simulate redelivery), and a sender
 * that crashes and retries sends again: receivers must deduplicate.
 */
final class Transport<T> {
    private final List<Partition<T>> partitions;
    private final int copies;
    final AtomicLong batches = new AtomicLong();
    final AtomicLong edges = new AtomicLong();

    Transport(List<Partition<T>> partitions, int copies) {
        if (copies < 1) {
            throw new IllegalArgumentException("copies must be >= 1");
        }
        this.partitions = partitions;
        this.copies = copies;
    }

    void send(DecrementBatch<T> batch) {
        batches.incrementAndGet();
        edges.addAndGet(batch.edges());
        for (int i = 0; i < copies; i++) {
            partitions.get(batch.to()).deliver(batch);
        }
    }
}
