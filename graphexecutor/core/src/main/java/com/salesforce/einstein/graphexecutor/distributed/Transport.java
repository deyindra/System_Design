package com.salesforce.einstein.graphexecutor.distributed;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * At-least-once delivery between partitions, standing in for a network or a log. Every message reaches
 * its receiver's inbox {@code copies} times (1 normally; more to simulate redelivery), and a sender
 * that crashes and retries sends again: receivers must tolerate duplicates.
 *
 * <p>Generic over the message so every executor in this package shares it: {@link DecrementBatch} for
 * {@link DistributedTopologicalExecutor}, {@link VisitBatch} for {@link DistributedTraversalExecutor}.
 */
final class Transport<M extends Transport.Message> {

    /** A combined message from one partition to another. */
    interface Message {
        /** The receiving partition. */
        int to();

        /** How many uncombined messages (one per edge) this one stands for. */
        int edges();
    }

    /** Where a partition receives messages. Called concurrently by senders. */
    interface Inbox<M> {
        void deliver(M message);
    }

    private final List<? extends Inbox<M>> inboxes;
    private final int copies;
    final AtomicLong batches = new AtomicLong();
    final AtomicLong edges = new AtomicLong();

    Transport(List<? extends Inbox<M>> inboxes, int copies) {
        if (copies < 1) {
            throw new IllegalArgumentException("copies must be >= 1");
        }
        this.inboxes = inboxes;
        this.copies = copies;
    }

    void send(M message) {
        batches.incrementAndGet();
        edges.addAndGet(message.edges());
        for (int i = 0; i < copies; i++) {
            inboxes.get(message.to()).deliver(message);
        }
    }
}
