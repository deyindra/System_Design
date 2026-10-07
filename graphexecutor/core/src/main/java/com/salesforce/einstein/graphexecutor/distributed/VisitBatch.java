package com.salesforce.einstein.graphexecutor.distributed;

import java.util.Set;

/**
 * Everything partition {@code from} tells partition {@code to} in one round of a
 * {@link DistributedTraversalExecutor}: "these nodes of yours were reached", combined into one set, so a
 * node reached over many edges appears once.
 *
 * <p>Unlike a {@link DecrementBatch}, a visit is <b>idempotent</b>: applying it twice adds the same nodes
 * to the same set, and nodes already visited are ignored. Receivers still drop duplicates by
 * {@link #id()}, as the topological executor does, but only to save work; correctness never depends on it.
 *
 * @param edges how many edges this batch combines (one uncombined message each)
 */
record VisitBatch<T>(int from, int to, int round, Set<T> nodes, int edges) implements Transport.Message {

    record Id(int from, int round) {
    }

    Id id() {
        return new Id(from, round);
    }
}
