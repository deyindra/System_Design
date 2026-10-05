package com.salesforce.einstein.graphexecutor.distributed;

import java.util.Map;

/**
 * Everything partition {@code from} tells partition {@code to} in one round, combined: each target node
 * appears once with the number of its dependencies that finished ({@code (v, -k)}, not k messages).
 *
 * <p><b>Deduplication.</b> Combining erases individual edges, so the receiver deduplicates by
 * {@link #id()}, (sender, round), instead of by edge id. That is equivalent and cheaper: a sender
 * recovering from a crash rebuilds the same state from the completion log and reuses the logged
 * outcomes instead of re-running them, so its replay re-sends the same batch
 * with the same id, and one id stands for every edge inside it. (The same idea as an idempotent
 * producer's sequence numbers in a log.)
 */
record DecrementBatch<T>(int from, int to, int round, Map<T, Integer> decrements) {

    record Id(int from, int round) {
    }

    Id id() {
        return new Id(from, round);
    }

    int edges() {
        return decrements.values().stream().mapToInt(Integer::intValue).sum();
    }
}
