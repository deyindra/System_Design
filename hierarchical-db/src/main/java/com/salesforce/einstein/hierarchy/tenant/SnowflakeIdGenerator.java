package com.salesforce.einstein.hierarchy.tenant;

import com.salesforce.einstein.hierarchy.spi.IdGenerator;

import java.time.Clock;

/**
 * 64-bit, roughly time-ordered ids with no coordination: 41 bits of milliseconds since 2024-01-01, 10 bits of worker id
 * (from the pod ordinal) and 12 bits of per-millisecond sequence. That is 4096 ids per ms per pod, enough for ~69 years.
 * Time order keeps B-tree inserts on the right edge of the primary key and means ids never collide across shards, so a
 * tenant can be moved to another shard as is.
 *
 * <p>If the clock steps backwards, ids keep coming from the last timestamp seen, so they stay unique.
 */
public final class SnowflakeIdGenerator implements IdGenerator {
    static final long EPOCH_MS = 1_704_067_200_000L;
    private static final int WORKER_BITS = 10;
    private static final int SEQ_BITS = 12;
    private static final long MAX_SEQ = (1L << SEQ_BITS) - 1;

    private final long workerId;
    private final Clock clock;
    private long lastMs = -1;
    private long seq;

    public SnowflakeIdGenerator(int workerId, Clock clock) {
        if (workerId < 0 || workerId >= 1 << WORKER_BITS) {
            throw new IllegalArgumentException("workerId must be in [0, 1024)");
        }
        this.workerId = workerId;
        this.clock = clock;
    }

    @Override
    public synchronized long nextId() {
        long now = Math.max(clock.millis(), lastMs);
        if (now == lastMs) {
            seq = (seq + 1) & MAX_SEQ;
            if (seq == 0) {
                now = lastMs + 1;   // sequence exhausted: borrow the next millisecond
            }
        } else {
            seq = 0;
        }
        lastMs = now;
        return ((now - EPOCH_MS) << (WORKER_BITS + SEQ_BITS)) | (workerId << SEQ_BITS) | seq;
    }
}
