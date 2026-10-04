package com.salesforce.einstein.tagging.spi;

/**
 * What a {@link TagStore} adapter can guarantee. The core reads these at startup and either adapts
 * or refuses to start, so a weaker backend can never silently break a guarantee.
 */
public enum Capability {
    /** Assignment rows, counters and the outbox event commit or roll back together. Required. */
    ATOMIC_OUTBOX,
    /** {@link ReadPreference#snapshotRead()} reads see one consistent point in time. Needed to bootstrap the inverted index. */
    SNAPSHOT_READS,
    /** The store can route eventual/session reads to replicas. */
    READ_REPLICAS
}
