package com.salesforce.einstein.tagging.spi;

import com.salesforce.einstein.tagging.domain.Consistency;

/**
 * How a read may be served.
 *
 * @param minSeq   for {@link Consistency#SESSION}: the replica must have relayed at least this outbox seq
 * @param snapshot run the whole read at one consistent point in time (repeatable read)
 */
public record ReadPreference(Consistency consistency, long minSeq, boolean snapshot) {
    private static final ReadPreference STRONG = new ReadPreference(Consistency.STRONG, 0, false);
    private static final ReadPreference EVENTUAL = new ReadPreference(Consistency.EVENTUAL, 0, false);
    private static final ReadPreference SNAPSHOT = new ReadPreference(Consistency.STRONG, 0, true);

    public static ReadPreference strong() {
        return STRONG;
    }

    public static ReadPreference eventual() {
        return EVENTUAL;
    }

    public static ReadPreference session(long minSeq) {
        return minSeq <= 0 ? EVENTUAL : new ReadPreference(Consistency.SESSION, minSeq, false);
    }

    public static ReadPreference snapshotRead() {
        return SNAPSHOT;
    }
}
