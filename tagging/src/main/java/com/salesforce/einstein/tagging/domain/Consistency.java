package com.salesforce.einstein.tagging.domain;

/**
 * Read consistency chosen per request. The default is {@link #STRONG}; the others are opt-in relaxations.
 *
 * <ul>
 *   <li>{@link #STRONG}: linearizable per tenant. Rows come from the shard primary; a search may use the
 *       index only once it has reached a read barrier taken on the primary.</li>
 *   <li>{@link #SESSION}: read-your-writes. Any replica, cache or index whose watermark has reached the
 *       caller's {@link ConsistencyToken} may serve; otherwise the read waits briefly, then falls back to primary.</li>
 *   <li>{@link #EVENTUAL}: whatever is closest (cache, index, replica). Bounded staleness, typically under 1 s.</li>
 * </ul>
 */
public enum Consistency {
    STRONG, SESSION, EVENTUAL
}
