package com.salesforce.einstein.tagging.domain;

import java.util.Objects;

/**
 * Returned by every write and echoed back by the client for {@link Consistency#SESSION} reads.
 *
 * <p>{@code seq} is the outbox sequence of the write's event on shard {@code shard}. A projection
 * (replica, cache entry, inverted index) can serve a session read once it has applied every event of
 * that shard up to {@code seq}. Clients keep the maximum token they've seen; {@link #max} does the merge.
 * Wire format: {@code <shard>:<seq>}.
 */
public record ConsistencyToken(String shard, long seq) {
    public static final ConsistencyToken NONE = new ConsistencyToken("", 0);

    public ConsistencyToken {
        Objects.requireNonNull(shard, "shard");
        if (seq < 0) {
            throw new IllegalArgumentException("seq < 0");
        }
    }

    public static ConsistencyToken parse(String header) {
        if (header == null || header.isBlank()) {
            return NONE;
        }
        int i = header.lastIndexOf(':');
        if (i <= 0 || i == header.length() - 1) {
            throw new InvalidRequestException("malformed consistency token");
        }
        try {
            return new ConsistencyToken(header.substring(0, i), Long.parseLong(header.substring(i + 1)));
        } catch (NumberFormatException e) {
            throw new InvalidRequestException("malformed consistency token");
        }
    }

    public boolean isNone() {
        return seq == 0;
    }

    /** The newer of two tokens of the same shard; a token from another shard wins (the tenant moved). */
    public ConsistencyToken max(ConsistencyToken other) {
        if (other == null || other.isNone()) {
            return this;
        }
        if (isNone() || !shard.equals(other.shard)) {
            return other;
        }
        return seq >= other.seq ? this : other;
    }

    public String format() {
        return isNone() ? "" : shard + ":" + seq;
    }
}
