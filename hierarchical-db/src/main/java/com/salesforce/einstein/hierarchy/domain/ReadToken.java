package com.salesforce.einstein.hierarchy.domain;

import org.springframework.lang.Nullable;

import java.util.Locale;

/**
 * {@code X-Read-Token}: the shard a write went to and the primary's WAL position right after it committed, as
 * {@code <shard>:<hi>/<lo>} (the LSN in Postgres's own text form, e.g. {@code s0:0/16B3748}). A replica whose
 * {@code pg_last_wal_replay_lsn()} is at or past {@link #lsn} has the write, so the writer can read it there.
 */
public record ReadToken(String shard, long lsn) {
    public static final ReadToken NONE = new ReadToken("", 0);

    public boolean isNone() {
        return lsn == 0;
    }

    public String format() {
        return shard + ":" + formatLsn(lsn);
    }

    /** Lenient on purpose: a malformed or foreign token just means "no token", never an error. */
    public static ReadToken parse(@Nullable String header) {
        if (header == null || header.isBlank()) {
            return NONE;
        }
        int colon = header.indexOf(':');
        if (colon <= 0) {
            return NONE;
        }
        try {
            long lsn = parseLsn(header.substring(colon + 1).trim());
            return lsn == 0 ? NONE : new ReadToken(header.substring(0, colon).trim(), lsn);
        } catch (IllegalArgumentException e) {
            return NONE;
        }
    }

    public static long parseLsn(String text) {
        int slash = text.indexOf('/');
        if (slash <= 0 || slash == text.length() - 1) {
            throw new IllegalArgumentException("not an LSN: " + text);
        }
        long hi = Long.parseLong(text.substring(0, slash), 16);
        long lo = Long.parseLong(text.substring(slash + 1), 16);
        if (hi < 0 || hi > 0xFFFF_FFFFL || lo < 0 || lo > 0xFFFF_FFFFL) {
            throw new IllegalArgumentException("not an LSN: " + text);
        }
        return (hi << 32) | lo;
    }

    public static String formatLsn(long lsn) {
        return Long.toHexString(lsn >>> 32).toUpperCase(Locale.ROOT) + "/"
                + Long.toHexString(lsn & 0xFFFF_FFFFL).toUpperCase(Locale.ROOT);
    }

    /** The later of two tokens for the same shard. */
    public ReadToken max(ReadToken other) {
        if (other.isNone()) {
            return this;
        }
        if (isNone()) {
            return other;
        }
        return Long.compareUnsigned(lsn, other.lsn) >= 0 ? this : other;
    }
}
