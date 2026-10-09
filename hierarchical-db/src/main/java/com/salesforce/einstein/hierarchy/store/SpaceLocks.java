package com.salesforce.einstein.hierarchy.store;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;

import java.time.Duration;
import java.util.Collection;
import java.util.TreeSet;
import java.util.UUID;

/**
 * The per-space advisory lock (DESIGN.md §8.1): shared for creates, exclusive for anything that rewrites paths. It lives
 * in shared memory, touches no row and is released at commit, so thousands of concurrent creates in one space never
 * wait for each other, only for a move.
 *
 * <p>Must be called inside a transaction. A wait longer than the lock timeout fails with SQLSTATE 55P03, which Spring
 * turns into {@code CannotAcquireLockException} and the API into {@code 409 Retry-After}.
 */
public final class SpaceLocks {
    private static final RowCallbackHandler IGNORE = rs -> { };

    private SpaceLocks() {
    }

    public static void shared(JdbcTemplate j, UUID tenant, long spaceId, Duration timeout) {
        setTimeout(j, timeout);
        j.query("SELECT pg_advisory_xact_lock_shared(hashtextextended(?::text || ':' || ?, 0))", IGNORE,
                tenant, Long.toString(spaceId));
    }

    /** Locks in ascending space id order, so two cross-space moves can't deadlock. */
    public static void exclusive(JdbcTemplate j, UUID tenant, Collection<Long> spaceIds, Duration timeout) {
        setTimeout(j, timeout);
        for (long spaceId : new TreeSet<>(spaceIds)) {
            j.query("SELECT pg_advisory_xact_lock(hashtextextended(?::text || ':' || ?, 0))", IGNORE,
                    tenant, Long.toString(spaceId));
        }
    }

    private static void setTimeout(JdbcTemplate j, Duration timeout) {
        j.query("SELECT set_config('lock_timeout', ?, true)", IGNORE, timeout.toMillis() + "ms");
    }
}
