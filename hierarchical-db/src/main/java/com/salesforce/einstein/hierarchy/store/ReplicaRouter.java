package com.salesforce.einstein.hierarchy.store;

import com.salesforce.einstein.hierarchy.domain.ReadToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Picks the server for a read (DESIGN.md §7.5).
 *
 * <ul>
 *   <li>With a read token: any healthy replica that has replayed at least the token's LSN, so the writer sees their own
 *       write. If none has yet (lag is milliseconds), the primary.</li>
 *   <li>Without a token: any healthy replica less than {@code maxLag} behind, else the primary.</li>
 * </ul>
 *
 * <p>{@link #refresh} runs every few milliseconds. It samples the primary's WAL position and each replica's replay
 * position; a replica's lag is the age of the newest primary sample it has replayed past.
 */
public final class ReplicaRouter {
    private static final Logger log = LoggerFactory.getLogger(ReplicaRouter.class);
    private static final long SAMPLE_WINDOW_MS = 60_000;

    /** Where LSNs come from; SQL in production, a fake in unit tests. */
    public interface LsnSource {
        long primaryLsn();

        long replayLsn(Db replica);
    }

    private record Sample(long atMs, long lsn) {
    }

    private static final class Replica {
        final Db db;
        volatile boolean healthy;
        volatile long replayed;
        volatile long lagMs = Long.MAX_VALUE;

        Replica(Db db) {
            this.db = db;
        }
    }

    private final Db primary;
    private final List<Replica> replicas = new ArrayList<>();
    private final LsnSource lsns;
    private final Clock clock;
    private final long maxLagMs;
    private final Deque<Sample> samples = new ArrayDeque<>();
    private final AtomicInteger next = new AtomicInteger();

    public ReplicaRouter(Db primary, List<Db> replicas, LsnSource lsns, Clock clock, Duration maxLag) {
        this.primary = primary;
        replicas.forEach(r -> this.replicas.add(new Replica(r)));
        this.lsns = lsns;
        this.clock = clock;
        this.maxLagMs = maxLag.toMillis();
    }

    public static LsnSource sql(Db primary) {
        return new LsnSource() {
            @Override
            public long primaryLsn() {
                return primary.currentLsn();
            }

            @Override
            public long replayLsn(Db replica) {
                return replica.replayLsn();
            }
        };
    }

    public List<Db> replicas() {
        return replicas.stream().map(r -> r.db).toList();
    }

    public Db primary() {
        return primary;
    }

    public synchronized void refresh() {
        if (replicas.isEmpty()) {
            return;
        }
        long now = clock.millis();
        long primaryLsn;
        try {
            primaryLsn = lsns.primaryLsn();
        } catch (RuntimeException e) {
            log.warn("cannot read the primary's WAL position; keeping the last replica state: {}", e.toString());
            log.debug("primary WAL position probe failed", e);
            return;
        }
        samples.addLast(new Sample(now, primaryLsn));
        while (samples.size() > 1 && samples.peekFirst().atMs() < now - SAMPLE_WINDOW_MS) {
            samples.removeFirst();
        }
        for (Replica r : replicas) {
            try {
                long replayed = lsns.replayLsn(r.db);
                r.replayed = replayed;
                r.lagMs = lagMs(now, replayed);
                r.healthy = true;
            } catch (RuntimeException e) {
                if (r.healthy) {
                    log.warn("replica {} is unreachable; reads go elsewhere: {}", r.db.name(), e.toString());
                    log.debug("replica {} probe failed", r.db.name(), e);
                }
                r.healthy = false;
            }
        }
    }

    private long lagMs(long now, long replayed) {
        var it = samples.descendingIterator();
        while (it.hasNext()) {
            Sample s = it.next();
            if (Long.compareUnsigned(replayed, s.lsn()) >= 0) {
                return now - s.atMs();
            }
        }
        return Long.MAX_VALUE;
    }

    /** {@code token} must already be this shard's (or {@link ReadToken#NONE}). */
    public Db forRead(ReadToken token) {
        int n = replicas.size();
        if (n == 0) {
            return primary;
        }
        int start = Math.floorMod(next.getAndIncrement(), n);
        for (int i = 0; i < n; i++) {
            Replica r = replicas.get((start + i) % n);
            if (!r.healthy) {
                continue;
            }
            boolean fresh = token.isNone()
                    ? r.lagMs <= maxLagMs
                    : Long.compareUnsigned(r.replayed, token.lsn()) >= 0;
            if (fresh) {
                return r.db;
            }
        }
        return primary;
    }

    /** The worst lag among healthy replicas; the move worker pauses while it is high. 0 without replicas. */
    public long maxLagMs() {
        long worst = 0;
        for (Replica r : replicas) {
            if (r.healthy) {
                worst = Math.max(worst, r.lagMs);
            }
        }
        return worst;
    }
}
