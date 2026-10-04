package com.salesforce.einstein.tagging.index;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.salesforce.einstein.tagging.domain.TagEvent;
import com.salesforce.einstein.tagging.domain.TagQuery;
import com.salesforce.einstein.tagging.spi.Posting;
import com.salesforce.einstein.tagging.spi.ReadPreference;
import com.salesforce.einstein.tagging.spi.SearchPostings;
import com.salesforce.einstein.tagging.spi.TagSearchIndex;
import com.salesforce.einstein.tagging.spi.TagStore;
import org.roaringbitmap.FastAggregation;
import org.roaringbitmap.PeekableIntIterator;
import org.roaringbitmap.RoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;

/**
 * In-process inverted index: one compressed {@link RoaringBitmap} posting list per (tenant, tag) over
 * the store's dense {@code entitySeq}s. AND, OR and ANDNOT of million-entry lists take microseconds,
 * and a whale tenant's whole index fits in tens of MB.
 *
 * <h2>Lifecycle of a tenant</h2>
 * Tenants are loaded lazily on their first search and evicted when idle (bounded by {@code maxTenants}).
 * <ol>
 *   <li><b>LOADING</b>: the state is created first, so from then on every event of the tenant is
 *       buffered. Then one snapshot read takes the relay position {@code S} and scans all postings.</li>
 *   <li>Buffered events with {@code seq > S} are replayed in order. Events at or below {@code S} are
 *       already in the snapshot, since the relay commits its position only after they were committed.</li>
 *   <li><b>READY</b>: events apply directly. Re-deliveries ({@code seq <= appliedSeq}) are ignored.</li>
 * </ol>
 * One race remains: an event can be <i>delivered</i> before the buffer existed while its relay
 * transaction has not committed {@code S} yet. To catch it, the index tracks the highest seq delivered per
 * shard ({@code D}, read right after the buffer is created). If {@code S < D}, a delivered event may be
 * missing from both the snapshot and the buffer, so the bootstrap retries.
 *
 * <p>Correctness of the incremental path: the events of one (entity, tag) pair are applied in commit
 * order (see {@link TagEvent}), so after applying a prefix of the stream the bitmap equals the store as
 * of that prefix.
 *
 * <p>Posting ids are {@code int}s treated as unsigned, so up to 2^32 entities per shard. A tenant
 * whose seqs go beyond that is served by the store (the {@code search} call returns empty).
 */
public final class RoaringInvertedIndex implements TagSearchIndex {
    private static final Logger log = LoggerFactory.getLogger(RoaringInvertedIndex.class);
    private static final long MAX_SEQ = 0xFFFF_FFFFL;
    private static final int MAX_BOOTSTRAP_ATTEMPTS = 20;

    private final Function<String, TagStore> storeOf;
    private final Executor bootstrapExecutor;
    private final Cache<String, TenantIndex> tenants;
    private final Map<String, Long> deliveredSeq = new ConcurrentHashMap<>();

    /**
     * @param storeOf           the tenant's shard store (used for the bootstrap snapshot)
     * @param maxTenants        how many tenants are kept loaded; least recently used ones are evicted
     * @param bootstrapExecutor runs bootstraps off the request thread
     */
    public RoaringInvertedIndex(Function<String, TagStore> storeOf, long maxTenants, Executor bootstrapExecutor) {
        this.storeOf = storeOf;
        this.bootstrapExecutor = bootstrapExecutor;
        this.tenants = Caffeine.newBuilder().maximumSize(maxTenants).build();
    }

    enum State { LOADING, READY, UNSUPPORTED }

    /** Everything about one tenant, guarded by its own lock: writers (events) never block other tenants. */
    static final class TenantIndex {
        final String shard;
        final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
        final Map<Long, RoaringBitmap> postings = new HashMap<>();
        final Map<String, RoaringBitmap> byType = new HashMap<>();
        final Set<Long> deletedTags = new HashSet<>();
        List<TagEvent> buffer = new ArrayList<>();
        volatile State state = State.LOADING;
        volatile long appliedSeq = -1;

        TenantIndex(String shard) {
            this.shard = shard;
        }
    }

    // --- events ----------------------------------------------------------------------------------

    @Override
    public void onEvents(List<TagEvent> events) {
        for (TagEvent e : events) {
            // Record delivery before looking up the tenant (see the bootstrap race in the class docs).
            deliveredSeq.merge(e.shard(), e.seq(), Math::max);
            TenantIndex t = tenants.getIfPresent(e.tenantId());
            if (t == null) {
                continue;   // not loaded: the next bootstrap reads it from the snapshot
            }
            if (!t.shard.equals(e.shard())) {
                tenants.invalidate(e.tenantId());   // the tenant moved shards; rebuild from the new one
                continue;
            }
            t.lock.writeLock().lock();
            try {
                if (t.state == State.LOADING) {
                    t.buffer.add(e);
                } else if (t.state == State.READY) {
                    apply(t, e);
                }
            } finally {
                t.lock.writeLock().unlock();
            }
        }
    }

    /** Applies one event. Called with the tenant's write lock held. */
    private void apply(TenantIndex t, TagEvent e) {
        if (e.seq() <= t.appliedSeq) {
            return;
        }
        t.appliedSeq = e.seq();
        switch (e.type()) {
            case TAGS_ATTACHED -> {
                if (e.entitySeq() > MAX_SEQ) {
                    t.state = State.UNSUPPORTED;
                    clear(t);
                    return;
                }
                int id = (int) e.entitySeq();
                t.byType.computeIfAbsent(e.entityType(), k -> new RoaringBitmap()).add(id);
                for (Long tagId : e.tagIds()) {
                    if (!t.deletedTags.contains(tagId)) {
                        t.postings.computeIfAbsent(tagId, k -> new RoaringBitmap()).add(id);
                    }
                }
            }
            case TAGS_DETACHED -> {
                for (Long tagId : e.tagIds()) {
                    RoaringBitmap b = t.postings.get(tagId);
                    if (b != null) {
                        b.remove((int) e.entitySeq());
                        if (b.isEmpty()) {
                            t.postings.remove(tagId);
                        }
                    }
                }
            }
            case TAG_DELETED -> {
                t.deletedTags.add(e.tagId());
                t.postings.remove(e.tagId());
            }
            case TAG_CREATED, TAG_UPDATED -> {
                // metadata only; postings unchanged
            }
        }
    }

    private static void clear(TenantIndex t) {
        t.postings.clear();
        t.byType.clear();
        t.buffer = List.of();
    }

    // --- search ----------------------------------------------------------------------------------

    @Override
    public Optional<SearchPostings> search(String tenantId, TagQuery q) {
        TenantIndex t = tenants.getIfPresent(tenantId);
        if (t == null) {
            startBootstrap(tenantId);
            t = tenants.getIfPresent(tenantId);   // a synchronous executor may have finished already
        }
        if (t == null || t.state != State.READY) {
            return Optional.empty();
        }
        t.lock.readLock().lock();
        try {
            return t.state == State.READY ? Optional.of(evaluate(t, q)) : Optional.empty();
        } finally {
            t.lock.readLock().unlock();
        }
    }

    private static SearchPostings evaluate(TenantIndex t, TagQuery q) {
        RoaringBitmap acc;   // always a private copy, so the in-place operations below are safe
        if (q.all().isEmpty()) {
            acc = union(t, q.any());   // TagQuery guarantees 'all' or 'any' is non-empty
        } else {
            acc = intersection(t, q.all());
            if (acc.isEmpty()) {
                return new SearchPostings(List.of(), 0, false);
            }
            if (!q.any().isEmpty()) {
                acc.and(union(t, q.any()));
            }
        }
        if (!q.none().isEmpty()) {
            acc.andNot(union(t, q.none()));
        }
        if (q.entityType() != null) {
            RoaringBitmap type = t.byType.get(q.entityType());
            acc = type == null ? new RoaringBitmap() : RoaringBitmap.and(acc, type);
        }

        long total = acc.getLongCardinality();
        List<Long> page = new ArrayList<>(Math.min(q.limit(), (int) Math.min(total, 1024)));
        if (q.afterSeq() >= MAX_SEQ) {
            return new SearchPostings(page, total, false);
        }
        PeekableIntIterator it = acc.getIntIterator();
        it.advanceIfNeeded((int) (q.afterSeq() + 1));   // unsigned order, same as Roaring's
        while (it.hasNext() && page.size() < q.limit()) {
            page.add(Integer.toUnsignedLong(it.next()));
        }
        return new SearchPostings(page, total, it.hasNext());
    }

    /** Entities carrying every tag; empty as soon as one tag has no postings. */
    private static RoaringBitmap intersection(TenantIndex t, Set<Long> tagIds) {
        List<RoaringBitmap> lists = new ArrayList<>();
        for (Long tagId : tagIds) {
            RoaringBitmap b = t.postings.get(tagId);
            if (b == null) {
                return new RoaringBitmap();
            }
            lists.add(b);
        }
        return lists.size() == 1 ? lists.get(0).clone() : FastAggregation.and(lists.iterator());
    }

    private static RoaringBitmap union(TenantIndex t, Set<Long> tagIds) {
        List<RoaringBitmap> lists = new ArrayList<>();
        for (Long tagId : tagIds) {
            RoaringBitmap b = t.postings.get(tagId);
            if (b != null) {
                lists.add(b);
            }
        }
        if (lists.isEmpty()) {
            return new RoaringBitmap();
        }
        return lists.size() == 1 ? lists.get(0).clone() : FastAggregation.or(lists.iterator());
    }

    @Override
    public long watermark(String tenantId) {
        TenantIndex t = tenants.getIfPresent(tenantId);
        return t == null || t.state != State.READY ? -1 : t.appliedSeq;
    }

    // --- bootstrap -------------------------------------------------------------------------------

    private void startBootstrap(String tenantId) {
        TagStore store = storeOf.apply(tenantId);
        boolean[] created = {false};
        TenantIndex t = tenants.get(tenantId, k -> {
            created[0] = true;
            return new TenantIndex(store.shardId());
        });
        if (created[0]) {
            try {
                bootstrapExecutor.execute(() -> bootstrap(tenantId, t, store));
            } catch (RuntimeException e) {   // executor saturated or shut down: try again on a later search
                tenants.asMap().remove(tenantId, t);
                log.warn("index bootstrap for tenant {} not scheduled: {}", tenantId, e.toString());
            }
        }
    }

    private void bootstrap(String tenantId, TenantIndex t, TagStore store) {
        try {
            for (int attempt = 1; ; attempt++) {
                long delivered = deliveredSeq.getOrDefault(store.shardId(), 0L);
                Snapshot snap = store.read(tenantId, ReadPreference.snapshotRead(), r -> {
                    Snapshot s = new Snapshot(r.relayedSeq());
                    r.scanAssignments(s::add);
                    return s;
                });
                if (snap.relayedSeq >= delivered) {
                    install(t, snap);
                    log.debug("index for tenant {} ready at seq {}", tenantId, t.appliedSeq);
                    return;
                }
                if (attempt == MAX_BOOTSTRAP_ATTEMPTS) {   // expected under relay lag: no stack trace, retried on a later search
                    log.warn("index bootstrap for tenant {} gave up: relay position {} stays behind delivered seq {}; "
                            + "the store will serve its searches", tenantId, snap.relayedSeq, delivered);
                    tenants.asMap().remove(tenantId, t);
                    return;
                }
                // Bounded backoff (MAX_BOOTSTRAP_ATTEMPTS) until the relay commits its position. That
                // happens in the store, so there is no local signal to wait on instead.
                //noinspection BusyWait
                Thread.sleep(Math.min(5L * attempt, 100));
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            tenants.asMap().remove(tenantId, t);
        } catch (RuntimeException e) {
            log.warn("index bootstrap for tenant {} failed; the store will serve its searches", tenantId, e);
            tenants.asMap().remove(tenantId, t);
        }
    }

    private void install(TenantIndex t, Snapshot snap) {
        t.lock.writeLock().lock();
        try {
            if (snap.unsupported) {
                t.state = State.UNSUPPORTED;
                clear(t);
                return;
            }
            snap.postings.values().forEach(RoaringBitmap::runOptimize);
            t.postings.putAll(snap.postings);
            t.byType.putAll(snap.byType);
            t.appliedSeq = snap.relayedSeq;
            t.state = State.READY;
            for (TagEvent e : t.buffer) {
                apply(t, e);   // skips seq <= relayedSeq
            }
            t.buffer = List.of();
        } finally {
            t.lock.writeLock().unlock();
        }
    }

    /** Postings gathered from one snapshot read. */
    private static final class Snapshot {
        final long relayedSeq;
        final Map<Long, RoaringBitmap> postings = new HashMap<>();
        final Map<String, RoaringBitmap> byType = new HashMap<>();
        boolean unsupported;

        Snapshot(long relayedSeq) {
            this.relayedSeq = relayedSeq;
        }

        void add(Posting p) {
            if (p.entitySeq() > MAX_SEQ) {
                unsupported = true;
                return;
            }
            int id = (int) p.entitySeq();
            postings.computeIfAbsent(p.tagId(), k -> new RoaringBitmap()).add(id);
            byType.computeIfAbsent(p.entityType(), k -> new RoaringBitmap()).add(id);
        }
    }

    /** Number of tenants currently loaded (exported as a gauge). */
    public long loadedTenants() {
        return tenants.estimatedSize();
    }
}
