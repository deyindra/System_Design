package com.salesforce.einstein.tagging.service;

import com.salesforce.einstein.tagging.config.TaggingProperties;
import com.salesforce.einstein.tagging.domain.Consistency;
import com.salesforce.einstein.tagging.domain.ConsistencyToken;
import com.salesforce.einstein.tagging.domain.Cursors;
import com.salesforce.einstein.tagging.domain.EntityRef;
import com.salesforce.einstein.tagging.domain.InvalidRequestException;
import com.salesforce.einstein.tagging.domain.NotFoundException;
import com.salesforce.einstein.tagging.domain.SearchResult;
import com.salesforce.einstein.tagging.domain.Tag;
import com.salesforce.einstein.tagging.domain.TagQuery;
import com.salesforce.einstein.tagging.domain.TenantInfo;
import com.salesforce.einstein.tagging.spi.EntityHit;
import com.salesforce.einstein.tagging.spi.ReadPreference;
import com.salesforce.einstein.tagging.spi.SearchPostings;
import com.salesforce.einstein.tagging.spi.TagReader;
import com.salesforce.einstein.tagging.spi.TagSearchIndex;
import com.salesforce.einstein.tagging.spi.TagStore;
import com.salesforce.einstein.tagging.spi.TenantIsolation;
import com.salesforce.einstein.tagging.spi.TenantIsolation.OpClass;
import com.salesforce.einstein.tagging.tenant.ShardRouter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.lang.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;

/**
 * Boolean tag search. The inverted index answers when it can, at the consistency the client asked for,
 * and the store's SQL path is the fallback.
 *
 * <p>STRONG uses the index only behind a read barrier: the tenant's latest committed outbox seq, read on the
 * primary after the request arrived. Every write acknowledged before the search has a seq at or below it, and
 * the index applies a tenant's events in seq order, so a watermark at the barrier means the index holds them all.
 *
 * <p>Deleted or unknown tags are resolved before the query. An {@code all} term naming one means no
 * entity can match. In {@code any} or {@code none} such a term is dropped, since it matches nothing.
 */
public final class TagSearchService {
    private final ShardRouter router;
    private final TenantIsolation isolation;
    @Nullable
    private final TagSearchIndex index;   // null when no index is configured: the store serves every search
    private final TaggingProperties props;
    private final MeterRegistry metrics;

    public TagSearchService(ShardRouter router, TenantIsolation isolation, @Nullable TagSearchIndex index,
                            TaggingProperties props, MeterRegistry metrics) {
        this.router = router;
        this.isolation = isolation;
        this.index = index;
        this.props = props;
        this.metrics = metrics;
    }

    public SearchResult search(String tenantId, Collection<Long> all, Collection<Long> any, Collection<Long> none,
                               String entityType, String cursor, Integer limit, Consistency c, ConsistencyToken token) {
        Set<Long> allSet = set(all);
        Set<Long> anySet = set(any);
        Set<Long> noneSet = set(none);
        int size = Reads.pageSize(limit, props.limits());
        long after = Cursors.seq(cursor);
        TagQuery query = new TagQuery(allSet, anySet, noneSet, entityType, after, size);   // validate before any I/O
        if (query.termCount() > props.limits().maxSearchTerms()) {
            throw new InvalidRequestException("at most " + props.limits().maxSearchTerms() + " tags per search");
        }

        TenantInfo t = router.tenant(tenantId);
        return isolation.execute(t, OpClass.SEARCH, 1, () -> {
            ReadPreference pref = Reads.preference(t, c, token);
            TagStore store = router.store(t);
            Set<Long> terms = new HashSet<>(allSet);
            terms.addAll(anySet);
            terms.addAll(noneSet);
            Set<Long> live = store.read(tenantId, pref, r -> r.findTags(terms)).stream()
                    .filter(x -> !x.deleted()).map(Tag::tagId).collect(Collectors.toSet());
            if (!live.containsAll(allSet)) {
                return empty();
            }
            Set<Long> liveAny = intersect(anySet, live);
            if (!anySet.isEmpty() && liveAny.isEmpty()) {
                return empty();
            }
            TagQuery q = new TagQuery(allSet, liveAny, intersect(noneSet, live), entityType, after, size);

            if (index != null) {
                long barrier = pref.consistency() == Consistency.STRONG
                        ? store.read(tenantId, pref, TagReader::latestSeq) : pref.minSeq();
                Optional<SearchResult> fromIndex = searchIndex(index, t, store, q, pref.consistency(), barrier);
                if (fromIndex.isPresent()) {
                    metrics.counter("tagging.search", "served_by", "index").increment();
                    return fromIndex.get();
                }
            }
            metrics.counter("tagging.search", "served_by", "store").increment();
            List<EntityHit> hits = store.read(tenantId, pref, r -> r.search(q));
            return page(hits, size);
        });
    }

    /** Searches the index if its watermark reaches {@code barrier} (waiting briefly); empty means "ask the store". */
    private Optional<SearchResult> searchIndex(TagSearchIndex idx, TenantInfo t, TagStore store, TagQuery q,
                                               Consistency level, long barrier) {
        String tenantId = t.tenantId();
        long wm = idx.watermark(tenantId);
        if (wm < 0) {
            idx.search(tenantId, q);   // starts loading the tenant; the store answers meanwhile
            wm = idx.watermark(tenantId);
            if (wm < 0) {
                return Optional.empty();
            }
        }
        if (level != Consistency.EVENTUAL && wm < barrier) {
            wm = awaitWatermark(idx, tenantId, barrier, props.consistency().indexWait());
            if (wm < barrier) {
                return Optional.empty();   // still behind an acknowledged write: the store answers
            }
        }
        Optional<SearchPostings> found = idx.search(tenantId, q);
        if (found.isEmpty()) {
            return Optional.empty();
        }
        SearchPostings postings = found.get();
        List<Long> seqs = postings.entitySeqs();
        // The index has applied events up to its watermark, so a replica that's at least that far along
        // has every entity row it can return (entity rows never change once created).
        long applied = Math.max(idx.watermark(tenantId), 0);
        ReadPreference rows = level == Consistency.STRONG ? ReadPreference.strong() : ReadPreference.session(applied);
        Map<Long, EntityRef> refs = store.read(tenantId, rows, r -> r.resolveEntities(seqs));
        List<EntityRef> items = new ArrayList<>();
        for (Long seq : seqs) {
            EntityRef ref = refs.get(seq);
            if (ref != null) {
                items.add(ref);
            }
        }
        String next = postings.hasMore() && !seqs.isEmpty() ? Cursors.ofSeq(seqs.get(seqs.size() - 1)) : null;
        return Optional.of(new SearchResult(items, next, postings.total(), "index"));
    }

    private static long awaitWatermark(TagSearchIndex idx, String tenantId, long minSeq, Duration maxWait) {
        long deadline = System.nanoTime() + maxWait.toNanos();
        long wm = idx.watermark(tenantId);
        while (wm < minSeq && wm >= 0 && System.nanoTime() < deadline) {
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(2));
            wm = idx.watermark(tenantId);
        }
        return wm;
    }

    /** Reverse lookup: entities carrying one tag, by keyset pagination on entitySeq. */
    public SearchResult entitiesOf(String tenantId, long tagId, String entityType, String cursor, Integer limit,
                                   Consistency c, ConsistencyToken token) {
        EntityRef.checkTypeFilter(entityType);
        int size = Reads.pageSize(limit, props.limits());
        long after = Cursors.seq(cursor);
        TenantInfo t = router.tenant(tenantId);
        return isolation.execute(t, OpClass.READ, 1, () -> router.store(t).read(tenantId, Reads.preference(t, c, token), r -> {
            r.findTag(tagId).filter(x -> !x.deleted())
                    .orElseThrow(() -> new NotFoundException("tag " + tagId + " not found"));
            return page(r.entitiesOf(tagId, entityType, after, size), size);
        }));
    }

    /** Turns up to {@code size + 1} store hits into a page with a cursor. */
    private static SearchResult page(List<EntityHit> hits, int size) {
        boolean more = hits.size() > size;
        List<EntityHit> page = more ? hits.subList(0, size) : hits;
        String next = more ? Cursors.ofSeq(page.get(page.size() - 1).entitySeq()) : null;
        return new SearchResult(page.stream().map(EntityHit::entity).toList(), next, SearchResult.TOTAL_UNKNOWN, "store");
    }

    /** No entity can match (a required tag is deleted or unknown); decided by the store. */
    private static SearchResult empty() {
        return new SearchResult(List.of(), null, 0, "store");
    }

    private static Set<Long> set(Collection<Long> c) {
        return c == null ? Set.of() : Set.copyOf(c);
    }

    private static Set<Long> intersect(Set<Long> a, Set<Long> b) {
        return a.stream().filter(b::contains).collect(Collectors.toSet());
    }
}
