package com.salesforce.einstein.tagging.service;

import com.salesforce.einstein.tagging.cache.TagCache;
import com.salesforce.einstein.tagging.config.TaggingProperties;
import com.salesforce.einstein.tagging.domain.Consistency;
import com.salesforce.einstein.tagging.domain.ConsistencyToken;
import com.salesforce.einstein.tagging.domain.DuplicateTagNameException;
import com.salesforce.einstein.tagging.domain.EntityRef;
import com.salesforce.einstein.tagging.domain.EntityTags;
import com.salesforce.einstein.tagging.domain.InvalidRequestException;
import com.salesforce.einstein.tagging.domain.LimitExceededException;
import com.salesforce.einstein.tagging.domain.NotFoundException;
import com.salesforce.einstein.tagging.domain.Tag;
import com.salesforce.einstein.tagging.domain.TagEvent;
import com.salesforce.einstein.tagging.domain.TagNames;
import com.salesforce.einstein.tagging.domain.TenantInfo;
import com.salesforce.einstein.tagging.domain.WriteResult;
import com.salesforce.einstein.tagging.spi.IdGenerator;
import com.salesforce.einstein.tagging.spi.ReadPreference;
import com.salesforce.einstein.tagging.spi.TagReader;
import com.salesforce.einstein.tagging.spi.TagStore;
import com.salesforce.einstein.tagging.spi.TenantIsolation;
import com.salesforce.einstein.tagging.spi.TenantIsolation.OpClass;
import com.salesforce.einstein.tagging.spi.UnitOfWork;
import com.salesforce.einstein.tagging.tenant.ShardRouter;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Tags on entities: the hot read path and the idempotent set operations.
 *
 * <p>Every write is one single-shard transaction that locks the entity, changes the assignment rows,
 * adjusts the usage counters and appends the events as its last statements (transactional outbox).
 * Each write returns the seq of its last event as the consistency token.
 */
public final class AssignmentService {
    private final ShardRouter router;
    private final TenantIsolation isolation;
    private final TagCache cache;
    private final IdGenerator ids;
    private final IdempotencyService idempotency;
    private final TaggingProperties.Limits limits;
    private final Clock clock;

    public AssignmentService(ShardRouter router, TenantIsolation isolation, TagCache cache, IdGenerator ids,
                             IdempotencyService idempotency, TaggingProperties.Limits limits, Clock clock) {
        this.router = router;
        this.isolation = isolation;
        this.cache = cache;
        this.ids = ids;
        this.idempotency = idempotency;
        this.limits = limits;
        this.clock = clock;
    }

    /** Outcome of a bulk attach: how many entities were touched and how many assignments were new. */
    public record BulkAttachResult(int entities, int attached) {
    }

    // --- reads -----------------------------------------------------------------------------------

    /**
     * The hot path, served from L1 then L2 then the store. STRONG always goes to the primary. SESSION
     * accepts a cache entry only if it reflects the client's token, and EVENTUAL accepts any entry.
     */
    public EntityTags tagsOf(String tenantId, EntityRef e, Consistency c, ConsistencyToken token) {
        TenantInfo t = router.tenant(tenantId);
        return isolation.execute(t, OpClass.READ, 1, () -> {
            ReadPreference pref = Reads.preference(t, c, token);
            TagStore store = router.store(t);
            if (pref.consistency() != Consistency.STRONG) {
                Optional<TagCache.CachedIds> hit = cache.entityTags(tenantId, e, pref.minSeq());
                if (hit.isPresent()) {
                    List<Long> tagIds = hit.get().tagIds();
                    Map<Long, Tag> meta = new HashMap<>(cache.tags(tenantId, tagIds, pref.minSeq()));
                    List<Long> missing = tagIds.stream().filter(id -> !meta.containsKey(id)).toList();
                    if (!missing.isEmpty()) {
                        meta.putAll(store.read(tenantId, pref, r -> loadTags(tenantId, r, missing)));
                    }
                    return new EntityTags(e, Reads.live(tagIds, meta), Reads.token(t, hit.get().asOfSeq()));
                }
            }
            return store.read(tenantId, pref, r -> {
                long asOf = r.relayedSeq();   // read first: everything at or below it is visible below
                List<Long> tagIds = r.tagsOf(e);
                Map<Long, Tag> meta = byId(r.findTags(tagIds));
                cache.putEntityTags(tenantId, e, tagIds, asOf);
                cache.putTags(tenantId, meta.values(), asOf);
                return new EntityTags(e, Reads.live(tagIds, meta), Reads.token(t, asOf));
            });
        });
    }

    private Map<Long, Tag> loadTags(String tenantId, TagReader r, Collection<Long> tagIds) {
        long asOf = r.relayedSeq();
        List<Tag> tags = r.findTags(tagIds);
        cache.putTags(tenantId, tags, asOf);
        return byId(tags);
    }

    // --- writes ----------------------------------------------------------------------------------

    /**
     * Adds tags (by id and/or by name) to an entity. Names that don't exist yet are created, so a client can
     * tag in one call. Already-present tags are a no-op, which makes retries safe.
     */
    public WriteResult<EntityTags> attach(String tenantId, String actor, EntityRef e, List<Long> tagIds,
                                          List<String> tagNames) {
        List<Long> byIds = tagIds == null ? List.of() : tagIds;
        List<String> byNames = tagNames == null ? List.of() : tagNames;
        if (byIds.isEmpty() && byNames.isEmpty()) {
            throw new InvalidRequestException("tagIds or tagNames is required");
        }
        if (byIds.size() + byNames.size() > limits.maxTagsPerEntity()) {
            throw new LimitExceededException("at most " + limits.maxTagsPerEntity() + " tags per entity");
        }
        TenantInfo t = router.tenant(tenantId);
        TagStore store = router.store(t);
        WriteResult<EntityTags> result = isolation.execute(t, OpClass.WRITE, 1, () -> {
            for (int attempt = 1; ; attempt++) {
                try {
                    return store.write(tenantId, uow -> {
                        Instant now = clock.instant();
                        List<TagEvent> events = new ArrayList<>();
                        Set<Long> wanted = new TreeSet<>(byIds);
                        wanted.addAll(resolveNames(uow, tenantId, actor, byNames, events, now));
                        requireLive(uow, wanted);
                        return change(uow, t, actor, e, wanted, Set.of(), events, now);
                    });
                } catch (DuplicateTagNameException raced) {
                    // A concurrent request created one of our names first. Retrying finds it by name.
                    if (byNames.isEmpty() || attempt == 3) {
                        throw raced;
                    }
                }
            }
        });
        afterWrite(tenantId, e, result.token());
        return result;
    }

    public WriteResult<EntityTags> detach(String tenantId, String actor, EntityRef e, List<Long> tagIds) {
        if (tagIds == null || tagIds.isEmpty()) {
            throw new InvalidRequestException("tagIds is required");
        }
        TenantInfo t = router.tenant(tenantId);
        WriteResult<EntityTags> result = isolation.execute(t, OpClass.WRITE, 1, () -> router.store(t).write(tenantId,
                uow -> change(uow, t, actor, e, Set.of(), new TreeSet<>(tagIds), new ArrayList<>(), clock.instant())));
        afterWrite(tenantId, e, result.token());
        return result;
    }

    /** Sets the entity's tags to exactly {@code tagIds} (an empty list clears them). */
    public WriteResult<EntityTags> replace(String tenantId, String actor, EntityRef e, List<Long> tagIds) {
        Set<Long> desired = new TreeSet<>(tagIds == null ? List.of() : tagIds);
        if (desired.size() > limits.maxTagsPerEntity()) {
            throw new LimitExceededException("at most " + limits.maxTagsPerEntity() + " tags per entity");
        }
        TenantInfo t = router.tenant(tenantId);
        WriteResult<EntityTags> result = isolation.execute(t, OpClass.WRITE, 1, () -> router.store(t).write(tenantId, uow -> {
            requireLive(uow, desired);
            long seq = uow.entitySeq(e, !desired.isEmpty());
            Set<Long> current = seq == 0 ? Set.of() : new TreeSet<>(uow.tagsOf(e));
            Set<Long> toRemove = new TreeSet<>(current);
            toRemove.removeAll(desired);
            return change(uow, t, actor, e, desired, toRemove, new ArrayList<>(), clock.instant());
        }));
        afterWrite(tenantId, e, result.token());
        return result;
    }

    /**
     * Attaches the same tags to many entities in one transaction, with an {@code Idempotency-Key} so a
     * retried import never double-counts. Entities are locked in sorted order, so concurrent bulks can't
     * deadlock.
     */
    public WriteResult<BulkAttachResult> bulkAttach(String tenantId, String actor, String idempotencyKey,
                                                    List<EntityRef> entities, List<Long> tagIds) {
        if (entities == null || entities.isEmpty() || tagIds == null || tagIds.isEmpty()) {
            throw new InvalidRequestException("entities and tagIds are required");
        }
        if (entities.size() > limits.maxBulkEntities() || tagIds.size() > limits.maxBulkTags()) {
            throw new LimitExceededException("bulk attach takes at most " + limits.maxBulkEntities()
                    + " entities and " + limits.maxBulkTags() + " tags");
        }
        TreeMap<String, EntityRef> sorted = new TreeMap<>();
        entities.forEach(x -> sorted.put(x.type() + '\u0000' + x.id(), x));
        Set<Long> tags = new TreeSet<>(tagIds);
        Map<String, Object> canonical = new LinkedHashMap<>();
        canonical.put("entities", List.copyOf(sorted.keySet()));
        canonical.put("tagIds", List.copyOf(tags));

        TenantInfo t = router.tenant(tenantId);
        TagStore store = router.store(t);
        WriteResult<BulkAttachResult> result = isolation.execute(t, OpClass.BULK, sorted.size(), () ->
                idempotency.run(store, tenantId, idempotencyKey, canonical, BulkAttachResult.class, uow -> {
                    requireLive(uow, tags);
                    Instant now = clock.instant();
                    Map<Long, Long> usage = new HashMap<>();
                    List<TagEvent> events = new ArrayList<>();
                    for (EntityRef e : sorted.values()) {
                        long seq = uow.entitySeq(e, true);
                        Set<Long> added = uow.attach(e, seq, tags, actor, now);
                        if (!added.isEmpty()) {
                            int live = liveCount(uow, uow.tagsOf(e));
                            if (live > limits.maxTagsPerEntity()) {
                                throw new LimitExceededException(e + " would exceed " + limits.maxTagsPerEntity() + " tags");
                            }
                            added.forEach(id -> usage.merge(id, 1L, Long::sum));
                            events.add(TagEvent.assignment(TagEvent.Type.TAGS_ATTACHED, tenantId, e, seq, List.copyOf(added), actor, now));
                        }
                    }
                    uow.adjustUsage(usage);   // one call, tags in order: no counter-row deadlocks
                    long last = appendAll(uow, events);
                    int attached = usage.values().stream().mapToInt(Long::intValue).sum();
                    return new WriteResult<>(new BulkAttachResult(sorted.size(), attached), Reads.token(t, last));
                }));
        if (!result.token().isNone()) {
            sorted.values().forEach(e -> cache.invalidateEntity(tenantId, e, result.token().seq()));
        }
        return result;
    }

    /**
     * The core of every single-entity write: lock the entity, remove {@code toRemove}, add {@code toAdd},
     * enforce the per-entity limit, adjust counters and append events last.
     */
    private WriteResult<EntityTags> change(UnitOfWork uow, TenantInfo t, String actor, EntityRef e,
                                           Set<Long> toAdd, Set<Long> toRemove, List<TagEvent> events, Instant now) {
        String tenantId = t.tenantId();
        long seq = uow.entitySeq(e, !toAdd.isEmpty());
        if (seq == 0) {   // detach from an entity that was never tagged
            return new WriteResult<>(new EntityTags(e, List.of(), ConsistencyToken.NONE), ConsistencyToken.NONE);
        }
        Set<Long> removed = toRemove.isEmpty() ? Set.of() : uow.detach(e, toRemove);
        Set<Long> after = new TreeSet<>(uow.tagsOf(e));
        after.addAll(toAdd);
        Map<Long, Tag> meta = byId(uow.findTags(after));
        List<Tag> live = Reads.live(after, meta);
        if (!toAdd.isEmpty() && live.size() > limits.maxTagsPerEntity()) {
            throw new LimitExceededException("at most " + limits.maxTagsPerEntity() + " tags per entity");
        }
        Set<Long> added = toAdd.isEmpty() ? Set.of() : uow.attach(e, seq, toAdd, actor, now);

        Map<Long, Long> usage = new HashMap<>();
        removed.forEach(id -> usage.merge(id, -1L, Long::sum));
        added.forEach(id -> usage.merge(id, 1L, Long::sum));
        uow.adjustUsage(usage);
        if (!removed.isEmpty()) {
            events.add(TagEvent.assignment(TagEvent.Type.TAGS_DETACHED, tenantId, e, seq, List.copyOf(removed), actor, now));
        }
        if (!added.isEmpty()) {
            events.add(TagEvent.assignment(TagEvent.Type.TAGS_ATTACHED, tenantId, e, seq, List.copyOf(added), actor, now));
        }
        ConsistencyToken token = Reads.token(t, appendAll(uow, events));
        return new WriteResult<>(new EntityTags(e, live, token), token);
    }

    /** Finds tags by name, creating the missing ones (their TAG_CREATED events are queued in {@code events}). */
    private List<Long> resolveNames(UnitOfWork uow, String tenantId, String actor, List<String> names,
                                    List<TagEvent> events, Instant now) {
        List<Long> out = new ArrayList<>();
        Map<String, String> byNorm = new TreeMap<>();   // sorted: unique-index locks in a fixed order
        names.forEach(n -> byNorm.putIfAbsent(TagNames.normalize(n), n));
        for (Map.Entry<String, String> n : byNorm.entrySet()) {
            Optional<Tag> found = uow.findTagByName(n.getKey());
            if (found.isPresent()) {
                out.add(found.get().tagId());
            } else {
                Tag created = TagService.insert(uow, tenantId, actor, n.getValue(), null, limits, ids, clock);
                events.add(TagEvent.tag(TagEvent.Type.TAG_CREATED, tenantId, created.tagId(), actor, now));
                out.add(created.tagId());
            }
        }
        return out;
    }

    private static void requireLive(UnitOfWork uow, Set<Long> tagIds) {
        if (tagIds.isEmpty()) {
            return;
        }
        Set<Long> live = uow.findTags(tagIds).stream().filter(x -> !x.deleted()).map(Tag::tagId)
                .collect(Collectors.toSet());
        if (live.size() != tagIds.size()) {
            Set<Long> missing = new TreeSet<>(tagIds);
            missing.removeAll(live);
            throw new NotFoundException("unknown or deleted tags: " + missing);
        }
    }

    private static int liveCount(UnitOfWork uow, Collection<Long> tagIds) {
        return (int) uow.findTags(tagIds).stream().filter(x -> !x.deleted()).count();
    }

    /** Appends the queued events, in order, as the transaction's last statements. Returns the last seq or 0. */
    private static long appendAll(UnitOfWork uow, List<TagEvent> events) {
        long last = 0;
        for (TagEvent ev : events) {
            last = uow.append(ev);
        }
        return last;
    }

    /** Read-your-writes on this pod at once. Other pods invalidate when the event reaches them. */
    private void afterWrite(String tenantId, EntityRef e, ConsistencyToken token) {
        if (!token.isNone()) {
            cache.invalidateEntity(tenantId, e, token.seq());
        }
    }

    private static Map<Long, Tag> byId(Collection<Tag> tags) {
        return tags.stream().collect(Collectors.toMap(Tag::tagId, Function.identity()));
    }
}
