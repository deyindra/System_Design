package com.salesforce.einstein.tagging.store;

import com.salesforce.einstein.tagging.domain.Cursors;
import com.salesforce.einstein.tagging.domain.DuplicateTagNameException;
import com.salesforce.einstein.tagging.domain.EntityRef;
import com.salesforce.einstein.tagging.domain.Page;
import com.salesforce.einstein.tagging.domain.StaleVersionException;
import com.salesforce.einstein.tagging.domain.Tag;
import com.salesforce.einstein.tagging.domain.TagEvent;
import com.salesforce.einstein.tagging.domain.TagQuery;
import com.salesforce.einstein.tagging.spi.Capability;
import com.salesforce.einstein.tagging.spi.EntityHit;
import com.salesforce.einstein.tagging.spi.IdempotencyRecord;
import com.salesforce.einstein.tagging.spi.Posting;
import com.salesforce.einstein.tagging.spi.ReadPreference;
import com.salesforce.einstein.tagging.spi.TagReader;
import com.salesforce.einstein.tagging.spi.TagStore;
import com.salesforce.einstein.tagging.spi.UnitOfWork;
import org.springframework.dao.DuplicateKeyException;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Deque;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Single-process {@link TagStore}: the reference implementation of the contract, used for
 * development, tests and single-node deployments.
 *
 * <p>One read-write lock serializes writers, which gives serializable transactions trivially. Rollback
 * replays an undo log. As with a database identity column, sequences aren't rolled back, so a
 * rolled-back append leaves a seq gap; it's permanent at once, since no other transaction can be in flight.
 */
public final class InMemoryTagStore implements TagStore {
    private final String shardId;
    private final Clock clock;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    private final Map<String, TenantData> tenants = new HashMap<>();
    private final TreeMap<Long, OutboxEntry> outbox = new TreeMap<>();
    private long nextOutboxSeq = 1;
    private long nextEntitySeq = 1;
    private long relayedSeq;

    public InMemoryTagStore(String shardId, Clock clock) {
        this.shardId = Objects.requireNonNull(shardId, "shardId");
        this.clock = Objects.requireNonNull(clock, "clock");
    }

    private record OutboxEntry(TagEvent event, Instant createdAt) {
    }

    private static final class TenantData {
        final Map<Long, Tag> tags = new HashMap<>();
        final Map<String, Long> names = new HashMap<>();
        final Map<EntityRef, Long> entities = new HashMap<>();
        final Map<Long, EntityRef> entitiesBySeq = new HashMap<>();
        final Map<EntityRef, TreeSet<Long>> forward = new HashMap<>();
        final Map<Long, TreeMap<Long, EntityRef>> reverse = new HashMap<>();
        final Map<Long, Long> usage = new HashMap<>();
        final Map<String, IdempotencyRecord> idempotency = new HashMap<>();
    }

    @Override
    public String shardId() {
        return shardId;
    }

    @Override
    public Set<Capability> capabilities() {
        return EnumSet.of(Capability.ATOMIC_OUTBOX, Capability.SNAPSHOT_READS);
    }

    @Override
    public <T> T write(String tenantId, Function<UnitOfWork, T> work) {
        lock.writeLock().lock();
        try {
            Uow uow = new Uow(tenantId);
            try {
                return work.apply(uow);
            } catch (RuntimeException | Error e) {
                uow.rollback();
                throw e;
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public <T> T read(String tenantId, ReadPreference pref, Function<TagReader, T> work) {
        lock.readLock().lock();
        try {
            return work.apply(new Reader(tenantId));
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public Optional<Instant> oldestUnrelayed() {
        lock.readLock().lock();
        try {
            Map.Entry<Long, OutboxEntry> next = outbox.higherEntry(relayedSeq);
            return next == null ? Optional.empty() : Optional.of(next.getValue().createdAt());
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public int relay(int maxEvents, Duration gapTimeout, Consumer<List<TagEvent>> sink) {
        lock.writeLock().lock();
        try {
            List<TagEvent> batch = new ArrayList<>();
            long last = relayedSeq;
            for (Map.Entry<Long, OutboxEntry> e : outbox.tailMap(relayedSeq, false).entrySet()) {
                if (batch.size() == maxEvents) {
                    break;
                }
                batch.add(e.getValue().event().sequenced(shardId, e.getKey()));
                last = e.getKey();
            }
            if (batch.isEmpty()) {
                return 0;
            }
            sink.accept(batch);
            relayedSeq = last;
            return batch.size();
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public int pruneOutbox(Instant cutoff) {
        lock.writeLock().lock();
        try {
            int n = 0;
            var it = outbox.headMap(relayedSeq, true).entrySet().iterator();
            while (it.hasNext()) {
                if (it.next().getValue().createdAt().isBefore(cutoff)) {
                    it.remove();
                    n++;
                }
            }
            return n;
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public int pruneIdempotency(Instant cutoff) {
        lock.writeLock().lock();
        try {
            int n = 0;
            for (TenantData t : tenants.values()) {
                int before = t.idempotency.size();
                t.idempotency.values().removeIf(r -> r.createdAt().isBefore(cutoff));
                n += before - t.idempotency.size();
            }
            return n;
        } finally {
            lock.writeLock().unlock();
        }
    }

    // ---------------------------------------------------------------------------------------------

    private class Reader implements TagReader {
        final String tenant;

        Reader(String tenant) {
            this.tenant = Objects.requireNonNull(tenant, "tenant");
        }

        /** Never creates: reads of an unknown tenant see an empty tenant. */
        TenantData data() {
            TenantData d = tenants.get(tenant);
            return d == null ? EMPTY : d;
        }

        @Override
        public Optional<Tag> findTag(long tagId) {
            return Optional.ofNullable(data().tags.get(tagId));
        }

        @Override
        public Optional<Tag> findTagByName(String nameNorm) {
            Long id = data().names.get(nameNorm);
            return id == null ? Optional.empty() : findTag(id).filter(t -> !t.deleted());
        }

        @Override
        public List<Tag> findTags(Collection<Long> tagIds) {
            return new TreeSet<>(tagIds).stream().map(data().tags::get).filter(Objects::nonNull).toList();
        }

        @Override
        public Page<Tag> listTags(String prefixNorm, String afterNameNorm, int limit) {
            String prefix = prefixNorm == null ? "" : prefixNorm;
            List<Tag> rows = data().tags.values().stream()
                    .filter(t -> !t.deleted() && t.nameNorm().startsWith(prefix))
                    .filter(t -> afterNameNorm == null || t.nameNorm().compareTo(afterNameNorm) > 0)
                    .sorted(Comparator.comparing(Tag::nameNorm))
                    .limit(limit + 1L)
                    .toList();
            return Page.fromRows(rows, limit, t -> Cursors.ofName(t.nameNorm()));
        }

        @Override
        public long countLiveTags() {
            return data().tags.values().stream().filter(t -> !t.deleted()).count();
        }

        @Override
        public Map<Long, Long> usage(Collection<Long> tagIds) {
            return lookup(tagIds, data().usage);
        }

        @Override
        public List<Long> tagsOf(EntityRef entity) {
            TreeSet<Long> s = data().forward.get(entity);
            return s == null ? List.of() : List.copyOf(s);
        }

        @Override
        public List<EntityHit> entitiesOf(long tagId, String entityType, long afterSeq, int limit) {
            TreeMap<Long, EntityRef> postings = data().reverse.get(tagId);
            if (postings == null) {
                return List.of();
            }
            List<EntityHit> out = new ArrayList<>();
            for (Map.Entry<Long, EntityRef> e : postings.tailMap(afterSeq, false).entrySet()) {
                if (entityType == null || entityType.equals(e.getValue().type())) {
                    out.add(new EntityHit(e.getKey(), e.getValue()));
                    if (out.size() == limit + 1) {
                        break;
                    }
                }
            }
            return out;
        }

        @Override
        public List<EntityHit> search(TagQuery q) {
            TenantData d = data();
            List<EntityHit> out = new ArrayList<>();
            for (Map.Entry<Long, EntityRef> e : new TreeMap<>(d.entitiesBySeq).tailMap(q.afterSeq(), false).entrySet()) {
                EntityRef ref = e.getValue();
                if (q.entityType() != null && !q.entityType().equals(ref.type())) {
                    continue;
                }
                Set<Long> tags = d.forward.getOrDefault(ref, new TreeSet<>());
                boolean match = tags.containsAll(q.all())
                        && (q.any().isEmpty() || q.any().stream().anyMatch(tags::contains))
                        && q.none().stream().noneMatch(tags::contains)
                        && !tags.isEmpty();
                if (match) {
                    out.add(new EntityHit(e.getKey(), ref));
                    if (out.size() == q.limit() + 1) {
                        break;
                    }
                }
            }
            return out;
        }

        @Override
        public Map<Long, EntityRef> resolveEntities(Collection<Long> entitySeqs) {
            return lookup(entitySeqs, data().entitiesBySeq);
        }

        @Override
        public long latestSeq() {
            for (Map.Entry<Long, OutboxEntry> e : outbox.descendingMap().entrySet()) {
                if (e.getValue().event().tenantId().equals(tenant)) {
                    return e.getKey();
                }
            }
            return 0;
        }

        @Override
        public long relayedSeq() {
            return relayedSeq;
        }

        @Override
        public void scanAssignments(Consumer<Posting> sink) {
            TenantData d = data();
            d.forward.forEach((ref, tagIds) -> {
                long seq = d.entities.get(ref);
                for (Long tagId : tagIds) {
                    Tag t = d.tags.get(tagId);
                    if (t != null && !t.deleted()) {
                        sink.accept(new Posting(tagId, ref.type(), seq));
                    }
                }
            });
        }
    }

    /** The entries of {@code from} whose key is in {@code keys}; missing keys are left out. */
    private static <V> Map<Long, V> lookup(Collection<Long> keys, Map<Long, V> from) {
        Map<Long, V> out = new HashMap<>();
        for (Long k : keys) {
            V v = from.get(k);
            if (v != null) {
                out.put(k, v);
            }
        }
        return out;
    }

    private static final TenantData EMPTY = new TenantData();

    private final class Uow extends Reader implements UnitOfWork {
        private final Deque<Runnable> undo = new ArrayDeque<>();

        Uow(String tenant) {
            super(tenant);
        }

        void rollback() {
            while (!undo.isEmpty()) {
                undo.pop().run();
            }
        }

        TenantData mutable() {
            return tenants.computeIfAbsent(tenant, k -> new TenantData());
        }

        private <K, V> void put(Map<K, V> map, K key, V value) {
            boolean had = map.containsKey(key);
            V old = map.put(key, value);
            undo.push(() -> {
                if (had) {
                    map.put(key, old);
                } else {
                    map.remove(key);
                }
            });
        }

        private <K, V> void remove(Map<K, V> map, K key) {
            if (map.containsKey(key)) {
                V old = map.remove(key);
                undo.push(() -> map.put(key, old));
            }
        }

        @Override
        public Tag insertTag(Tag tag) {
            requireTenant(tag.tenantId());
            TenantData d = mutable();
            if (d.names.containsKey(tag.nameNorm())) {
                throw new DuplicateTagNameException("a tag named '" + tag.name() + "' already exists");
            }
            if (d.tags.containsKey(tag.tagId())) {
                throw new DuplicateKeyException("tag id " + tag.tagId() + " exists");
            }
            put(d.tags, tag.tagId(), tag);
            put(d.names, tag.nameNorm(), tag.tagId());
            return tag;
        }

        @Override
        public Tag updateTag(Tag tag, long expectedVersion) {
            requireTenant(tag.tenantId());
            TenantData d = mutable();
            Tag current = d.tags.get(tag.tagId());
            if (current == null || current.version() != expectedVersion) {
                throw new StaleVersionException("tag " + tag.tagId() + " is not at version " + expectedVersion);
            }
            Long holder = d.names.get(tag.nameNorm());
            if (holder != null && holder != tag.tagId()) {
                throw new DuplicateTagNameException("a tag named '" + tag.name() + "' already exists");
            }
            Tag stored = tag.withVersion(expectedVersion + 1);
            remove(d.names, current.nameNorm());
            put(d.names, stored.nameNorm(), stored.tagId());
            put(d.tags, stored.tagId(), stored);
            return stored;
        }

        @Override
        public long entitySeq(EntityRef e, boolean create) {
            Long seq = data().entities.get(e);
            if (seq != null) {
                return seq;
            }
            if (!create) {
                return 0;
            }
            TenantData d = mutable();
            long s = nextEntitySeq++;
            put(d.entities, e, s);
            put(d.entitiesBySeq, s, e);
            return s;
        }

        @Override
        public Set<Long> attach(EntityRef e, long entitySeq, Collection<Long> tagIds, String actor, Instant at) {
            TenantData d = mutable();
            Set<Long> added = new LinkedHashSet<>();
            for (Long tagId : new TreeSet<>(tagIds)) {
                TreeSet<Long> current = d.forward.get(e);
                if (current != null && current.contains(tagId)) {
                    continue;
                }
                TreeSet<Long> next = current == null ? new TreeSet<>() : new TreeSet<>(current);
                next.add(tagId);
                put(d.forward, e, next);
                TreeMap<Long, EntityRef> postings = d.reverse.get(tagId);
                TreeMap<Long, EntityRef> nextPostings = postings == null ? new TreeMap<>() : new TreeMap<>(postings);
                nextPostings.put(entitySeq, e);
                put(d.reverse, tagId, nextPostings);
                added.add(tagId);
            }
            return added;
        }

        @Override
        public Set<Long> detach(EntityRef e, Collection<Long> tagIds) {
            TenantData d = mutable();
            Long seq = d.entities.get(e);
            Set<Long> removed = new LinkedHashSet<>();
            for (Long tagId : new TreeSet<>(tagIds)) {
                TreeSet<Long> current = d.forward.get(e);
                if (current == null || !current.contains(tagId)) {
                    continue;
                }
                TreeSet<Long> next = new TreeSet<>(current);
                next.remove(tagId);
                if (next.isEmpty()) {
                    remove(d.forward, e);
                } else {
                    put(d.forward, e, next);
                }
                TreeMap<Long, EntityRef> nextPostings = new TreeMap<>(d.reverse.get(tagId));
                nextPostings.remove(seq);
                put(d.reverse, tagId, nextPostings);
                removed.add(tagId);
            }
            return removed;
        }

        @Override
        public void adjustUsage(Map<Long, Long> deltas) {
            TenantData d = mutable();
            deltas.forEach((tagId, delta) -> {
                if (delta != 0) {
                    put(d.usage, tagId, d.usage.getOrDefault(tagId, 0L) + delta);
                }
            });
        }

        @Override
        public int purgeAssignments(long tagId, int limit) {
            TenantData d = mutable();
            TreeMap<Long, EntityRef> postings = d.reverse.get(tagId);
            List<EntityRef> victims = postings == null ? List.of()
                    : postings.values().stream().limit(limit).toList();
            for (EntityRef e : victims) {
                detach(e, List.of(tagId));
            }
            if (victims.size() < limit) {
                remove(d.usage, tagId);
                remove(d.reverse, tagId);
            }
            return victims.size();
        }

        @Override
        public long append(TagEvent event) {
            requireTenant(event.tenantId());
            long seq = nextOutboxSeq++;
            put(outbox, seq, new OutboxEntry(event, clock.instant()));
            return seq;
        }

        @Override
        public Optional<IdempotencyRecord> findIdempotency(String key) {
            return Optional.ofNullable(data().idempotency.get(key));
        }

        @Override
        public void saveIdempotency(IdempotencyRecord record) {
            requireTenant(record.tenantId());
            TenantData d = mutable();
            if (d.idempotency.containsKey(record.key())) {
                throw new DuplicateKeyException("idempotency key exists");
            }
            put(d.idempotency, record.key(), record);
        }

        @Override
        public void completeIdempotency(String key, String response) {
            TenantData d = mutable();
            IdempotencyRecord r = d.idempotency.get(key);
            if (r == null) {
                throw new IllegalStateException("idempotency key " + key + " was not claimed");
            }
            put(d.idempotency, key, new IdempotencyRecord(r.tenantId(), key, r.requestHash(), response, r.createdAt()));
        }

        private void requireTenant(String t) {
            if (!tenant.equals(t)) {
                throw new IllegalArgumentException("unit of work is bound to another tenant");
            }
        }
    }
}
