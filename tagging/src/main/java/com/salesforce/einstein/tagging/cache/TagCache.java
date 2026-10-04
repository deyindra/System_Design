package com.salesforce.einstein.tagging.cache;

import com.salesforce.einstein.tagging.config.TaggingProperties;
import com.salesforce.einstein.tagging.domain.EntityRef;
import com.salesforce.einstein.tagging.domain.Tag;
import com.salesforce.einstein.tagging.domain.TagEvent;
import com.salesforce.einstein.tagging.spi.DistributedCache;
import com.salesforce.einstein.tagging.spi.TagEventListener;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Two read-through caches for the hot path "tags of an entity". The first maps an entity to its tag
 * ids (L1 Caffeine, then the {@link DistributedCache} L2). The second maps a tag id to its metadata
 * (L1 only, small and very hot).
 *
 * <p>Ids and metadata are cached separately, so a rename invalidates one tag entry, not every entity
 * carrying the tag. Entries are versioned (see {@link VersionedCache}) and invalidated by events: on
 * every pod through the event stream, and at once on the writing pod for read-your-writes.
 */
public final class TagCache implements TagEventListener {
    record EntityKey(String tenantId, EntityRef entity) {
    }

    record TagKey(String tenantId, long tagId) {
    }

    /** Tag ids of an entity, valid as of {@code asOfSeq}. */
    public record CachedIds(List<Long> tagIds, long asOfSeq) {
    }

    private final VersionedCache<EntityKey, List<Long>> entities;
    private final VersionedCache<TagKey, Tag> tags;
    private final DistributedCache l2;

    public TagCache(TaggingProperties.Cache cfg, DistributedCache l2) {
        this.entities = new VersionedCache<>(cfg.entityMaxSize(), cfg.ttl(), cfg.tombstoneTtl());
        this.tags = new VersionedCache<>(cfg.tagMaxSize(), cfg.ttl(), cfg.tombstoneTtl());
        this.l2 = l2;
    }

    public Optional<CachedIds> entityTags(String tenantId, EntityRef e, long minSeq) {
        EntityKey key = new EntityKey(tenantId, e);
        var l1 = entities.get(key, minSeq);
        if (l1.isPresent()) {
            return Optional.of(new CachedIds(l1.get().value(), l1.get().asOfSeq()));
        }
        Optional<DistributedCache.Entry> remote = l2.get(tenantId, e).filter(x -> x.asOfSeq() >= minSeq);
        remote.ifPresent(x -> entities.putIfNewer(key, x.tagIds(), x.asOfSeq()));
        return remote.map(x -> new CachedIds(x.tagIds(), x.asOfSeq()));
    }

    public void putEntityTags(String tenantId, EntityRef e, List<Long> tagIds, long asOfSeq) {
        entities.putIfNewer(new EntityKey(tenantId, e), List.copyOf(tagIds), asOfSeq);
        l2.putIfNewer(tenantId, e, new DistributedCache.Entry(tagIds, asOfSeq));
    }

    /** Cached metadata of {@code tagIds} at least as new as {@code minSeq}. Misses are simply absent. */
    public Map<Long, Tag> tags(String tenantId, Collection<Long> tagIds, long minSeq) {
        Map<Long, Tag> out = new HashMap<>();
        for (Long id : tagIds) {
            tags.get(new TagKey(tenantId, id), minSeq).ifPresent(v -> out.put(id, v.value()));
        }
        return out;
    }

    public void putTags(String tenantId, Collection<Tag> loaded, long asOfSeq) {
        for (Tag t : loaded) {
            tags.putIfNewer(new TagKey(tenantId, t.tagId()), t, asOfSeq);
        }
    }

    public void invalidateEntity(String tenantId, EntityRef e, long seq) {
        entities.invalidate(new EntityKey(tenantId, e), seq);
        l2.invalidate(tenantId, e, seq);
    }

    public void invalidateTag(String tenantId, long tagId, long seq) {
        tags.invalidate(new TagKey(tenantId, tagId), seq);
    }

    @Override
    public void onEvents(List<TagEvent> events) {
        for (TagEvent e : events) {
            switch (e.type()) {
                case TAGS_ATTACHED, TAGS_DETACHED -> invalidateEntity(e.tenantId(), e.entity(), e.seq());
                case TAG_CREATED, TAG_UPDATED, TAG_DELETED -> invalidateTag(e.tenantId(), e.tagId(), e.seq());
            }
        }
    }
}
