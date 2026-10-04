package com.salesforce.einstein.tagging.service;

import com.salesforce.einstein.tagging.cache.TagCache;
import com.salesforce.einstein.tagging.config.TaggingProperties;
import com.salesforce.einstein.tagging.domain.Consistency;
import com.salesforce.einstein.tagging.domain.ConsistencyToken;
import com.salesforce.einstein.tagging.domain.Cursors;
import com.salesforce.einstein.tagging.domain.InvalidRequestException;
import com.salesforce.einstein.tagging.domain.LimitExceededException;
import com.salesforce.einstein.tagging.domain.NotFoundException;
import com.salesforce.einstein.tagging.domain.Page;
import com.salesforce.einstein.tagging.domain.StaleVersionException;
import com.salesforce.einstein.tagging.domain.Tag;
import com.salesforce.einstein.tagging.domain.TagEvent;
import com.salesforce.einstein.tagging.domain.TagNames;
import com.salesforce.einstein.tagging.domain.TenantInfo;
import com.salesforce.einstein.tagging.domain.WriteResult;
import com.salesforce.einstein.tagging.spi.IdGenerator;
import com.salesforce.einstein.tagging.spi.TagStore;
import com.salesforce.einstein.tagging.spi.TenantIsolation;
import com.salesforce.einstein.tagging.spi.TenantIsolation.OpClass;
import com.salesforce.einstein.tagging.spi.UnitOfWork;
import com.salesforce.einstein.tagging.tenant.ShardRouter;
import org.springframework.dao.DuplicateKeyException;

import java.time.Clock;
import java.util.List;
import java.util.regex.Pattern;

/** Tag metadata: create, read, list (autocomplete), rename/recolor (optimistic), soft delete. */
public final class TagService {
    private static final Pattern COLOR = Pattern.compile("#[0-9a-fA-F]{6}|[a-z]{3,20}");

    private final ShardRouter router;
    private final TenantIsolation isolation;
    private final IdGenerator ids;
    private final TagCache cache;
    private final TaggingProperties.Limits limits;
    private final Clock clock;

    public TagService(ShardRouter router, TenantIsolation isolation, IdGenerator ids, TagCache cache,
                      TaggingProperties.Limits limits, Clock clock) {
        this.router = router;
        this.isolation = isolation;
        this.ids = ids;
        this.cache = cache;
        this.limits = limits;
        this.clock = clock;
    }

    /** A tag plus its usage count. */
    public record TagDetails(Tag tag, long usage) {
    }

    public WriteResult<Tag> create(String tenantId, String actor, String name, String color) {
        validateColor(color);
        TenantInfo t = router.tenant(tenantId);
        TagStore store = router.store(t);
        return isolation.execute(t, OpClass.WRITE, 1, () -> {
            for (int attempt = 1; ; attempt++) {
                try {
                    return store.write(tenantId, uow -> {
                        Tag tag = insert(uow, tenantId, actor, name, color, limits, ids, clock);
                        long seq = uow.append(TagEvent.tag(TagEvent.Type.TAG_CREATED, tenantId, tag.tagId(), actor, clock.instant()));
                        return new WriteResult<>(tag, Reads.token(t, seq));
                    });
                } catch (DuplicateKeyException idCollision) {   // name clashes are DuplicateTagNameException
                    if (attempt == 3) {
                        throw idCollision;
                    }
                }
            }
        });
    }

    /** Inserts a new tag inside {@code uow}, enforcing the per-tenant tag limit. */
    static Tag insert(UnitOfWork uow, String tenantId, String actor, String name, String color,
                      TaggingProperties.Limits limits, IdGenerator ids, Clock clock) {
        if (uow.countLiveTags() >= limits.maxTagsPerTenant()) {
            throw new LimitExceededException("tenant has reached " + limits.maxTagsPerTenant() + " tags");
        }
        return uow.insertTag(Tag.create(tenantId, ids.nextId(), name, color, actor, clock.instant()));
    }

    public TagDetails get(String tenantId, long tagId, Consistency c, ConsistencyToken token) {
        TenantInfo t = router.tenant(tenantId);
        return isolation.execute(t, OpClass.READ, 1, () -> router.store(t).read(tenantId, Reads.preference(t, c, token), r -> {
            Tag tag = r.findTag(tagId).filter(x -> !x.deleted())
                    .orElseThrow(() -> new NotFoundException("tag " + tagId + " not found"));
            return new TagDetails(tag, r.usage(List.of(tagId)).getOrDefault(tagId, 0L));
        }));
    }

    /** Autocomplete: live tags whose name starts with {@code prefix}, in name order. Eventual by default. */
    public Page<Tag> list(String tenantId, String prefix, String cursor, Integer limit, Consistency c,
                          ConsistencyToken token) {
        TenantInfo t = router.tenant(tenantId);
        int size = Reads.pageSize(limit, limits);
        String prefixNorm = TagNames.normalizePrefix(prefix);
        String after = Cursors.name(cursor);
        return isolation.execute(t, OpClass.READ, 1, () -> router.store(t)
                .read(tenantId, Reads.preference(t, c, token), r -> r.listTags(prefixNorm, after, size)));
    }

    /** Rename and/or recolor. {@code expectedVersion} comes from {@code If-Match}. */
    public WriteResult<Tag> update(String tenantId, String actor, long tagId, long expectedVersion,
                                   String newName, String newColor) {
        if (newName == null && newColor == null) {
            throw new InvalidRequestException("nothing to update");
        }
        validateColor(newColor);
        TenantInfo t = router.tenant(tenantId);
        WriteResult<Tag> result = isolation.execute(t, OpClass.WRITE, 1, () -> router.store(t).write(tenantId, uow -> {
            Tag current = live(uow, tagId);
            if (current.version() != expectedVersion) {
                throw new StaleVersionException("tag " + tagId + " is at version " + current.version());
            }
            Tag next = current;
            if (newName != null) {
                next = next.renamed(newName, clock.instant());
            }
            if (newColor != null) {
                next = next.recolored(newColor, clock.instant());
            }
            Tag stored = uow.updateTag(next, expectedVersion);
            long seq = uow.append(TagEvent.tag(TagEvent.Type.TAG_UPDATED, tenantId, tagId, actor, clock.instant()));
            return new WriteResult<>(stored, Reads.token(t, seq));
        }));
        cache.invalidateTag(tenantId, tagId, result.token().seq());
        return result;
    }

    /**
     * Soft delete. The tag disappears from every read at once (reads filter deleted tags). Its name is
     * free immediately, and its assignments are purged in the background ({@link TagPurgeJob}).
     * Deleting an already-deleted tag is a no-op.
     *
     * @param expectedVersion from {@code If-Match}, or null for an unconditional delete
     */
    public WriteResult<Void> delete(String tenantId, String actor, long tagId, Long expectedVersion) {
        TenantInfo t = router.tenant(tenantId);
        WriteResult<Void> result = isolation.execute(t, OpClass.WRITE, 1, () -> router.store(t).write(tenantId, uow -> {
            Tag current = uow.findTag(tagId).orElseThrow(() -> new NotFoundException("tag " + tagId + " not found"));
            if (current.deleted()) {
                return new WriteResult<>(null, ConsistencyToken.NONE);
            }
            if (expectedVersion != null && current.version() != expectedVersion) {
                throw new StaleVersionException("tag " + tagId + " is at version " + current.version());
            }
            uow.updateTag(current.tombstoned(clock.instant()), current.version());
            long seq = uow.append(TagEvent.tag(TagEvent.Type.TAG_DELETED, tenantId, tagId, actor, clock.instant()));
            return new WriteResult<>(null, Reads.token(t, seq));
        }));
        if (!result.token().isNone()) {
            cache.invalidateTag(tenantId, tagId, result.token().seq());
        }
        return result;
    }

    private static Tag live(UnitOfWork uow, long tagId) {
        return uow.findTag(tagId).filter(x -> !x.deleted())
                .orElseThrow(() -> new NotFoundException("tag " + tagId + " not found"));
    }

    private static void validateColor(String color) {
        if (color != null && !COLOR.matcher(color).matches()) {
            throw new InvalidRequestException("color must be #rrggbb or a lower-case color name");
        }
    }
}
