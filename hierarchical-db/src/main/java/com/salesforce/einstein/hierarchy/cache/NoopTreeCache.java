package com.salesforce.einstein.hierarchy.cache;

import com.salesforce.einstein.hierarchy.domain.Crumb;
import com.salesforce.einstein.hierarchy.spi.TreeCache;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;

/** {@code hierarchy.cache.type=none}: every read goes to the database. */
public final class NoopTreeCache implements TreeCache {
    @Override
    public OptionalLong treeVersion(UUID tenant, long spaceId) {
        return OptionalLong.empty();
    }

    @Override
    public void raiseTreeVersion(UUID tenant, long spaceId, long version) {
    }

    @Override
    public Optional<Breadcrumb> breadcrumb(UUID tenant, long nodeId) {
        return Optional.empty();
    }

    @Override
    public void putBreadcrumb(UUID tenant, long nodeId, Breadcrumb value) {
    }

    @Override
    public Map<Long, Crumb> crumbs(UUID tenant, Collection<Long> nodeIds) {
        return Map.of();
    }

    @Override
    public void putCrumbs(UUID tenant, Collection<Crumb> crumbs) {
    }

    @Override
    public Optional<ChildrenPage> children(UUID tenant, long parentId) {
        return Optional.empty();
    }

    @Override
    public void putChildren(UUID tenant, long parentId, ChildrenPage page) {
    }

    @Override
    public void evictNode(UUID tenant, long nodeId, Collection<Long> parentIds) {
    }
}
