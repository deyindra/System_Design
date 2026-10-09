package com.salesforce.einstein.hierarchy.spi;

import com.salesforce.einstein.hierarchy.domain.Crumb;
import com.salesforce.einstein.hierarchy.domain.Node;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;

/**
 * Versioned read cache (DESIGN.md §7.6). Structure-dependent entries (breadcrumb ids, first children page) remember the
 * space's {@code tree_version} they were computed at and are valid only while it is still current, so a move retires
 * them all with one write. Per-node entries (titles) are deleted on change.
 *
 * <p>Every method is best-effort: a cache outage must make reads slower, never fail them.
 */
public interface TreeCache {
    /** Breadcrumb ids of a node, root first, ending with the node. Value of {@code bc:{t}:{node}}. */
    record Breadcrumb(long spaceId, long treeVersion, List<Long> ids) {
        public Breadcrumb {
            ids = List.copyOf(ids);
        }
    }

    /** The first page of a parent's children. Value of {@code ch:{t}:{parent}}. */
    record ChildrenPage(long treeVersion, List<Node> items, boolean hasMore) {
        public ChildrenPage {
            items = List.copyOf(items);
        }
    }

    OptionalLong treeVersion(UUID tenant, long spaceId);

    /** Raises the cached version to at least {@code version}; never lowers it (late writers can't resurrect old keys). */
    void raiseTreeVersion(UUID tenant, long spaceId, long version);

    Optional<Breadcrumb> breadcrumb(UUID tenant, long nodeId);

    void putBreadcrumb(UUID tenant, long nodeId, Breadcrumb value);

    Map<Long, Crumb> crumbs(UUID tenant, Collection<Long> nodeIds);

    void putCrumbs(UUID tenant, Collection<Crumb> crumbs);

    Optional<ChildrenPage> children(UUID tenant, long parentId);

    void putChildren(UUID tenant, long parentId, ChildrenPage page);

    /** A node's title, status or position under its parent changed. */
    void evictNode(UUID tenant, long nodeId, Collection<Long> parentIds);
}
