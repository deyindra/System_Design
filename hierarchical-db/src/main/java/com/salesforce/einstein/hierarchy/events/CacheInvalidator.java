package com.salesforce.einstein.hierarchy.events;

import com.salesforce.einstein.hierarchy.domain.TreeEvent;
import com.salesforce.einstein.hierarchy.spi.TreeCache;
import com.salesforce.einstein.hierarchy.spi.TreeEventListener;

import java.util.ArrayList;
import java.util.List;

/**
 * Re-applies each write's cache invalidation from the relayed event. The writer already invalidated after commit; this
 * covers a writer that crashed in between, or whose cache call failed. Both are idempotent: raising a version is
 * monotonic and deleting twice is harmless.
 */
public final class CacheInvalidator implements TreeEventListener {
    private final TreeCache cache;

    public CacheInvalidator(TreeCache cache) {
        this.cache = cache;
    }

    @Override
    public void onEvents(List<TreeEvent> events) {
        for (TreeEvent e : events) {
            switch (e.type()) {
                case NODE_MOVED, MOVE_STARTED, MOVE_COMPLETED -> {
                    cache.raiseTreeVersion(e.tenantId(), e.spaceId(), e.treeVersion());
                    if (e.oldSpaceId() != null) {
                        cache.raiseTreeVersion(e.tenantId(), e.oldSpaceId(), e.oldTreeVersion());
                    }
                    cache.evictNode(e.tenantId(), e.nodeId(), parents(e));
                }
                case NODE_CREATED, NODE_UPDATED, NODE_REORDERED, NODE_TRASHED, NODE_RESTORED, NODE_PURGED ->
                        cache.evictNode(e.tenantId(), e.nodeId(), parents(e));
                case SPACE_CREATED, RESTRICTIONS_CHANGED -> {
                    // Nothing cached depends on them: restrictions are always checked against the database.
                }
            }
        }
    }

    private static List<Long> parents(TreeEvent e) {
        List<Long> out = new ArrayList<>(2);
        if (e.parentId() != null) {
            out.add(e.parentId());
        }
        if (e.oldParentId() != null && !e.oldParentId().equals(e.parentId())) {
            out.add(e.oldParentId());
        }
        return out;
    }
}
