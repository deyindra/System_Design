package com.salesforce.einstein.tagging.cache;

import com.salesforce.einstein.tagging.domain.EntityRef;
import com.salesforce.einstein.tagging.spi.DistributedCache;

import java.util.Optional;

/** The default L2: none. Each pod's L1 still absorbs the hot read path. */
public final class NoopDistributedCache implements DistributedCache {
    @Override
    public Optional<Entry> get(String tenantId, EntityRef entity) {
        return Optional.empty();
    }

    @Override
    public void putIfNewer(String tenantId, EntityRef entity, Entry entry) {
    }

    @Override
    public void invalidate(String tenantId, EntityRef entity, long seq) {
    }
}
