package com.salesforce.einstein.tagging.tenant;

import com.salesforce.einstein.tagging.domain.TenantInfo;
import com.salesforce.einstein.tagging.spi.TagStore;
import com.salesforce.einstein.tagging.spi.TenantDirectory;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;

/** Resolves a tenant to its shard's {@link TagStore}. Every tenant operation goes through here. */
public final class ShardRouter {
    private final TenantDirectory directory;
    private final Map<String, TagStore> stores;

    public ShardRouter(TenantDirectory directory, Map<String, TagStore> stores) {
        this.directory = Objects.requireNonNull(directory, "directory");
        this.stores = Map.copyOf(stores);
        stores.forEach((id, s) -> {
            if (!id.equals(s.shardId())) {
                throw new IllegalArgumentException("store for shard " + id + " reports shard " + s.shardId());
            }
        });
    }

    public TenantInfo tenant(String tenantId) {
        return directory.resolve(tenantId);
    }

    public TagStore store(TenantInfo tenant) {
        TagStore s = stores.get(tenant.shardId());
        if (s == null) {
            throw new IllegalStateException("tenant " + tenant.tenantId() + " maps to unknown shard " + tenant.shardId());
        }
        return s;
    }

    public TagStore store(String tenantId) {
        return store(tenant(tenantId));
    }

    public Collection<TagStore> all() {
        return stores.values();
    }
}
