package com.salesforce.einstein.tagging.spi;

import com.salesforce.einstein.tagging.domain.TenantInfo;

/**
 * Port that maps a tenant to its shard and quota tier. Built in: {@code StaticTenantDirectory}
 * (config plus consistent hashing). Production adapters read a replicated directory table, Consul
 * or etcd, and cache it.
 */
public interface TenantDirectory {
    TenantInfo resolve(String tenantId);
}
