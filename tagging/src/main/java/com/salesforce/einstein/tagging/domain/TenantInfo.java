package com.salesforce.einstein.tagging.domain;

import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Where a tenant lives and which quota tier applies. Resolved once per request by the
 * {@code TenantDirectory}. A tenant's rows all live on one shard, so every write is a single-shard
 * transaction.
 */
public record TenantInfo(String tenantId, String shardId, String tier) {
    private static final Pattern ID = Pattern.compile("[A-Za-z0-9-]{1,64}");

    public TenantInfo {
        Objects.requireNonNull(shardId, "shardId");
        Objects.requireNonNull(tier, "tier");
        requireValidId(tenantId);
    }

    public static String requireValidId(String tenantId) {
        if (tenantId == null || !ID.matcher(tenantId).matches()) {
            throw new InvalidRequestException("tenant id must match " + ID.pattern());
        }
        return tenantId;
    }
}
