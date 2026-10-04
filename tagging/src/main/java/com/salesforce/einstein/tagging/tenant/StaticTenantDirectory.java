package com.salesforce.einstein.tagging.tenant;

import com.salesforce.einstein.tagging.config.TaggingProperties;
import com.salesforce.einstein.tagging.domain.TenantInfo;
import com.salesforce.einstein.tagging.spi.TenantDirectory;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Places tenants on shards by <b>rendezvous (highest-random-weight) hashing</b>, with explicit pins
 * for whale tenants. Adding a shard moves only ~1/N of tenants, and only those whose new
 * highest-weight shard is the new one.
 *
 * <p>Placement is a pure function of config here. A production directory stores the placement
 * (a {@code tenants} table, Consul or etcd) so tenants can be moved between shards one at a time.
 */
public final class StaticTenantDirectory implements TenantDirectory {
    private final List<String> shards;
    private final Map<String, TaggingProperties.TenantOverride> overrides;
    private final String defaultTier;

    public StaticTenantDirectory(TaggingProperties props) {
        this(List.copyOf(props.shards().keySet()), props.tenants(), props.defaultTier());
    }

    public StaticTenantDirectory(List<String> shards, Map<String, TaggingProperties.TenantOverride> overrides,
                                 String defaultTier) {
        if (shards.isEmpty()) {
            throw new IllegalArgumentException("no shards");
        }
        this.shards = shards.stream().sorted().toList();
        this.overrides = Map.copyOf(overrides);
        this.defaultTier = Objects.requireNonNull(defaultTier, "defaultTier");
    }

    @Override
    public TenantInfo resolve(String tenantId) {
        TenantInfo.requireValidId(tenantId);
        TaggingProperties.TenantOverride o = overrides.get(tenantId);
        String shard = o != null && o.shard() != null ? o.shard() : place(tenantId);
        String tier = o != null && o.tier() != null ? o.tier() : defaultTier;
        return new TenantInfo(tenantId, shard, tier);
    }

    private String place(String tenantId) {
        String best = null;
        long bestWeight = Long.MIN_VALUE;
        for (String shard : shards) {
            long w = fnv1a64(tenantId + '\u0000' + shard);
            if (w > bestWeight) {
                bestWeight = w;
                best = shard;
            }
        }
        return best;
    }

    /** FNV-1a 64 with a final avalanche mix: stable across JVMs and releases (unlike {@code String.hashCode} mixing). */
    static long fnv1a64(String s) {
        long h = 0xcbf29ce484222325L;
        for (byte b : s.getBytes(StandardCharsets.UTF_8)) {
            h ^= b & 0xff;
            h *= 0x100000001b3L;
        }
        h ^= h >>> 33;
        h *= 0xff51afd7ed558ccdL;
        h ^= h >>> 33;
        return h;
    }
}
