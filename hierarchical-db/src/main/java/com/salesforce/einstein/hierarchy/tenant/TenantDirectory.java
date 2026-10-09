package com.salesforce.einstein.hierarchy.tenant;

import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Tenant to shard. Explicit placements win (a big tenant on its own shard, or one being migrated); every other tenant
 * is placed by rendezvous hashing over the shard names, so adding a shard moves only about 1/n of them.
 */
public final class TenantDirectory {
    private final List<String> shards;
    private final Map<UUID, String> placements;

    public TenantDirectory(List<String> shards, Map<UUID, String> placements) {
        if (shards.isEmpty()) {
            throw new IllegalArgumentException("at least one shard is required");
        }
        placements.forEach((tenant, shard) -> {
            if (!shards.contains(shard)) {
                throw new IllegalArgumentException("tenant " + tenant + " is placed on unknown shard " + shard);
            }
        });
        this.shards = List.copyOf(shards);
        this.placements = Map.copyOf(placements);
    }

    public String shardOf(UUID tenant) {
        String placed = placements.get(tenant);
        if (placed != null) {
            return placed;
        }
        String best = shards.get(0);
        long bestScore = Long.MIN_VALUE;
        for (String shard : shards) {
            long score = mix(tenant.getMostSignificantBits() ^ mix(tenant.getLeastSignificantBits() ^ shard.hashCode()));
            if (score > bestScore) {
                bestScore = score;
                best = shard;
            }
        }
        return best;
    }

    /** SplitMix64 finalizer: a cheap, well-distributed 64-bit mix. */
    private static long mix(long z) {
        z = (z ^ (z >>> 30)) * 0xbf58476d1ce4e5b9L;
        z = (z ^ (z >>> 27)) * 0x94d049bb133111ebL;
        return z ^ (z >>> 31);
    }
}
