package com.salesforce.einstein.hierarchy.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

/** Every tunable of the service, bound from {@code hierarchy.*}. Defaults suit a local run against one Postgres. */
@ConfigurationProperties("hierarchy")
public record HierarchyProperties(
        @DefaultValue("0") int workerId,
        Map<String, Shard> shards,
        Map<String, String> tenants,
        @DefaultValue("2s") Duration lockTimeout,
        @DefaultValue Move move,
        @DefaultValue Relay relay,
        @DefaultValue Replicas replicas,
        @DefaultValue Cache cache,
        @DefaultValue Events events) {

    public static final String DEFAULT_SHARD = "s0";

    public HierarchyProperties {
        shards = shards == null || shards.isEmpty()
                ? Map.of(DEFAULT_SHARD, new Shard("jdbc:postgresql://localhost:5432/hierarchy", "hierarchy",
                "hierarchy", 10, List.of()))
                : Map.copyOf(shards);
        tenants = tenants == null ? Map.of() : Map.copyOf(tenants);
        for (Map.Entry<String, String> e : tenants.entrySet()) {
            if (!shards.containsKey(e.getValue())) {
                throw new IllegalArgumentException("tenant " + e.getKey() + " is placed on unknown shard "
                        + e.getValue());
            }
        }
    }

    /** {@code tenants} with UUID keys: explicit placements (whales on their own shard); others are hashed. */
    public Map<UUID, String> placements() {
        return tenants.entrySet().stream().collect(Collectors.toMap(e -> UUID.fromString(e.getKey()),
                Map.Entry::getValue));
    }

    /** One database shard: a primary and any number of streaming replicas. */
    public record Shard(String url, String username, String password, @DefaultValue("10") int poolSize,
                        List<String> replicas) {
        public Shard {
            replicas = replicas == null ? List.of() : List.copyOf(replicas);
        }
    }

    /**
     * @param syncLimit       subtrees up to this size move in one statement; larger ones become a background job
     * @param workerEnabled   run the background move worker in this pod
     * @param sweepIntervalMs how often the worker looks for unclaimed jobs (expired leases)
     * @param maxReplicaLag   the worker pauses while replicas lag more than this
     */
    public record Move(@DefaultValue("10000") int syncLimit,
                       @DefaultValue("1000") int batchSize,
                       @DefaultValue("true") boolean workerEnabled,
                       @DefaultValue("1000") long sweepIntervalMs,
                       @DefaultValue("30s") Duration lease,
                       @DefaultValue("1s") Duration maxReplicaLag,
                       @DefaultValue("200ms") Duration pause,
                       @DefaultValue("1000") int purgeBatchSize) {
    }

    public record Relay(@DefaultValue("true") boolean enabled,
                        @DefaultValue("50") long intervalMs,
                        @DefaultValue("500") int batchSize,
                        @DefaultValue("20") int maxBatchesPerTick,
                        @DefaultValue("24h") Duration idempotencyTtl) {
    }

    /** @param maxLag token-less reads go to a replica only while it is less than this behind */
    public record Replicas(@DefaultValue("20") long probeIntervalMs, @DefaultValue("1s") Duration maxLag) {
    }

    /** @param type {@code none} or {@code redis} */
    public record Cache(@DefaultValue("none") String type,
                        @DefaultValue("redis://localhost:6379") String redisUri,
                        @DefaultValue("1h") Duration ttl,
                        @DefaultValue("10m") Duration nodeTtl) {
    }

    /** @param type {@code inprocess} or {@code kafka} */
    public record Events(@DefaultValue("inprocess") String type,
                         @DefaultValue("localhost:9092") String kafkaBootstrap,
                         @DefaultValue("hierarchy.events") String topic) {
    }
}
