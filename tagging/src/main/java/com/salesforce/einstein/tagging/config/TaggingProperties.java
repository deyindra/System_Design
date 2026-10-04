package com.salesforce.einstein.tagging.config;

import com.salesforce.einstein.tagging.store.JdbcTagStore;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.Bindable;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Every tunable of the service, bound from {@code tagging.*}. Defaults are production-sane.
 * Infrastructure choices are made here by name ({@code providers}), never in code, so the same
 * build runs on AWS, GCP or on-prem.
 */
@ConfigurationProperties("tagging")
public record TaggingProperties(
        @DefaultValue Providers providers,
        Map<String, Shard> shards,
        Map<String, TenantOverride> tenants,
        Map<String, Tier> tiers,
        @DefaultValue("STANDARD") String defaultTier,
        @DefaultValue Limits limits,
        @DefaultValue Consistency consistency,
        @DefaultValue Relay relay,
        @DefaultValue Purge purge,
        @DefaultValue Cache cache,
        @DefaultValue Index index,
        @DefaultValue Kafka kafka,
        @DefaultValue Trending trending,
        @DefaultValue("16") int usageBuckets,
        @DefaultValue("0") int workerId) {

    public static final String DEFAULT_SHARD = "s0";

    public TaggingProperties {
        shards = shards == null || shards.isEmpty() ? Map.of(DEFAULT_SHARD, new Shard(null, null, null, null, 10))
                : Map.copyOf(shards);
        tenants = tenants == null ? Map.of() : Map.copyOf(tenants);
        Map<String, Tier> t = new LinkedHashMap<>();
        t.put("STANDARD", Tier.STANDARD);
        if (tiers != null) {
            t.putAll(tiers);
        }
        tiers = Map.copyOf(t);
        if (!tiers.containsKey(defaultTier)) {
            throw new IllegalArgumentException("tagging.default-tier '" + defaultTier + "' is not a configured tier");
        }
        for (Map.Entry<String, TenantOverride> e : tenants.entrySet()) {
            TenantOverride o = e.getValue();
            if (o.shard() != null && !shards.containsKey(o.shard())) {
                throw new IllegalArgumentException("tenant " + e.getKey() + " pinned to unknown shard " + o.shard());
            }
            if (o.tier() != null && !tiers.containsKey(o.tier())) {
                throw new IllegalArgumentException("tenant " + e.getKey() + " has unknown tier " + o.tier());
            }
        }
    }

    /** Binds {@code overrides} (relaxed names under {@code tagging.}) over the defaults. Used by tests and tools. */
    public static TaggingProperties of(Map<String, String> overrides) {
        Map<String, String> prefixed = new LinkedHashMap<>();
        overrides.forEach((k, v) -> prefixed.put("tagging." + k, v));
        return new Binder(new MapConfigurationPropertySource(prefixed))
                .bindOrCreate("tagging", Bindable.of(TaggingProperties.class));
    }

    public Tier tier(String name) {
        Tier t = tiers.get(name);
        return t == null ? tiers.get(defaultTier) : t;
    }

    /** Adapter names per port. Each name selects one {@code @Bean}; new adapters add new names. */
    public record Providers(@DefaultValue("jdbc") String store, @DefaultValue("inprocess") String events) {
    }

    /** One database shard: a primary and an optional async replica for eventual/session reads. */
    public record Shard(String url, String username, String password, String replicaUrl,
                        @DefaultValue("10") int poolSize) {
    }

    /** Pins a tenant to a shard (whales get dedicated shards) and/or a quota tier. */
    public record TenantOverride(String shard, String tier) {
    }

    /** Per-tenant quotas, enforced per pod. Cluster-wide quotas belong in the ingress (Envoy global rate limit). */
    public record Tier(@DefaultValue("2000") int readsPerSecond,
                       @DefaultValue("500") int writesPerSecond,
                       @DefaultValue("200") int searchesPerSecond,
                       @DefaultValue("64") int maxConcurrent,
                       @DefaultValue("4") int maxConcurrentBulk) {
        static final Tier STANDARD = new Tier(2000, 500, 200, 64, 4);
    }

    public record Limits(@DefaultValue("10000") int maxTagsPerTenant,
                         @DefaultValue("100") int maxTagsPerEntity,
                         @DefaultValue("500") int maxBulkEntities,
                         @DefaultValue("50") int maxBulkTags,
                         @DefaultValue("32") int maxSearchTerms,
                         @DefaultValue("50") int defaultPageSize,
                         @DefaultValue("200") int maxPageSize) {
    }

    /** How long a search waits for the index to reach its read barrier before the primary's SQL answers. */
    public record Consistency(@DefaultValue("50ms") Duration indexWait) {
    }

    public record Relay(@DefaultValue("500") int batchSize,
                        @DefaultValue("20") int maxBatchesPerTick,
                        @DefaultValue("30s") Duration gapTimeout,
                        @DefaultValue("7d") Duration outboxRetention,
                        @DefaultValue("24h") Duration idempotencyRetention) {
        public Relay {
            if (gapTimeout.toSeconds() <= JdbcTagStore.WRITE_TIMEOUT_SECONDS) {
                throw new IllegalArgumentException("tagging.relay.gap-timeout must exceed the "
                        + JdbcTagStore.WRITE_TIMEOUT_SECONDS + "s write transaction timeout");
            }
        }
    }

    public record Purge(@DefaultValue("1000") int batchSize) {
    }

    public record Cache(@DefaultValue("1000000") long entityMaxSize,
                        @DefaultValue("200000") long tagMaxSize,
                        @DefaultValue("10m") Duration ttl,
                        @DefaultValue("5m") Duration tombstoneTtl) {
    }

    public record Index(@DefaultValue("true") boolean enabled,
                        @DefaultValue("1000") long maxTenants,
                        @DefaultValue("2") int bootstrapThreads) {
    }

    /**
     * The trending projection. {@code url} null means an in-process H2 database (local runs); in production
     * point it at its own PostgreSQL, apart from the shards. Retention must cover two windows, because
     * "rising" compares a window with the one before it.
     */
    public record Trending(@DefaultValue("true") boolean enabled,
                           String url, String username, String password,
                           @DefaultValue("5") int poolSize,
                           @DefaultValue("30s") Duration cacheTtl,
                           @DefaultValue("100000") long cacheMaxSize,
                           @DefaultValue("10") int defaultLimit,
                           @DefaultValue("50") int maxLimit,
                           @DefaultValue("3d") Duration hourlyRetention,
                           @DefaultValue("15d") Duration dailyRetention) {
        public Trending {
            if (hourlyRetention.compareTo(Duration.ofHours(49)) < 0 || dailyRetention.compareTo(Duration.ofDays(15)) < 0) {
                throw new IllegalArgumentException("tagging.trending retention must cover two windows plus the "
                        + "current bucket: hourly-retention >= 49h, daily-retention >= 15d");
            }
        }
    }

    public record Kafka(@DefaultValue("localhost:9092") String bootstrapServers,
                        @DefaultValue("tag-events") String topic,
                        @DefaultValue("tagging") String groupPrefix,
                        String podId) {
    }
}
