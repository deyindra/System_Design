package com.salesforce.einstein.tagging.tenant;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.salesforce.einstein.tagging.config.TaggingProperties;
import com.salesforce.einstein.tagging.domain.RateLimitedException;
import com.salesforce.einstein.tagging.domain.TenantInfo;
import com.salesforce.einstein.tagging.spi.TenantIsolation;
import io.github.resilience4j.bulkhead.Bulkhead;
import io.github.resilience4j.bulkhead.BulkheadConfig;
import io.github.resilience4j.ratelimiter.RateLimiter;
import io.github.resilience4j.ratelimiter.RateLimiterConfig;
import io.micrometer.core.instrument.MeterRegistry;

import java.time.Duration;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Per-tenant token buckets (one per operation class) plus per-tenant bulkheads, created lazily and
 * evicted when a tenant goes idle. A tenant that floods the service exhausts only its own buckets, so
 * other tenants on the same pod and shard are unaffected. Bulk work has its own small bulkhead, so an
 * import can't take the tenant's interactive concurrency.
 *
 * <p>Rejections are counted by tier, never by tenant id, to keep metric cardinality bounded.
 */
public final class Resilience4jTenantIsolation implements TenantIsolation {
    private static final Duration PERIOD = Duration.ofSeconds(1);

    private final TaggingProperties props;
    private final MeterRegistry metrics;
    private final Cache<String, Guards> guards = Caffeine.newBuilder()
            .expireAfterAccess(Duration.ofMinutes(10))
            .maximumSize(200_000)
            .build();

    public Resilience4jTenantIsolation(TaggingProperties props, MeterRegistry metrics) {
        this.props = props;
        this.metrics = metrics;
    }

    private record Guards(RateLimiter reads, RateLimiter writes, RateLimiter searches,
                          Bulkhead interactive, Bulkhead bulk, String tier) {
    }

    @Override
    public <T> T execute(TenantInfo tenant, OpClass op, int permits, Supplier<T> work) {
        Guards g = guards.get(tenant.tenantId() + '|' + tenant.tier(), k -> create(tenant));
        RateLimiter limiter = switch (op) {
            case READ -> g.reads();
            case WRITE, BULK -> g.writes();
            case SEARCH -> g.searches();
        };
        int cost = Math.max(1, Math.min(permits, limiter.getRateLimiterConfig().getLimitForPeriod()));
        if (!limiter.acquirePermission(cost)) {
            reject(g, op, "rate");
            throw new RateLimitedException("tenant over " + op.name().toLowerCase() + " quota", PERIOD);
        }
        Bulkhead bulkhead = op == OpClass.BULK ? g.bulk() : g.interactive();
        if (!bulkhead.tryAcquirePermission()) {
            reject(g, op, "concurrency");
            throw new RateLimitedException("too many concurrent requests for tenant", PERIOD);
        }
        try {
            return work.get();
        } finally {
            bulkhead.onComplete();
        }
    }

    private void reject(Guards g, OpClass op, String reason) {
        metrics.counter("tagging.tenant.rejected", "tier", g.tier(), "op", op.name(), "reason", reason).increment();
    }

    private Guards create(TenantInfo tenant) {
        TaggingProperties.Tier tier = props.tier(tenant.tier());
        Function<Integer, RateLimiter> limiter = perSecond -> RateLimiter.of(tenant.tenantId(),
                RateLimiterConfig.custom()
                        .limitForPeriod(perSecond)
                        .limitRefreshPeriod(PERIOD)
                        .timeoutDuration(Duration.ZERO)
                        .build());
        Function<Integer, Bulkhead> bulkhead = max -> Bulkhead.of(tenant.tenantId(),
                BulkheadConfig.custom().maxConcurrentCalls(max).maxWaitDuration(Duration.ZERO).build());
        return new Guards(limiter.apply(tier.readsPerSecond()), limiter.apply(tier.writesPerSecond()),
                limiter.apply(tier.searchesPerSecond()), bulkhead.apply(tier.maxConcurrent()),
                bulkhead.apply(tier.maxConcurrentBulk()), tenant.tier());
    }
}
