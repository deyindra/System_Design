package com.salesforce.einstein.tagging.spi;

import com.salesforce.einstein.tagging.domain.TenantInfo;

import java.util.function.Supplier;

/**
 * Port that stops noisy neighbours: it applies the tenant's rate quota and concurrency cap around a
 * unit of work.
 *
 * <p>Built in: {@code Resilience4jTenantIsolation} (per pod). A cluster-wide adapter (Redis token
 * bucket, Envoy global rate limit service) implements the same interface.
 */
public interface TenantIsolation {
    enum OpClass { READ, WRITE, BULK, SEARCH }

    /**
     * @param permits cost of the call in quota units (a bulk write of 500 entities costs more than one attach)
     * @throws com.salesforce.einstein.tagging.domain.RateLimitedException when the tenant is over quota
     */
    <T> T execute(TenantInfo tenant, OpClass op, int permits, Supplier<T> work);
}
