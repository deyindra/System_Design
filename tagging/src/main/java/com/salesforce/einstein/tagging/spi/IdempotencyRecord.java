package com.salesforce.einstein.tagging.spi;

import java.time.Instant;

/** A completed request, stored with the write so a retry with the same key returns the same response. */
public record IdempotencyRecord(String tenantId, String key, String requestHash, String response, Instant createdAt) {
}
