package com.salesforce.einstein.tagging.domain;

import java.time.Duration;

/** The tenant exceeded its rate or concurrency quota (HTTP 429 with {@code Retry-After}). */
public class RateLimitedException extends TaggingException {
    private final Duration retryAfter;

    public RateLimitedException(String message, Duration retryAfter) {
        super(message);
        this.retryAfter = retryAfter;
    }

    public Duration retryAfter() {
        return retryAfter;
    }
}
