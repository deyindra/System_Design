package com.salesforce.einstein.webcrawler.engine;

/** The same {@code Idempotency-Key} was reused with a different request body (HTTP 409). */
public final class IdempotencyConflictException extends RuntimeException {
    public IdempotencyConflictException(String key) {
        super("Idempotency-Key '" + key + "' was already used with a different request");
    }
}
