package com.salesforce.einstein.webcrawler.engine;

import java.io.Serial;

/** The same {@code Idempotency-Key} was reused with a different request body (HTTP 409). */
public final class IdempotencyConflictException extends RuntimeException {
    @Serial
    private static final long serialVersionUID = 1L;

    public IdempotencyConflictException(String key) {
        super("Idempotency-Key '" + key + "' was already used with a different request");
    }
}
