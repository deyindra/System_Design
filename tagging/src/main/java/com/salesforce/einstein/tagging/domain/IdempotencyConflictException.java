package com.salesforce.einstein.tagging.domain;

/** An {@code Idempotency-Key} was reused with a different request body (HTTP 422). */
public class IdempotencyConflictException extends TaggingException {
    public IdempotencyConflictException(String message) {
        super(message);
    }
}
