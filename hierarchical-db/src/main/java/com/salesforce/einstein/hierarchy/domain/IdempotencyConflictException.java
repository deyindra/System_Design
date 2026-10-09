package com.salesforce.einstein.hierarchy.domain;

import java.io.Serial;

/** 422: an {@code Idempotency-Key} was reused with a different request body. */
public final class IdempotencyConflictException extends HierarchyException {
    @Serial
    private static final long serialVersionUID = 1L;

    public IdempotencyConflictException(String key) {
        super("Idempotency-Key '" + key + "' was already used for a different request");
    }
}
