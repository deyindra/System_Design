package com.salesforce.einstein.hierarchy.domain;

import java.io.Serial;

/**
 * 409: the request is valid but conflicts with the tree's current state (a cycle, the depth limit, a large move still
 * running). {@link #retryable()} conflicts carry {@code Retry-After}, since they clear on their own.
 */
public final class ConflictException extends HierarchyException {
    @Serial
    private static final long serialVersionUID = 1L;

    private final boolean retryable;

    private ConflictException(String message, boolean retryable) {
        super(message);
        this.retryable = retryable;
    }

    public static ConflictException permanent(String message) {
        return new ConflictException(message, false);
    }

    public static ConflictException retryLater(String message) {
        return new ConflictException(message, true);
    }

    public boolean retryable() {
        return retryable;
    }
}
