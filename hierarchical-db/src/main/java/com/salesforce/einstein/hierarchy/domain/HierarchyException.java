package com.salesforce.einstein.hierarchy.domain;

import java.io.Serial;

/** Base of every business error. The API layer maps each subclass to one HTTP status. */
public abstract class HierarchyException extends RuntimeException {
    @Serial
    private static final long serialVersionUID = 1L;

    protected HierarchyException(String message) {
        super(message);
    }
}
