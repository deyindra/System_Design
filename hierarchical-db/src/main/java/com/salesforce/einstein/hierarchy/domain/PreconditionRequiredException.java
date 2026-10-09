package com.salesforce.einstein.hierarchy.domain;

import java.io.Serial;

/** 428: an edit arrived without {@code If-Match}, which would allow lost updates. */
public final class PreconditionRequiredException extends HierarchyException {
    @Serial
    private static final long serialVersionUID = 1L;

    public PreconditionRequiredException(String message) {
        super(message);
    }
}
