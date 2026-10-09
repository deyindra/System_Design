package com.salesforce.einstein.hierarchy.domain;

import java.io.Serial;

/** 403: the caller can view the node but may not change it. */
public final class ForbiddenException extends HierarchyException {
    @Serial
    private static final long serialVersionUID = 1L;

    public ForbiddenException(String message) {
        super(message);
    }
}
