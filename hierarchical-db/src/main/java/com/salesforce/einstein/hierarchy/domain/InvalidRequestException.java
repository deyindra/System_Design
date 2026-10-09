package com.salesforce.einstein.hierarchy.domain;

import java.io.Serial;

/** 400: the request is malformed or names things that can't go together. */
public final class InvalidRequestException extends HierarchyException {
    @Serial
    private static final long serialVersionUID = 1L;

    public InvalidRequestException(String message) {
        super(message);
    }
}
