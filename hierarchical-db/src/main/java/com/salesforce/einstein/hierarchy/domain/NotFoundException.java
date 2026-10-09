package com.salesforce.einstein.hierarchy.domain;

import java.io.Serial;

/** 404. Also used when the caller may not view the node, so restricted pages don't leak their existence. */
public final class NotFoundException extends HierarchyException {
    @Serial
    private static final long serialVersionUID = 1L;

    public NotFoundException(String message) {
        super(message);
    }
}
