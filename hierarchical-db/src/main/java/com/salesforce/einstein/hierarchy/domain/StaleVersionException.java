package com.salesforce.einstein.hierarchy.domain;

import java.io.Serial;

/** 412: the {@code If-Match} version is not the current one; someone else changed the node first. */
public final class StaleVersionException extends HierarchyException {
    @Serial
    private static final long serialVersionUID = 1L;

    public StaleVersionException(long nodeId, long expected) {
        super("node " + nodeId + " is no longer at version " + expected + "; re-read it and retry");
    }
}
