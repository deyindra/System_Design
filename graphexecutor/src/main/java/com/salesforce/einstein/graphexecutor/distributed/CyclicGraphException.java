package com.salesforce.einstein.graphexecutor.distributed;

import java.io.Serial;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * The dry run found cycles and the policy is {@code REJECT_GRAPH}: nothing ran.
 *
 * <p>Not generic (Java forbids generic {@code Throwable}s), hence {@code List<?>}. The map and lists are
 * immutable JDK collections, so the exception serializes if the nodes do.
 */
public final class CyclicGraphException extends IllegalArgumentException {
    @Serial
    private static final long serialVersionUID = 1L;

    private final Map<Integer, List<?>> blockedByGroup;   // immutable and serializable when the nodes are

    CyclicGraphException(Map<Integer, ? extends List<?>> blockedByGroup) {
        super("graph is not eligible for execution: cycles in groups " + new TreeMap<>(blockedByGroup));
        this.blockedByGroup = Map.copyOf(blockedByGroup);
    }

    /** Group id -> its nodes that are on a cycle or downstream of one. */
    public Map<Integer, List<?>> blockedByGroup() {
        return blockedByGroup;
    }
}
