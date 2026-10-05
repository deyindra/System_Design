package com.salesforce.einstein.graphexecutor.executor;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Thrown by {@link TaskExecutor#submit} under {@link IneligibleGroupPolicy#REJECT_GRAPH} when at least one
 * group fails {@link TaskExecutor#validateGroup}. No group has run.
 *
 * <p>An {@link IllegalArgumentException} because the input graph is what is wrong. (Exceptions cannot be
 * generic, hence the wildcard types.)
 */
public final class IneligibleGraphException extends IllegalArgumentException {
    private final transient List<GroupResult<?, ?>> rejected;

    IneligibleGraphException(List<? extends GroupResult<?, ?>> rejected) {
        super("graph is not eligible for execution: " + rejected.stream()
                .map(r -> "group " + r.group().id() + " (" + r.error().getMessage() + ")")
                .collect(Collectors.joining(", ")));
        this.rejected = List.copyOf(rejected);
        rejected.forEach(r -> addSuppressed(r.error()));
    }

    /** One failed result per rejected group, in group order; {@link GroupResult#error()} says why. */
    public List<GroupResult<?, ?>> rejected() {
        return rejected;
    }
}
