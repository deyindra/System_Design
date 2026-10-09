package com.salesforce.einstein.hierarchy.domain;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.UUID;

/**
 * Who is calling: the tenant (which picks the shard), the actor and the actor's groups. The gateway authenticates;
 * this service trusts the headers it forwards.
 */
public record Caller(UUID tenantId, String actorId, Set<String> groups) {
    public Caller {
        groups = Set.copyOf(groups);
    }

    public static Caller of(UUID tenantId, String actorId) {
        return new Caller(tenantId, actorId, Set.of());
    }

    /** The principals restrictions are written against: {@code user:<actor>} and {@code group:<g>} for each group. */
    public Set<String> principals() {
        Set<String> p = new LinkedHashSet<>();
        p.add("user:" + actorId);
        groups.forEach(g -> p.add("group:" + g));
        return p;
    }
}
