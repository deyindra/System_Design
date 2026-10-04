package com.salesforce.einstein.tagging.domain;

import java.util.Set;

/**
 * Boolean tag search: entities that carry <b>all</b> of {@code all}, <b>at least one</b> of {@code any},
 * and <b>none</b> of {@code none}, optionally of a single entity type.
 *
 * <p>At least one of {@code all} or {@code any} must be non-empty. A pure-negative query ("everything
 * without tag X") would be a scan of the whole tenant and is rejected.
 *
 * @param afterSeq keyset cursor: only entities whose {@code entitySeq} is greater than this are returned
 */
public record TagQuery(Set<Long> all, Set<Long> any, Set<Long> none, String entityType, long afterSeq, int limit) {
    public TagQuery {
        all = all == null ? Set.of() : Set.copyOf(all);
        any = any == null ? Set.of() : Set.copyOf(any);
        none = none == null ? Set.of() : Set.copyOf(none);
        if (all.isEmpty() && any.isEmpty()) {
            throw new InvalidRequestException("search needs at least one tag in 'all' or 'any'");
        }
        EntityRef.checkTypeFilter(entityType);
        if (limit <= 0) {
            throw new InvalidRequestException("limit must be positive");
        }
        if (afterSeq < 0) {
            throw new InvalidRequestException("invalid cursor");
        }
    }

    public int termCount() {
        return all.size() + any.size() + none.size();
    }
}
