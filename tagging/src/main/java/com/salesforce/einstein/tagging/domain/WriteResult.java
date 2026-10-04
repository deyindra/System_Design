package com.salesforce.einstein.tagging.domain;

import java.util.Objects;

/**
 * The outcome of a write plus the token for read-your-writes. A write that changed nothing returns
 * {@link ConsistencyToken#NONE}: it produced no event, so there's nothing new to wait for.
 */
public record WriteResult<T>(T value, ConsistencyToken token) {
    public WriteResult {
        Objects.requireNonNull(token, "token");
    }
}
