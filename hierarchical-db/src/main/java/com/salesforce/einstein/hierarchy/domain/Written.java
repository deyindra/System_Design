package com.salesforce.einstein.hierarchy.domain;

/** The result of a write plus the read token that lets the writer read it back from a replica. */
public record Written<T>(T value, ReadToken token) {
}
