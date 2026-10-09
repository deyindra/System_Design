package com.salesforce.einstein.hierarchy.domain;

import org.springframework.lang.Nullable;

/** A node as {@code GET /nodes/{id}} returns it: {@code inTrash} is true when it or any ancestor is TRASHED. */
public record NodeDetail(Node node, boolean inTrash, @Nullable String body) {
}
