package com.salesforce.einstein.hierarchy.domain;

/** A Confluence space or a Jira project: one tree, one root, and the unit of move serialization. */
public record Space(long id, String key, long rootNodeId, long treeVersion) {
}
