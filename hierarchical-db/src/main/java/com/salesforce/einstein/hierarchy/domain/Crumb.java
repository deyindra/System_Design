package com.salesforce.einstein.hierarchy.domain;

/** One breadcrumb entry: just enough to render the path to a node. */
public record Crumb(long id, String title, String type, NodeStatus status, int depth) {
}
