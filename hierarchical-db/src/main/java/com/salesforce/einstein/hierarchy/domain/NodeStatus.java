package com.salesforce.einstein.hierarchy.domain;

/** A TRASHED node hides its whole subtree; descendants keep their own status (trash is O(1)). */
public enum NodeStatus {
    ACTIVE, TRASHED
}
