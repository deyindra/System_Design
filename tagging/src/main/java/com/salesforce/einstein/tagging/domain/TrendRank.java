package com.salesforce.einstein.tagging.domain;

/** How trending tags are ordered. */
public enum TrendRank {
    /** Most attaches in the window. */
    POPULAR,
    /** Largest increase in attaches over the window before; only tags that grew. */
    RISING
}
