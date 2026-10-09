package com.salesforce.einstein.hierarchy.domain;

/** A move either finished in its transaction (small subtree) or was accepted with a background job (large subtree). */
public sealed interface MoveOutcome {
    Node node();

    /** 200: every path is rewritten. */
    record Done(Node node) implements MoveOutcome {
    }

    /** 202: the tree already reads as moved (overlay); the job rewrites the stored paths. */
    record Accepted(Node node, MoveJob job) implements MoveOutcome {
    }
}
