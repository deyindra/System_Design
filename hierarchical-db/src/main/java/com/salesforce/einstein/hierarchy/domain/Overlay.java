package com.salesforce.einstein.hierarchy.domain;

/**
 * The path-rewrite rule of a RUNNING large move (DESIGN.md §8.5): a stored path under {@code oldPrefix} is really
 * {@code newPrefix + rest}, {@code depthDelta} levels deeper or shallower. A space has at most one such rule, and every
 * read applies it, so the tree looks fully moved from the moment the move is accepted.
 */
public record Overlay(long jobId, String oldPrefix, String newPrefix, int depthDelta) {
    public static final Overlay NONE = new Overlay(0, "", "", 0);

    public boolean isActive() {
        return jobId != 0;
    }

    /** True when the stored path has not been rewritten yet. */
    public boolean applies(String storedPath) {
        return isActive() && storedPath.startsWith(oldPrefix);
    }

    public String path(String storedPath) {
        return applies(storedPath) ? newPrefix + storedPath.substring(oldPrefix.length()) : storedPath;
    }

    public int depth(String storedPath, int storedDepth) {
        return applies(storedPath) ? storedDepth + depthDelta : storedDepth;
    }

    public Node apply(Node stored) {
        return applies(stored.path()) ? stored.withPath(path(stored.path()), stored.depth() + depthDelta) : stored;
    }
}
