package com.salesforce.einstein.webcrawler.model;

/** State of one node (URL) inside one crawl job. */
public enum PageStatus {
    /** Admitted to the frontier, not processed yet. */
    QUEUED,
    /** Downloaded in this job. */
    FETCHED,
    /** Not downloaded: an earlier crawl's copy was fresh enough (or the origin answered 304). */
    REUSED,
    /** Same bytes as another URL of this job ({@link JobPage#duplicateOf()}); not expanded again. */
    DUPLICATE,
    /** 3xx; the target is {@link JobPage#redirectTo()}. */
    REDIRECT,
    /** Asset recorded (URL, type, referrer) but not downloaded, by policy (e.g. video, audio). */
    REFERENCED,
    BLOCKED_ROBOTS,
    TOO_LARGE,
    FAILED;

    /** Has bytes we can serve. */
    public boolean hasContent() { return this == FETCHED || this == REUSED; }

    public boolean isFinal() { return this != QUEUED; }
}
