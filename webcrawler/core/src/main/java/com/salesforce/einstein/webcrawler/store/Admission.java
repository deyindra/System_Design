package com.salesforce.einstein.webcrawler.store;

/** Result of the atomic seen-test + budget check when a URL is discovered. */
public enum Admission {
    /** First time this job sees the URL; it is now a node of the job's graph. */
    ADMITTED,
    /** Already a node of this job's graph (cycle, or a second link to it). */
    SEEN,
    /**
     * Already a node, but this path is shorter, so its depth was lowered (no budget charged). With many workers the
     * crawl is only roughly breadth-first, so a page can be reached first by a long path; this corrects it.
     */
    SHALLOWER,
    /** Budget (pages or assets) exhausted; not recorded. */
    OVER_BUDGET
}
