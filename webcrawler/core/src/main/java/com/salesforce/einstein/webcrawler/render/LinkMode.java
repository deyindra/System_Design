package com.salesforce.einstein.webcrawler.render;

/** How references inside a served page are written. */
public enum LinkMode {
    /** Exactly the bytes we fetched. Relative links break when served from our domain. */
    RAW,
    /** Every reference made absolute to the live origin. Looks like the original site, but still loads from it. */
    ABSOLUTE,
    /**
     * Every reference to something this job stored points at our copy; everything else is absolute to the origin.
     * Browsing the result never leaves the snapshot for content we have, and never breaks for content we don't.
     */
    SNAPSHOT
}
