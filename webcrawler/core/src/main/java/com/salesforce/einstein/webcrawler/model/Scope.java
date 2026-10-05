package com.salesforce.einstein.webcrawler.model;

/** Which <b>pages</b> a job may follow. Assets of an in-scope page are allowed from any host (CDNs). */
public enum Scope {
    /** Only the seed's exact host ({@code docs.example.com}). */
    SAME_HOST,
    /** The seed's registrable domain ({@code *.example.com}). */
    SAME_DOMAIN,
    /** Anywhere; only depth and page budgets bound the crawl. */
    ANY
}
