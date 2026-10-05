package com.salesforce.einstein.webcrawler.model;

/**
 * The crawling boundary. {@link #SYNC} is a convenience for tiny crawls (bounded depth, pages and deadline);
 * if the deadline passes it degrades to {@link #ASYNC} and the caller gets a job id instead of a result.
 */
public enum CrawlMode { SYNC, ASYNC }
