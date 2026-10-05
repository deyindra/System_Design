package com.salesforce.einstein.webcrawler.api;

/** 404. Also used for another tenant's job, so job ids can't be probed. */
public final class NotFoundException extends RuntimeException {
    public NotFoundException(String message) { super(message); }
}
