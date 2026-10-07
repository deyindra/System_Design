package com.salesforce.einstein.webcrawler.api;

import java.io.Serial;

/** 404. Also used for another tenant's job, so job ids can't be probed. */
public final class NotFoundException extends RuntimeException {
    @Serial
    private static final long serialVersionUID = 1L;

    public NotFoundException(String message) { super(message); }
}
