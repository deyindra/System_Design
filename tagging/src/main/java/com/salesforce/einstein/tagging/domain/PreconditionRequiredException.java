package com.salesforce.einstein.tagging.domain;

/** A conditional request header ({@code If-Match}) is required but missing (HTTP 428). */
public class PreconditionRequiredException extends TaggingException {
    public PreconditionRequiredException(String message) {
        super(message);
    }
}
