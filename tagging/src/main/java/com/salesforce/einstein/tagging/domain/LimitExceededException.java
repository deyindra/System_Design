package com.salesforce.einstein.tagging.domain;

/** A per-tenant or per-entity quota would be exceeded, such as max tags per tenant (HTTP 422). */
public class LimitExceededException extends TaggingException {
    public LimitExceededException(String message) {
        super(message);
    }
}
