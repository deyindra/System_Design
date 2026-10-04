package com.salesforce.einstein.tagging.domain;

/** Another live tag in the tenant already has this normalized name (HTTP 409). */
public class DuplicateTagNameException extends TaggingException {
    public DuplicateTagNameException(String message) {
        super(message);
    }
}
