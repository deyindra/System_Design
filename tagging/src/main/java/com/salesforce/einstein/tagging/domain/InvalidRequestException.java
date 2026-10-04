package com.salesforce.einstein.tagging.domain;

/** The request is malformed or violates a validation rule (HTTP 400). */
public class InvalidRequestException extends TaggingException {
    public InvalidRequestException(String message) {
        super(message);
    }
}
