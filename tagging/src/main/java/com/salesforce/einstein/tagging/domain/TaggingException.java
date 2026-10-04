package com.salesforce.einstein.tagging.domain;

/** Base of every business error. The API layer maps each subclass to one HTTP status. */
public abstract class TaggingException extends RuntimeException {
    protected TaggingException(String message) {
        super(message);
    }
}
