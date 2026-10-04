package com.salesforce.einstein.tagging.domain;

/** The {@code If-Match} version doesn't match the current tag version (HTTP 412). */
public class StaleVersionException extends TaggingException {
    public StaleVersionException(String message) {
        super(message);
    }
}
