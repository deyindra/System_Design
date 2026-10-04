package com.salesforce.einstein.tagging.domain;

/** The tag or entity doesn't exist in this tenant, or is deleted (HTTP 404). Cross-tenant lookups also land here, so tenant existence never leaks. */
public class NotFoundException extends TaggingException {
    public NotFoundException(String message) {
        super(message);
    }
}
