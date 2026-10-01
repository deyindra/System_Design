package com.salesforce.einstein.scheduler;

/**
 * Completes a {@link TaskHandle} when its job failed on another node, so the original exception object
 * isn't available. The message is that exception's {@code toString()}.
 */
public final class JobFailedException extends RuntimeException {
    public JobFailedException(String message) { super(message); }
}
