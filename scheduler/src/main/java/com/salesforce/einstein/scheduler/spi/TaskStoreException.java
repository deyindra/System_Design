package com.salesforce.einstein.scheduler.spi;

/** The backing store failed (I/O, SQL, coordination service); the operation had no effect. */
public class TaskStoreException extends RuntimeException {
    public TaskStoreException(String message, Throwable cause) { super(message, cause); }
}
