package com.salesforce.einstein.webcrawler.model;

public enum JobStatus {
    QUEUED, RUNNING, COMPLETED, CANCELLED, FAILED;

    public boolean isTerminal() { return this == COMPLETED || this == CANCELLED || this == FAILED; }
}
