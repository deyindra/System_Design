package com.salesforce.einstein.hierarchy.domain;

import org.springframework.lang.Nullable;

import java.time.Instant;

/** A large move: accepted at once through the overlay, then rewritten in background batches. */
public record MoveJob(long id, long spaceId, long rootNodeId, String oldPrefix, String newPrefix, int depthDelta,
                      State state, long rowsDone, Instant createdAt, @Nullable Instant finishedAt) {
    public enum State {
        RUNNING, DONE
    }

    public Overlay overlay() {
        return state == State.RUNNING ? new Overlay(id, oldPrefix, newPrefix, depthDelta) : Overlay.NONE;
    }
}
