package com.salesforce.einstein.hierarchy.spi;

import com.salesforce.einstein.hierarchy.domain.TreeEvent;

import java.util.List;

/**
 * Where the outbox relay sends committed events. {@link #publish} returns only once the events are durable at the
 * destination; if it throws, the relay retries the same batch, so delivery is at least once.
 */
public interface EventPublisher {
    void publish(List<TreeEvent> events);
}
