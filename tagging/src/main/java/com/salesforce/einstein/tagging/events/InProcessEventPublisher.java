package com.salesforce.einstein.tagging.events;

import com.salesforce.einstein.tagging.domain.TagEvent;
import com.salesforce.einstein.tagging.spi.EventPublisher;
import com.salesforce.einstein.tagging.spi.TagEventListener;

import java.util.List;

/**
 * Delivers events straight to the local listeners, on the relay's thread. It suits a single node,
 * tests and local runs. With several pods, use a broker (Kafka) so every pod's projections see every
 * event. A listener failure fails the publish, so the batch is redelivered to all listeners (they dedupe).
 */
public final class InProcessEventPublisher implements EventPublisher {
    private final List<TagEventListener> listeners;

    public InProcessEventPublisher(List<? extends TagEventListener> listeners) {
        this.listeners = List.copyOf(listeners);
    }

    @Override
    public void publish(List<TagEvent> events) {
        if (events.isEmpty()) {
            return;
        }
        for (TagEventListener l : listeners) {
            l.onEvents(events);
        }
    }
}
