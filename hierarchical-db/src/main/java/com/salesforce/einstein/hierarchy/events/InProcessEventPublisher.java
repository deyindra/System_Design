package com.salesforce.einstein.hierarchy.events;

import com.salesforce.einstein.hierarchy.domain.TreeEvent;
import com.salesforce.einstein.hierarchy.spi.EventPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * {@code hierarchy.events.type=inprocess}: no broker. Events are only logged; the relay still hands every batch to the
 * local {@link com.salesforce.einstein.hierarchy.spi.TreeEventListener}s, which is all a single node needs.
 */
public final class InProcessEventPublisher implements EventPublisher {
    private static final Logger log = LoggerFactory.getLogger(InProcessEventPublisher.class);

    @Override
    public void publish(List<TreeEvent> events) {
        if (log.isDebugEnabled()) {
            events.forEach(e -> log.debug("event {} {} node {} in space {}", e.seq(), e.type(), e.nodeId(),
                    e.spaceId()));
        }
    }
}
