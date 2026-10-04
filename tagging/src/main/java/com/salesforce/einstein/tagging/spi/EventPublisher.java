package com.salesforce.einstein.tagging.spi;

import com.salesforce.einstein.tagging.domain.TagEvent;

import java.util.List;

/**
 * Port to the event transport. It must preserve order per tenant and may only return once every
 * event is durably accepted. Throwing makes the relay retry the batch.
 *
 * <p>Built in: {@code InProcessEventPublisher} (single node) and {@code KafkaEventPublisher} (any
 * Kafka-protocol broker). Cloud-specific adapters (Pub/Sub with ordering keys, SNS/SQS FIFO with
 * message group = tenant) implement this interface in their own module.
 */
public interface EventPublisher {
    void publish(List<TagEvent> events);
}
