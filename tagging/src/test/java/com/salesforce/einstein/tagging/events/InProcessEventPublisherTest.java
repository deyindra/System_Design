package com.salesforce.einstein.tagging.events;

import com.salesforce.einstein.tagging.spi.EventPublisher;
import com.salesforce.einstein.tagging.spi.EventPublisherContractTest;
import com.salesforce.einstein.tagging.spi.TagEventListener;

import java.util.List;

class InProcessEventPublisherTest extends EventPublisherContractTest {
    @Override
    protected EventPublisher newPublisher(TagEventListener listener) {
        return new InProcessEventPublisher(List.of(listener));
    }

    @Override
    protected EventPublisher failingPublisher() {
        return new InProcessEventPublisher(List.of(events -> {
            throw new IllegalStateException("listener down");
        }));
    }
}
