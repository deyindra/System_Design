package com.salesforce.einstein.tagging.spi;

import com.salesforce.einstein.tagging.domain.TagEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * The {@link EventPublisher} contract: per-tenant order is preserved, and {@code publish} returns only
 * once the events are durable (or throws), because the relay advances its position right after.
 */
public abstract class EventPublisherContractTest {
    private static final Instant NOW = Instant.parse("2026-01-01T00:00:00Z");
    private static final String TENANT = "t1";

    /** A publisher whose events eventually reach {@code listener}. */
    protected abstract EventPublisher newPublisher(TagEventListener listener);

    /** A publisher whose transport is down. */
    protected abstract EventPublisher failingPublisher();

    /** {@code count} events of one tenant, with seqs 1..count. */
    private static List<TagEvent> events(long count) {
        return LongStream.rangeClosed(1, count)
                .mapToObj(s -> TagEvent.tag(TagEvent.Type.TAG_CREATED, TENANT, s, "alice", NOW).sequenced("s0", s))
                .toList();
    }

    @Test
    @DisplayName("events of a tenant arrive in seq order")
    void orderPerTenant() {
        List<TagEvent> got = new CopyOnWriteArrayList<>();
        EventPublisher p = newPublisher(got::addAll);
        List<TagEvent> sent = new ArrayList<>(events(50));
        p.publish(sent.subList(0, 20));
        p.publish(sent.subList(20, 50));
        await().until(() -> got.size() >= 50);
        assertThat(got.stream().filter(e -> e.tenantId().equals(TENANT)).map(TagEvent::seq).toList())
                .isSorted().hasSize(50);
    }

    @Test
    @DisplayName("an empty batch is a no-op")
    void emptyBatch() {
        List<TagEvent> got = new CopyOnWriteArrayList<>();
        newPublisher(got::addAll).publish(List.of());
        assertThat(got).isEmpty();
    }

    @Test
    @DisplayName("a transport failure surfaces as an exception, so the relay retries")
    void failurePropagates() {
        assertThatThrownBy(() -> failingPublisher().publish(events(1))).isInstanceOf(RuntimeException.class);
    }
}
