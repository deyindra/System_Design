package com.salesforce.einstein.tagging.service;

import com.salesforce.einstein.tagging.domain.TagEvent;
import com.salesforce.einstein.tagging.spi.TagEventListener;
import com.salesforce.einstein.tagging.spi.TrendStore;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Feeds the trending projection from the event stream. It writes synchronously and lets failures
 * propagate, so with Kafka the shared consumer group commits an offset only after the counts are
 * durable, and redelivery is harmless because {@link TrendStore#record} is exactly-once.
 *
 * <p>Runs once per event cluster-wide (a shared consumer group), unlike the per-pod index and cache.
 */
public final class TrendingAggregator implements TagEventListener {
    private static final Logger log = LoggerFactory.getLogger(TrendingAggregator.class);

    private final TrendStore store;
    private final Counter counted;

    public TrendingAggregator(TrendStore store, MeterRegistry metrics) {
        this.store = store;
        this.counted = metrics.counter("tagging.trending.events");
    }

    @Override
    public void onEvents(List<TagEvent> events) {
        Map<String, List<TagEvent>> byShard = new LinkedHashMap<>();   // seq is per source shard
        for (TagEvent e : events) {
            if (e.type() == TagEvent.Type.TAGS_ATTACHED || e.type() == TagEvent.Type.TAG_DELETED) {
                byShard.computeIfAbsent(e.shard(), k -> new ArrayList<>()).add(e);
            }
        }
        byShard.forEach((shard, batch) -> {
            store.record(shard, batch);
            counted.increment(batch.size());
        });
    }

    /**
     * For the in-process publisher, where listeners run on the relay thread: a failure there would stall
     * the relay, and with it the index and cache, for a "popular tags" widget. This variant logs and
     * drops the batch instead, trading exact counts for isolation. The Kafka wiring doesn't need it.
     */
    public TagEventListener bestEffort() {
        return events -> {
            try {
                onEvents(events);
            } catch (RuntimeException e) {
                log.warn("trending dropped a batch of {} events", events.size(), e);
            }
        };
    }
}
