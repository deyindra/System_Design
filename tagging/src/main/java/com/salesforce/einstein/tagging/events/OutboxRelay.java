package com.salesforce.einstein.tagging.events;

import com.salesforce.einstein.tagging.config.TaggingProperties;
import com.salesforce.einstein.tagging.spi.EventPublisher;
import com.salesforce.einstein.tagging.spi.TagStore;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;

import java.time.Clock;
import java.time.Duration;
import java.util.Collection;
import java.util.List;

/**
 * Moves committed outbox events from every shard to the {@link EventPublisher}, in seq order.
 *
 * <p>Any number of pods may run the relay: the store serializes relays of one shard (row lock on the
 * relay position), so the duplicates are harmless and a pod crash only delays delivery. A failed
 * publish leaves the position unchanged and the batch is retried, giving at-least-once delivery.
 * Every consumer dedupes by seq.
 */
public final class OutboxRelay {
    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);

    private final List<TagStore> stores;
    private final EventPublisher publisher;
    private final TaggingProperties.Relay cfg;
    private final Clock clock;
    private final Counter relayed;
    private final Counter failures;

    public OutboxRelay(Collection<TagStore> stores, EventPublisher publisher, TaggingProperties.Relay cfg,
                       Clock clock, MeterRegistry metrics) {
        this.stores = List.copyOf(stores);
        this.publisher = publisher;
        this.cfg = cfg;
        this.clock = clock;
        this.relayed = metrics.counter("tagging.outbox.relayed");
        this.failures = metrics.counter("tagging.outbox.relay.failures");
        for (TagStore store : this.stores) {
            // Freshness SLI: how far projections (index, caches, downstream) trail this shard's commits.
            Gauge.builder("tagging.relay.lag", store, s -> lagSeconds(s, clock))
                    .tag("shard", store.shardId()).baseUnit("seconds").register(metrics);
        }
    }

    private static double lagSeconds(TagStore store, Clock clock) {
        try {
            return store.oldestUnrelayed()
                    .map(t -> Math.max(0, Duration.between(t, clock.instant()).toMillis() / 1000.0))
                    .orElse(0.0);
        } catch (RuntimeException e) {
            return Double.NaN;   // shard unreachable: the gauge shows a gap rather than a false 0
        }
    }

    /** Drains each shard up to {@code maxBatchesPerTick} batches. Returns the number of events published. */
    @Scheduled(fixedDelayString = "${tagging.relay.interval-ms:50}")
    public int relayAll() {
        int total = 0;
        for (TagStore store : stores) {
            try {
                for (int i = 0; i < cfg.maxBatchesPerTick(); i++) {
                    int n = store.relay(cfg.batchSize(), cfg.gapTimeout(), publisher::publish);
                    total += n;
                    relayed.increment(n);
                    if (n < cfg.batchSize()) {
                        break;
                    }
                }
            } catch (RuntimeException e) {
                failures.increment();
                log.warn("outbox relay of shard {} failed; will retry", store.shardId(), e);
            }
        }
        return total;
    }

    /** Hourly housekeeping: drop relayed outbox rows and expired idempotency keys. */
    @Scheduled(fixedDelayString = "${tagging.relay.prune-interval-ms:3600000}", initialDelay = 60_000)
    public void prune() {
        for (TagStore store : stores) {
            try {
                int events = store.pruneOutbox(clock.instant().minus(cfg.outboxRetention()));
                int keys = store.pruneIdempotency(clock.instant().minus(cfg.idempotencyRetention()));
                log.info("pruned shard {}: {} outbox rows, {} idempotency keys", store.shardId(), events, keys);
            } catch (RuntimeException e) {
                log.warn("pruning shard {} failed", store.shardId(), e);
            }
        }
    }
}
