package com.salesforce.einstein.hierarchy.events;

import com.salesforce.einstein.hierarchy.domain.TreeEvent;
import com.salesforce.einstein.hierarchy.spi.EventPublisher;
import com.salesforce.einstein.hierarchy.spi.TreeEventListener;
import com.salesforce.einstein.hierarchy.store.Shard;
import com.salesforce.einstein.hierarchy.store.TreeStore;
import com.salesforce.einstein.hierarchy.tenant.ShardRouter;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;

/**
 * Moves committed outbox events of every shard to the {@link EventPublisher}, in seq order, then hands them to the
 * local listeners (cache invalidation, the move worker).
 *
 * <p>Any number of pods may run it: a row lock on {@code outbox_relay_state} ({@code SKIP LOCKED}) lets one relay per
 * shard work at a time and the others skip. Rows are deleted only after the publish succeeded, in the same
 * transaction, so a crash or a failed publish re-sends the batch: delivery is at least once and consumers dedupe by
 * seq.
 */
public final class OutboxRelay {
    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);

    /**
     * @param batchSize         events per transaction
     * @param maxBatchesPerTick batches drained per shard per {@link #relayAll()} call
     * @param idempotencyTtl    how long create idempotency keys are kept
     */
    public record Settings(int batchSize, int maxBatchesPerTick, Duration idempotencyTtl) {
        public static Settings defaults() {
            return new Settings(500, 20, Duration.ofHours(24));
        }
    }

    private final ShardRouter shards;
    private final TreeStore store;
    private final EventPublisher publisher;
    private final List<TreeEventListener> listeners;
    private final Settings settings;
    private final Counter relayed;
    private final Counter failures;

    public OutboxRelay(ShardRouter shards, TreeStore store, EventPublisher publisher,
                       List<? extends TreeEventListener> listeners, Settings settings, MeterRegistry metrics) {
        this.shards = shards;
        this.store = store;
        this.publisher = publisher;
        this.listeners = List.copyOf(listeners);
        this.settings = settings;
        this.relayed = metrics.counter("hierarchy.outbox.relayed");
        this.failures = metrics.counter("hierarchy.outbox.relay.failures");
        for (Shard shard : shards.all()) {
            // Freshness SLI: how far caches and downstream consumers trail this shard's commits.
            Gauge.builder("hierarchy.outbox.lag", shard, this::lagSeconds)
                    .tag("shard", shard.name()).baseUnit("seconds").register(metrics);
        }
    }

    private double lagSeconds(Shard shard) {
        try {
            return shard.primary().read(store::outboxLagMs) / 1000.0;
        } catch (RuntimeException e) {
            return Double.NaN;   // shard unreachable: a gap in the graph, not a false 0
        }
    }

    /** Drains every shard up to {@code maxBatchesPerTick} batches. Returns the number of events relayed. */
    public int relayAll() {
        int total = 0;
        for (Shard shard : shards.all()) {
            try {
                for (int i = 0; i < settings.maxBatchesPerTick(); i++) {
                    List<TreeEvent> batch = relayBatch(shard);
                    total += batch.size();
                    relayed.increment(batch.size());
                    notifyListeners(batch);
                    if (batch.size() < settings.batchSize()) {
                        break;
                    }
                }
            } catch (RuntimeException e) {
                failures.increment();
                log.warn("outbox relay of shard {} failed; will retry", shard.name(), e);
            }
        }
        return total;
    }

    private List<TreeEvent> relayBatch(Shard shard) {
        return shard.primary().write(j -> {
            if (!store.lockRelay(j)) {
                return List.of();   // another pod is relaying this shard
            }
            List<TreeEvent> batch = store.outboxBatch(j, settings.batchSize());
            if (!batch.isEmpty()) {
                publisher.publish(batch);
                store.deleteRelayed(j, batch.stream().map(TreeEvent::seq).toList());
            }
            return batch;
        });
    }

    /** After commit. A failing listener is logged and skipped; it must not block the others or the relay. */
    private void notifyListeners(List<TreeEvent> batch) {
        if (batch.isEmpty()) {
            return;
        }
        for (TreeEventListener l : listeners) {
            try {
                l.onEvents(batch);
            } catch (RuntimeException e) {
                log.warn("listener {} failed on {} events", l.getClass().getSimpleName(), batch.size(), e);
            }
        }
    }

    /** Housekeeping: drops expired idempotency keys. */
    public void prune() {
        for (Shard shard : shards.all()) {
            try {
                int keys = shard.primary().write(j -> store.pruneIdempotencyKeys(j, settings.idempotencyTtl()));
                log.info("pruned shard {}: {} idempotency keys", shard.name(), keys);
            } catch (RuntimeException e) {
                log.warn("pruning shard {} failed", shard.name(), e);
            }
        }
    }
}
