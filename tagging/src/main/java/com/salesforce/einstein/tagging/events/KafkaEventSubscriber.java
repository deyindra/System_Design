package com.salesforce.einstein.tagging.events;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.salesforce.einstein.tagging.domain.TagEvent;
import com.salesforce.einstein.tagging.spi.TagEventListener;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Feeds Kafka events to local {@link TagEventListener}s. Two modes:
 * <ul>
 *   <li><b>broadcast</b> (a group id unique to the pod, starting at the latest offset, no commits): every
 *       pod sees every event. It feeds per-pod projections: the inverted index and L1 cache invalidation.
 *       A restarted pod rebuilds them from the store, so old events aren't needed.</li>
 *   <li><b>shared</b> (one group for the fleet, committed offsets): each event is handled once by one pod.
 *       It feeds side-effecting jobs such as the tag purge.</li>
 * </ul>
 * Offsets are committed only after the listeners return (at-least-once), and the listeners dedupe.
 */
public final class KafkaEventSubscriber implements SmartLifecycle {
    private static final Logger log = LoggerFactory.getLogger(KafkaEventSubscriber.class);

    private final Consumer<String, String> consumer;
    private final String topic;
    private final boolean commit;
    private final List<TagEventListener> listeners;
    private final ObjectMapper json;
    private volatile boolean running;
    private Thread thread;

    public KafkaEventSubscriber(Consumer<String, String> consumer, String topic, boolean commit,
                                List<? extends TagEventListener> listeners, ObjectMapper json) {
        this.consumer = consumer;
        this.topic = topic;
        this.commit = commit;
        this.listeners = List.copyOf(listeners);
        this.json = json;
    }

    public static Consumer<String, String> newConsumer(String bootstrapServers, String groupId, boolean broadcast) {
        return new KafkaConsumer<>(Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ConsumerConfig.GROUP_ID_CONFIG, groupId,
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, broadcast ? "latest" : "earliest",
                ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed",
                ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1000,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class));
    }

    @Override
    public void start() {
        running = true;
        thread = new Thread(this::loop, "kafka-subscriber-" + topic);
        thread.setDaemon(true);
        thread.start();
    }

    private void loop() {
        try {
            consumer.subscribe(List.of(topic));
            while (running) {
                try {
                    pollOnce();
                } catch (WakeupException e) {
                    break;   // shutting down
                } catch (RuntimeException e) {
                    // e.g. CommitFailedException after a rebalance: the uncommitted records are redelivered
                    // (to this pod or another) and the listeners dedupe, so keep going.
                    log.error("kafka subscriber for {} failed; continuing", topic, e);
                    pause(1_000);
                }
            }
        } finally {
            running = false;   // isRunning() reports a dead loop
            consumer.close();
        }
    }

    private void pollOnce() {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
        if (records.isEmpty()) {
            return;
        }
        List<TagEvent> events = new ArrayList<>(records.count());
        for (ConsumerRecord<String, String> r : records) {
            try {
                events.add(json.readValue(r.value(), TagEvent.class));
            } catch (IOException e) {
                // Only our own relay writes this topic, so this means a version skew or a bug. Projections
                // fed by this pod may miss the event until their tenant is rebuilt; alert on this log.
                log.error("skipping undecodable event at {}-{}@{}", r.topic(), r.partition(), r.offset(), e);
            }
        }
        deliver(events);
        if (commit) {
            consumer.commitSync();
        }
    }

    /**
     * Retries each listener until it succeeds or the subscriber stops. Skipping a batch would leave a
     * projection permanently wrong, while a stalled one only falls behind: its watermark stops, SESSION
     * reads fall back to the store, and the consumer-lag alert fires.
     */
    private void deliver(List<TagEvent> events) {
        for (TagEventListener l : listeners) {
            for (int attempt = 1; running; attempt++) {
                try {
                    l.onEvents(events);
                    break;
                } catch (RuntimeException e) {
                    log.warn("listener {} failed (attempt {}); retrying", l.getClass().getSimpleName(), attempt, e);
                    pause(Math.min(100L << Math.min(attempt, 7), 10_000));
                }
            }
        }
    }

    private static void pause(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new WakeupException();
        }
    }

    @Override
    public void stop() {
        running = false;
        consumer.wakeup();
        if (thread != null) {
            try {
                thread.join(5_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }
}
