package com.salesforce.einstein.hierarchy.events;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.salesforce.einstein.hierarchy.domain.TreeEvent;
import com.salesforce.einstein.hierarchy.spi.EventPublisher;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Publishes to any Kafka-protocol broker. Key = tenant id, so a tenant's events stay on one partition in seq order. The
 * producer is idempotent with {@code acks=all}; {@link #publish} returns only after every record is acknowledged, and
 * until then the relay keeps the rows in the outbox.
 */
public final class KafkaEventPublisher implements EventPublisher, AutoCloseable {
    private final Producer<String, String> producer;
    private final String topic;
    private final ObjectMapper json;

    public KafkaEventPublisher(Producer<String, String> producer, String topic, ObjectMapper json) {
        this.producer = producer;
        this.topic = topic;
        this.json = json;
    }

    public static Producer<String, String> newProducer(String bootstrapServers, String clientId) {
        return new KafkaProducer<>(Map.ofEntries(
                Map.entry(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers),
                Map.entry(ProducerConfig.CLIENT_ID_CONFIG, clientId),
                Map.entry(ProducerConfig.ACKS_CONFIG, "all"),
                Map.entry(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true),
                Map.entry(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5),
                Map.entry(ProducerConfig.LINGER_MS_CONFIG, 5),
                Map.entry(ProducerConfig.COMPRESSION_TYPE_CONFIG, "zstd"),
                Map.entry(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 10_000),
                // delivery.timeout.ms must be at least linger.ms + request.timeout.ms
                Map.entry(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 30_000),
                Map.entry(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class),
                Map.entry(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class)));
    }

    @Override
    public void publish(List<TreeEvent> events) {
        List<Future<RecordMetadata>> acks = new ArrayList<>(events.size());
        for (TreeEvent e : events) {
            acks.add(producer.send(new ProducerRecord<>(topic, e.tenantId().toString(), toJson(e))));
        }
        try {
            for (Future<RecordMetadata> ack : acks) {
                ack.get(30, TimeUnit.SECONDS);
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while publishing", ex);
        } catch (ExecutionException | TimeoutException ex) {
            throw new IllegalStateException("kafka publish failed", ex);
        }
    }

    private String toJson(TreeEvent e) {
        try {
            return json.writeValueAsString(e);
        } catch (JsonProcessingException ex) {
            throw new IllegalStateException(ex);
        }
    }

    @Override
    public void close() {
        producer.close();
    }
}
