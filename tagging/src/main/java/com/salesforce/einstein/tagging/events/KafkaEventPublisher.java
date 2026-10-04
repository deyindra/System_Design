package com.salesforce.einstein.tagging.events;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.salesforce.einstein.tagging.domain.TagEvent;
import com.salesforce.einstein.tagging.spi.EventPublisher;
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
 * Publishes to any Kafka-protocol broker (Apache Kafka, Strimzi, MSK, Confluent, Redpanda), so
 * nothing here is cloud-specific.
 *
 * <p>Key = tenantId, so a tenant's events land on one partition and stay ordered. The producer is
 * idempotent with {@code acks=all}, so retries never reorder or duplicate within a session.
 * {@link #publish} returns only after the broker has acknowledged every record; until then the relay
 * doesn't advance.
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
        return new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ProducerConfig.CLIENT_ID_CONFIG, clientId,
                ProducerConfig.ACKS_CONFIG, "all",
                ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true,
                ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5,
                ProducerConfig.LINGER_MS_CONFIG, 5,
                ProducerConfig.COMPRESSION_TYPE_CONFIG, "zstd",
                ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 30_000,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class));
    }

    @Override
    public void publish(List<TagEvent> events) {
        List<Future<RecordMetadata>> acks = new ArrayList<>(events.size());
        for (TagEvent e : events) {
            acks.add(producer.send(new ProducerRecord<>(topic, e.tenantId(), toJson(e))));
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

    private String toJson(TagEvent e) {
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
