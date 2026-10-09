package com.salesforce.einstein.hierarchy;

import com.fasterxml.jackson.databind.JsonNode;
import com.salesforce.einstein.hierarchy.cache.NoopTreeCache;
import com.salesforce.einstein.hierarchy.domain.MoveNode;
import com.salesforce.einstein.hierarchy.domain.Node;
import com.salesforce.einstein.hierarchy.domain.Space;
import com.salesforce.einstein.hierarchy.events.KafkaEventPublisher;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers(disabledWithoutDocker = true)
class KafkaEventPublisherIT {
    @Container
    static final ConfluentKafkaContainer KAFKA = new ConfluentKafkaContainer("confluentinc/cp-kafka:7.6.0");

    @Test
    void theRelayPublishesEventsKeyedByTenantInOrder() throws Exception {
        String topic = "hierarchy-events-" + UUID.randomUUID();
        try (KafkaEventPublisher publisher = new KafkaEventPublisher(
                KafkaEventPublisher.newProducer(KAFKA.getBootstrapServers(), "it"), topic, Fixture.JSON)) {
            Fixture f = new Fixture(10_000, new NoopTreeCache(), publisher);
            Space s = f.space("KAFKA");
            Node a = f.page(s, s.rootNodeId(), "a");
            Node b = f.page(s, s.rootNodeId(), "b");
            f.tree.move(f.alice, b.id(), new MoveNode(a.id(), null, null), null);
            f.relay.relayAll();

            List<ConsumerRecord<String, String>> mine = new ArrayList<>();
            try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(Map.of(
                    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                    ConsumerConfig.GROUP_ID_CONFIG, "it-" + UUID.randomUUID(),
                    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class))) {
                consumer.subscribe(List.of(topic));
                Instant deadline = Instant.now().plusSeconds(30);
                while (mine.size() < 4 && Instant.now().isBefore(deadline)) {
                    consumer.poll(Duration.ofMillis(500)).forEach(r -> {
                        if (r.key().equals(f.tenant.toString())) {
                            mine.add(r);
                        }
                    });
                }
            }
            List<JsonNode> events = new ArrayList<>();
            for (ConsumerRecord<String, String> r : mine) {
                events.add(Fixture.JSON.readTree(r.value()));
            }
            assertThat(events).extracting(e -> e.get("type").asText())
                    .containsExactly("SPACE_CREATED", "NODE_CREATED", "NODE_CREATED", "NODE_MOVED");
            assertThat(events).extracting(e -> e.get("seq").asLong()).isSorted();
            assertThat(mine).extracting(ConsumerRecord::partition).containsOnly(mine.get(0).partition());
            assertThat(events.get(3).get("oldParentId").asLong()).isEqualTo(s.rootNodeId());
        }
    }
}
