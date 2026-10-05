package com.salesforce.einstein.webcrawler.adapter.kafka;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

import java.time.Duration;
import java.util.Map;

/**
 * {@code crawler.kafka.*}.
 *
 * @param partitions        fixed for the life of the topic: a host's partition is {@code hash(host) % partitions}, so
 *                          changing it would move every host. Make it a multiple of the largest fleet you expect.
 * @param clientId          this node's name in the group (logs, metrics); the host name in production
 * @param sessionTimeout    how long a silent node keeps its partitions; the failover time for a crashed node
 * @param fetchLease        written to the {@code HostSchedule} when a fetch starts, so a host handed over mid-fetch
 *                          isn't hit by the new owner before the old request can have finished
 * @param maxBuffered       tasks fetched from the log and waiting in this node's back queues before it pauses
 * @param extra             passed through to the producer and consumer (security, SASL, TLS)
 */
@ConfigurationProperties("crawler.kafka")
public record KafkaFrontierSettings(
        @DefaultValue("localhost:9092") String bootstrapServers,
        @DefaultValue("crawl.frontier") String topic,
        @DefaultValue("64") int partitions,
        @DefaultValue("3") short replicationFactor,
        @DefaultValue("true") boolean createTopic,
        @DefaultValue("webcrawler") String groupId,
        @DefaultValue("webcrawler-node") String clientId,
        @DefaultValue("45s") Duration sessionTimeout,
        @DefaultValue("1s") Duration commitInterval,
        @DefaultValue("30s") Duration fetchLease,
        @DefaultValue("30s") Duration sendTimeout,
        @DefaultValue("10000") int maxBuffered,
        @DefaultValue Map<String, String> extra) {

    public KafkaFrontierSettings {
        extra = extra == null ? Map.of() : Map.copyOf(extra);
    }

    /** Defaults for a test or a single broker, with the given bootstrap servers and node name. */
    public static KafkaFrontierSettings of(String bootstrapServers, String clientId) {
        return new KafkaFrontierSettings(bootstrapServers, "crawl.frontier", 16, (short) 1, true, "webcrawler",
                clientId, Duration.ofSeconds(6), Duration.ofMillis(200), Duration.ofSeconds(30),
                Duration.ofSeconds(30), 10_000, Map.of());
    }

    public KafkaFrontierSettings withTopic(String t, int p) {
        return new KafkaFrontierSettings(bootstrapServers, t, p, replicationFactor, createTopic, groupId, clientId,
                sessionTimeout, commitInterval, fetchLease, sendTimeout, maxBuffered, extra);
    }

    public KafkaFrontierSettings withFetchLease(Duration l) {
        return new KafkaFrontierSettings(bootstrapServers, topic, partitions, replicationFactor, createTopic, groupId,
                clientId, sessionTimeout, commitInterval, l, sendTimeout, maxBuffered, extra);
    }

    public KafkaFrontierSettings withGroup(String g) {
        return new KafkaFrontierSettings(bootstrapServers, topic, partitions, replicationFactor, createTopic, g,
                clientId, sessionTimeout, commitInterval, fetchLease, sendTimeout, maxBuffered, extra);
    }
}
