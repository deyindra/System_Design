package com.salesforce.einstein.webcrawler.adapter.kafka;

import com.salesforce.einstein.webcrawler.frontier.Frontier;
import com.salesforce.einstein.webcrawler.frontier.HostSchedule;
import com.salesforce.einstein.webcrawler.frontier.InMemoryHostSchedule;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import java.time.Clock;

/**
 * {@code crawler.adapters.frontier: kafka}. Runs after the store adapters so that a shared {@link HostSchedule} they
 * provide (Redis, in {@code webcrawler-adapter-redis-cql}) wins over the single-process fallback.
 */
@AutoConfiguration(afterName = "com.salesforce.einstein.webcrawler.adapter.rediscql.RedisCqlAutoConfiguration")
@ConditionalOnProperty(prefix = "crawler.adapters", name = "frontier", havingValue = "kafka")
@EnableConfigurationProperties(KafkaFrontierSettings.class)
public class KafkaFrontierAutoConfiguration {

    @Bean @ConditionalOnMissingBean HostSchedule hostSchedule() {
        LoggerFactory.getLogger(KafkaFrontierAutoConfiguration.class).warn(
                "no shared HostSchedule: politeness delays won't survive a partition moving to another node");
        return new InMemoryHostSchedule();
    }

    @Bean(destroyMethod = "close") @ConditionalOnMissingBean(Frontier.class)
    KafkaFrontier kafkaFrontier(KafkaFrontierSettings settings, HostSchedule schedule, Clock clock) {
        return new KafkaFrontier(settings, schedule, clock);
    }
}
