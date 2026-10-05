package com.salesforce.einstein.webcrawler.adapter.rediscql;

import com.datastax.oss.driver.api.core.CqlSession;
import com.salesforce.einstein.webcrawler.frontier.HostSchedule;
import com.salesforce.einstein.webcrawler.store.JobStore;
import io.lettuce.core.RedisClient;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import java.time.Clock;

/**
 * {@code crawler.adapters.job-store: redis-cql}. Runs before Boot's Cassandra autoconfiguration so that this
 * {@link CqlSession} is the one, not a second session from {@code spring.cassandra.*}. Every bean backs off to one the
 * application defines.
 *
 * <p>The JDBC pool is the store's own, not a {@code DataSource} bean, so it can't collide with the application's.
 */
@AutoConfiguration(beforeName = "org.springframework.boot.autoconfigure.cassandra.CassandraAutoConfiguration")
@ConditionalOnProperty(prefix = "crawler.adapters", name = "job-store", havingValue = "redis-cql")
@EnableConfigurationProperties(RedisCqlSettings.class)
public class RedisCqlAutoConfiguration {

    @Bean(destroyMethod = "shutdown") @ConditionalOnMissingBean
    RedisClient redisClient(RedisCqlSettings s) { return RedisClient.create(s.redisUri()); }

    @Bean(destroyMethod = "close") @ConditionalOnMissingBean
    CqlSession cqlSession(RedisCqlSettings s) { return RedisCqlSchema.cqlSession(s); }

    @Bean(destroyMethod = "close") @ConditionalOnMissingBean(JobStore.class)
    RedisCqlJobStore redisCqlJobStore(RedisCqlSettings s, RedisClient redis, CqlSession cql) {
        return RedisCqlJobStore.open(s, redis, cql);
    }

    /** Shared politeness for the Kafka frontier (its autoconfiguration runs after this one). */
    @Bean(destroyMethod = "close") @ConditionalOnMissingBean(HostSchedule.class)
    RedisHostSchedule redisHostSchedule(RedisClient redis, Clock clock) { return new RedisHostSchedule(redis, clock); }
}
