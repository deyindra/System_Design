package com.salesforce.einstein.hierarchy.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.salesforce.einstein.hierarchy.cache.NoopTreeCache;
import com.salesforce.einstein.hierarchy.cache.RedisTreeCache;
import com.salesforce.einstein.hierarchy.events.CacheInvalidator;
import com.salesforce.einstein.hierarchy.events.InProcessEventPublisher;
import com.salesforce.einstein.hierarchy.events.KafkaEventPublisher;
import com.salesforce.einstein.hierarchy.events.OutboxRelay;
import com.salesforce.einstein.hierarchy.service.MoveJobWorker;
import com.salesforce.einstein.hierarchy.service.TreeService;
import com.salesforce.einstein.hierarchy.spi.EventPublisher;
import com.salesforce.einstein.hierarchy.spi.IdGenerator;
import com.salesforce.einstein.hierarchy.spi.TreeCache;
import com.salesforce.einstein.hierarchy.spi.TreeEventListener;
import com.salesforce.einstein.hierarchy.store.Db;
import com.salesforce.einstein.hierarchy.store.JdbcSchema;
import com.salesforce.einstein.hierarchy.store.ReplicaRouter;
import com.salesforce.einstein.hierarchy.store.Shard;
import com.salesforce.einstein.hierarchy.store.TreeStore;
import com.salesforce.einstein.hierarchy.tenant.ShardRouter;
import com.salesforce.einstein.hierarchy.tenant.SnowflakeIdGenerator;
import com.salesforce.einstein.hierarchy.tenant.TenantDirectory;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.metrics.micrometer.MicrometerMetricsTrackerFactory;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Wires the service from {@link HierarchyProperties}. Each shard gets its own Hikari pools (primary plus replicas),
 * migrated at startup; they are owned by the {@link ShardRouter} rather than exposed as {@code DataSource} beans, so
 * Boot's single-database auto-configuration stays out of the way.
 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(HierarchyProperties.class)
public class HierarchyConfiguration {

    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }

    @Bean
    ShardRouter shardRouter(HierarchyProperties props, MeterRegistry metrics, Clock clock) {
        List<Shard> shards = new ArrayList<>();
        for (Map.Entry<String, HierarchyProperties.Shard> e : props.shards().entrySet()) {
            String name = e.getKey();
            HierarchyProperties.Shard cfg = e.getValue();
            Db primary = new Db(name, pool(name + "-primary", cfg, cfg.url(), false, metrics));
            JdbcSchema.migrate(primary.dataSource());
            List<Db> replicas = new ArrayList<>();
            for (int i = 0; i < cfg.replicas().size(); i++) {
                String rname = name + "-replica-" + i;
                replicas.add(new Db(rname, pool(rname, cfg, cfg.replicas().get(i), true, metrics)));
            }
            ReplicaRouter router = new ReplicaRouter(primary, replicas, ReplicaRouter.sql(primary), clock,
                    props.replicas().maxLag());
            shards.add(new Shard(name, primary, router));
        }
        TenantDirectory directory = new TenantDirectory(List.copyOf(props.shards().keySet()), props.placements());
        return new ShardRouter(directory, shards);
    }

    private static HikariDataSource pool(String name, HierarchyProperties.Shard cfg, String url, boolean readOnly,
                                         MeterRegistry metrics) {
        HikariConfig c = new HikariConfig();
        c.setPoolName(name);
        c.setJdbcUrl(url);
        c.setUsername(cfg.username());
        c.setPassword(cfg.password());
        c.setMaximumPoolSize(cfg.poolSize());
        c.setReadOnly(readOnly);
        c.setMetricsTrackerFactory(new MicrometerMetricsTrackerFactory(metrics));
        return new HikariDataSource(c);
    }

    @Bean
    TreeStore treeStore(ObjectMapper json) {
        return new TreeStore(json);
    }

    @Bean
    IdGenerator idGenerator(HierarchyProperties props, Clock clock) {
        return new SnowflakeIdGenerator(props.workerId(), clock);
    }

    // ------------------------------------------------------------------ cache

    @Bean
    @ConditionalOnProperty(name = "hierarchy.cache.type", havingValue = "none", matchIfMissing = true)
    TreeCache noopTreeCache() {
        return new NoopTreeCache();
    }

    @Bean(destroyMethod = "shutdown")
    @ConditionalOnProperty(name = "hierarchy.cache.type", havingValue = "redis")
    RedisClient redisClient(HierarchyProperties props) {
        return RedisClient.create(props.cache().redisUri());
    }

    @Bean(destroyMethod = "close")
    @ConditionalOnProperty(name = "hierarchy.cache.type", havingValue = "redis")
    StatefulRedisConnection<String, String> redisConnection(RedisClient client) {
        return client.connect();
    }

    @Bean
    @ConditionalOnProperty(name = "hierarchy.cache.type", havingValue = "redis")
    TreeCache redisTreeCache(StatefulRedisConnection<String, String> connection, HierarchyProperties props) {
        return new RedisTreeCache(connection, props.cache().ttl(), props.cache().nodeTtl());
    }

    // ------------------------------------------------------------------ events

    @Bean
    @ConditionalOnProperty(name = "hierarchy.events.type", havingValue = "inprocess", matchIfMissing = true)
    EventPublisher inProcessEventPublisher() {
        return new InProcessEventPublisher();
    }

    @Bean
    @ConditionalOnProperty(name = "hierarchy.events.type", havingValue = "kafka")
    EventPublisher kafkaEventPublisher(HierarchyProperties props, ObjectMapper json) {
        return new KafkaEventPublisher(KafkaEventPublisher.newProducer(props.events().kafkaBootstrap(),
                "hierarchy-relay-" + props.workerId()), props.events().topic(), json);
    }

    @Bean
    CacheInvalidator cacheInvalidator(TreeCache cache) {
        return new CacheInvalidator(cache);
    }

    @Bean
    OutboxRelay outboxRelay(ShardRouter shards, TreeStore store, EventPublisher publisher,
                            List<TreeEventListener> listeners, HierarchyProperties props, MeterRegistry metrics) {
        HierarchyProperties.Relay r = props.relay();
        return new OutboxRelay(shards, store, publisher, listeners,
                new OutboxRelay.Settings(r.batchSize(), r.maxBatchesPerTick(), r.idempotencyTtl()), metrics);
    }

    // ------------------------------------------------------------------ service

    @Bean
    TreeService treeService(ShardRouter shards, TreeStore store, IdGenerator ids, TreeCache cache,
                            HierarchyProperties props) {
        return new TreeService(shards, store, ids, cache, new TreeService.Settings(props.move().syncLimit(),
                props.lockTimeout(), props.move().purgeBatchSize()));
    }

    @Bean
    MoveJobWorker moveJobWorker(ShardRouter shards, TreeStore store, TreeCache cache, HierarchyProperties props) {
        HierarchyProperties.Move m = props.move();
        String owner = System.getenv().getOrDefault("HOSTNAME", "local") + "-"
                + UUID.randomUUID().toString().substring(0, 8);
        return new MoveJobWorker(shards, store, cache, new MoveJobWorker.Settings(m.batchSize(), m.lease(),
                m.maxReplicaLag().toMillis(), m.pause(), m.workerEnabled(), props.lockTimeout()), owner);
    }

    @Bean
    Schedules schedules(OutboxRelay relay, MoveJobWorker worker, ShardRouter shards, HierarchyProperties props) {
        return new Schedules(relay, worker, shards, props);
    }

    @Bean
    ShardHealthIndicator shardHealthIndicator(ShardRouter shards) {
        return new ShardHealthIndicator(shards);
    }
}
