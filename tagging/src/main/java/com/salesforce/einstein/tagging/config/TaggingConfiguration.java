package com.salesforce.einstein.tagging.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.salesforce.einstein.tagging.cache.NoopDistributedCache;
import com.salesforce.einstein.tagging.cache.TagCache;
import com.salesforce.einstein.tagging.events.InProcessEventPublisher;
import com.salesforce.einstein.tagging.events.KafkaEventPublisher;
import com.salesforce.einstein.tagging.events.KafkaEventSubscriber;
import com.salesforce.einstein.tagging.events.OutboxRelay;
import com.salesforce.einstein.tagging.index.RoaringInvertedIndex;
import com.salesforce.einstein.tagging.service.AssignmentService;
import com.salesforce.einstein.tagging.service.IdempotencyService;
import com.salesforce.einstein.tagging.service.TagPurgeJob;
import com.salesforce.einstein.tagging.service.TagSearchService;
import com.salesforce.einstein.tagging.service.TagService;
import com.salesforce.einstein.tagging.service.TrendingAggregator;
import com.salesforce.einstein.tagging.service.TrendingService;
import com.salesforce.einstein.tagging.spi.Capability;
import com.salesforce.einstein.tagging.spi.DistributedCache;
import com.salesforce.einstein.tagging.spi.EventPublisher;
import com.salesforce.einstein.tagging.spi.IdGenerator;
import com.salesforce.einstein.tagging.spi.TagEventListener;
import com.salesforce.einstein.tagging.spi.TagSearchIndex;
import com.salesforce.einstein.tagging.spi.TagStore;
import com.salesforce.einstein.tagging.spi.TenantDirectory;
import com.salesforce.einstein.tagging.spi.TenantIsolation;
import com.salesforce.einstein.tagging.spi.TrendStore;
import com.salesforce.einstein.tagging.store.InMemoryTagStore;
import com.salesforce.einstein.tagging.store.JdbcSchema;
import com.salesforce.einstein.tagging.store.InMemoryTrendStore;
import com.salesforce.einstein.tagging.store.JdbcTagStore;
import com.salesforce.einstein.tagging.store.JdbcTrendStore;
import com.salesforce.einstein.tagging.tenant.Resilience4jTenantIsolation;
import com.salesforce.einstein.tagging.tenant.ShardRouter;
import com.salesforce.einstein.tagging.tenant.SnowflakeIdGenerator;
import com.salesforce.einstein.tagging.tenant.StaticTenantDirectory;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.Clock;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Wires ports to adapters. Each adapter is chosen by name in {@code tagging.providers.*}; a cloud-specific
 * adapter is a new module whose beans replace these (most are {@code @ConditionalOnMissingBean}).
 */
@Configuration(proxyBeanMethods = false)
@EnableConfigurationProperties(TaggingProperties.class)
public class TaggingConfiguration {

    @Bean
    @ConditionalOnMissingBean
    Clock clock() {
        return Clock.systemUTC();
    }

    // --- store, tenancy ----------------------------------------------------------------------------

    @Bean(destroyMethod = "close")
    @ConditionalOnMissingBean
    ShardStores shardStores(TaggingProperties props, ObjectMapper json, Clock clock) {
        Map<String, TagStore> stores = new LinkedHashMap<>();
        List<AutoCloseable> resources = new ArrayList<>();
        props.shards().forEach((id, shard) -> {
            switch (props.providers().store()) {
                case "memory" -> stores.put(id, new InMemoryTagStore(id, clock));
                case "jdbc" -> {
                    HikariDataSource primary = pool(id, shard.url() == null ? h2Url(id) : shard.url(), shard, false);
                    resources.add(primary);
                    JdbcSchema.migrate(primary);
                    HikariDataSource replica = null;
                    if (shard.replicaUrl() != null && !shard.replicaUrl().isBlank()) {
                        replica = pool(id, shard.replicaUrl(), shard, true);
                        resources.add(replica);
                    }
                    stores.put(id, new JdbcTagStore(id, primary, replica, json, clock, props.usageBuckets()));
                }
                default -> throw new IllegalArgumentException("unknown tagging.providers.store: " + props.providers().store());
            }
        });
        return new ShardStores(stores, resources);
    }

    /** An unconfigured shard (or trend store) is an in-process H2 database: zero-setup local runs. */
    private static String h2Url(String name) {
        return "jdbc:h2:mem:tagging-" + name
                + ";MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DEFAULT_NULL_ORDERING=HIGH;DB_CLOSE_DELAY=-1";
    }

    private static HikariDataSource pool(String shard, String url, TaggingProperties.Shard cfg, boolean replica) {
        return pool("tagging-" + shard + (replica ? "-replica" : ""), url, cfg.username(), cfg.password(),
                cfg.poolSize(), replica);
    }

    private static HikariDataSource pool(String name, String url, String user, String password, int size,
                                         boolean readOnly) {
        HikariConfig c = new HikariConfig();
        c.setPoolName(name);
        c.setJdbcUrl(url);
        c.setUsername(user);
        c.setPassword(password);
        c.setMaximumPoolSize(size);
        c.setReadOnly(readOnly);
        c.setAutoCommit(true);
        return new HikariDataSource(c);
    }

    @Bean
    @ConditionalOnMissingBean
    TenantDirectory tenantDirectory(TaggingProperties props) {
        return new StaticTenantDirectory(props);
    }

    @Bean
    ShardRouter shardRouter(TenantDirectory directory, ShardStores stores) {
        return new ShardRouter(directory, stores.byShard());
    }

    @Bean
    @ConditionalOnMissingBean
    TenantIsolation tenantIsolation(TaggingProperties props, MeterRegistry metrics) {
        return new Resilience4jTenantIsolation(props, metrics);
    }

    @Bean
    @ConditionalOnMissingBean
    IdGenerator idGenerator(TaggingProperties props, Clock clock) {
        return new SnowflakeIdGenerator(props.workerId(), clock);
    }

    // --- projections -------------------------------------------------------------------------------

    @Bean
    @ConditionalOnMissingBean
    DistributedCache distributedCache() {
        return new NoopDistributedCache();
    }

    @Bean
    TagCache tagCache(TaggingProperties props, DistributedCache l2) {
        return new TagCache(props.cache(), l2);
    }

    @Bean(destroyMethod = "shutdownNow")
    ExecutorService indexBootstrapExecutor(TaggingProperties props) {
        int n = props.index().bootstrapThreads();
        return new ThreadPoolExecutor(n, n, 0, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(1000),
                daemon("tagging-index-bootstrap"));
    }

    @Bean
    @ConditionalOnProperty(name = "tagging.index.enabled", havingValue = "true", matchIfMissing = true)
    @ConditionalOnMissingBean(TagSearchIndex.class)
    RoaringInvertedIndex tagSearchIndex(TaggingProperties props, ShardRouter router, ShardStores stores,
                                        @Qualifier("indexBootstrapExecutor") ExecutorService bootstrapExecutor, MeterRegistry metrics) {
        stores.requireCapability(Capability.SNAPSHOT_READS, "the inverted index bootstrap");
        RoaringInvertedIndex index = new RoaringInvertedIndex(router::store, props.index().maxTenants(),
                bootstrapExecutor);
        Gauge.builder("tagging.index.tenants", index, RoaringInvertedIndex::loadedTenants).register(metrics);
        return index;
    }

    // --- services ----------------------------------------------------------------------------------

    @Bean
    IdempotencyService idempotencyService(ObjectMapper json, Clock clock) {
        return new IdempotencyService(json, clock);
    }

    @Bean
    TagService tagService(ShardRouter router, TenantIsolation isolation, IdGenerator ids, TagCache cache,
                          TaggingProperties props, Clock clock) {
        return new TagService(router, isolation, ids, cache, props.limits(), clock);
    }

    @Bean
    AssignmentService assignmentService(ShardRouter router, TenantIsolation isolation, TagCache cache, IdGenerator ids,
                                        IdempotencyService idempotency, TaggingProperties props, Clock clock) {
        return new AssignmentService(router, isolation, cache, ids, idempotency, props.limits(), clock);
    }

    @Bean
    TagSearchService tagSearchService(ShardRouter router, TenantIsolation isolation,
                                      ObjectProvider<TagSearchIndex> index, TaggingProperties props,
                                      MeterRegistry metrics) {
        return new TagSearchService(router, isolation, index.getIfAvailable(), props, metrics);
    }

    @Bean(destroyMethod = "shutdownNow")
    ExecutorService purgeExecutor() {
        return Executors.newSingleThreadExecutor(daemon("tagging-purge"));
    }

    @Bean
    TagPurgeJob tagPurgeJob(ShardRouter router, @Qualifier("purgeExecutor") ExecutorService purgeExecutor, TaggingProperties props,
                            MeterRegistry metrics) {
        return new TagPurgeJob(router, purgeExecutor, props.purge().batchSize(), metrics);
    }

    // --- trending (an eventual projection of the event stream, in its own database) -----------------

    @Bean
    @ConditionalOnProperty(name = "tagging.trending.enabled", havingValue = "true", matchIfMissing = true)
    @ConditionalOnMissingBean
    TrendStore trendStore(TaggingProperties props, Clock clock) {
        TaggingProperties.Trending t = props.trending();
        if (props.providers().store().equals("memory")) {
            return new InMemoryTrendStore();
        }
        HikariDataSource ds = pool("tagging-trends", t.url() == null ? h2Url("trends") : t.url(), t.username(),
                t.password(), t.poolSize(), false);
        try {
            JdbcSchema.migrateTrends(ds);
        } catch (RuntimeException e) {
            ds.close();
            throw e;
        }
        return new JdbcTrendStore(ds, clock);   // closes the pool on shutdown
    }

    @Bean
    @ConditionalOnProperty(name = "tagging.trending.enabled", havingValue = "true", matchIfMissing = true)
    TrendingAggregator trendingAggregator(TrendStore store, MeterRegistry metrics) {
        return new TrendingAggregator(store, metrics);
    }

    @Bean
    @ConditionalOnProperty(name = "tagging.trending.enabled", havingValue = "true", matchIfMissing = true)
    TrendingService trendingService(ShardRouter router, TenantIsolation isolation, TrendStore store,
                                    TaggingProperties props, Clock clock) {
        return new TrendingService(router, isolation, store, props.trending(), clock);
    }

    // --- events ------------------------------------------------------------------------------------

    /** Projections every pod keeps for itself (each pod must see every event). */
    private static List<TagEventListener> perPod(ObjectProvider<TagSearchIndex> index, TagCache cache) {
        List<TagEventListener> l = new ArrayList<>();
        index.ifAvailable(l::add);
        l.add(cache);
        return l;
    }

    @Bean
    @ConditionalOnProperty(name = "tagging.providers.events", havingValue = "inprocess", matchIfMissing = true)
    EventPublisher inProcessEventPublisher(ObjectProvider<TagSearchIndex> index, TagCache cache, TagPurgeJob purge,
                                           ObjectProvider<TrendingAggregator> trending) {
        List<TagEventListener> l = perPod(index, cache);
        l.add(purge);
        trending.ifAvailable(t -> l.add(t.bestEffort()));
        return new InProcessEventPublisher(l);
    }

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnProperty(name = "tagging.providers.events", havingValue = "kafka")
    static class KafkaEvents {
        @Bean(destroyMethod = "close")
        KafkaEventPublisher kafkaEventPublisher(TaggingProperties props, ObjectMapper json) {
            TaggingProperties.Kafka k = props.kafka();
            return new KafkaEventPublisher(KafkaEventPublisher.newProducer(k.bootstrapServers(),
                    k.groupPrefix() + "-relay-" + podId(k)), k.topic(), json);
        }

        /** Every pod reads every event for its own index and cache (unique group, no commits). */
        @Bean
        KafkaEventSubscriber projectionSubscriber(TaggingProperties props, ObjectProvider<TagSearchIndex> index,
                                                  TagCache cache, ObjectMapper json) {
            TaggingProperties.Kafka k = props.kafka();
            return new KafkaEventSubscriber(
                    KafkaEventSubscriber.newConsumer(k.bootstrapServers(), k.groupPrefix() + "-proj-" + podId(k), true),
                    k.topic(), false, perPod(index, cache), json);
        }

        /** Work done once per event cluster-wide (shared group, committed offsets). */
        @Bean
        KafkaEventSubscriber purgeSubscriber(TaggingProperties props, TagPurgeJob purge, ObjectMapper json) {
            TaggingProperties.Kafka k = props.kafka();
            return new KafkaEventSubscriber(
                    KafkaEventSubscriber.newConsumer(k.bootstrapServers(), k.groupPrefix() + "-purge", false),
                    k.topic(), true, List.of(purge), json);
        }

        /** Trending counts, once cluster-wide. Its own group, so a slow trend store never delays the purge. */
        @Bean
        @ConditionalOnProperty(name = "tagging.trending.enabled", havingValue = "true", matchIfMissing = true)
        KafkaEventSubscriber trendingSubscriber(TaggingProperties props, TrendingAggregator trending, ObjectMapper json) {
            TaggingProperties.Kafka k = props.kafka();
            return new KafkaEventSubscriber(
                    KafkaEventSubscriber.newConsumer(k.bootstrapServers(), k.groupPrefix() + "-trends", false),
                    k.topic(), true, List.of(trending), json);
        }

        private static String podId(TaggingProperties.Kafka k) {
            if (k.podId() != null && !k.podId().isBlank()) {
                return k.podId();
            }
            // Each pod needs its own broadcast group: a shared one would split the events between pods.
            String host = System.getenv("HOSTNAME");
            return host != null && !host.isBlank() ? host : java.util.UUID.randomUUID().toString();
        }
    }

    @Bean
    OutboxRelay outboxRelay(ShardStores stores, EventPublisher publisher, TaggingProperties props, Clock clock,
                            MeterRegistry metrics) {
        stores.requireCapability(Capability.ATOMIC_OUTBOX, "the outbox relay");
        return new OutboxRelay(stores.byShard().values(), publisher, props.relay(), clock, metrics);
    }

    private static java.util.concurrent.ThreadFactory daemon(String name) {
        return r -> {
            Thread t = new Thread(r, name);
            t.setDaemon(true);
            return t;
        };
    }
}
