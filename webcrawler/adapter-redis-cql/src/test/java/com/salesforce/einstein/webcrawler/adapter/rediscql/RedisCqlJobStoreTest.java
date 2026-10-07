package com.salesforce.einstein.webcrawler.adapter.rediscql;

import com.datastax.oss.driver.api.core.CqlSession;
import com.salesforce.einstein.webcrawler.JobStoreContract;
import com.salesforce.einstein.webcrawler.model.CrawlJob;
import com.salesforce.einstein.webcrawler.model.CrawlRequest;
import com.salesforce.einstein.webcrawler.model.JobStatus;
import com.salesforce.einstein.webcrawler.store.JobStore;
import io.lettuce.core.RedisClient;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.cassandra.CassandraContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The {@link JobStoreContract} on real Redis, Cassandra and Postgres. */
@Testcontainers(disabledWithoutDocker = true)
class RedisCqlJobStoreTest extends JobStoreContract {

    @Container static final GenericContainer<?> REDIS = new GenericContainer<>("redis:7");
    static { REDIS.addExposedPort(6379); }
    /** Ready when CQL is up; the default strategy polls with cqlsh and logs every refused attempt as an ERROR. */
    @Container static final CassandraContainer CASSANDRA = new CassandraContainer("cassandra:4.1");
    static {
        CASSANDRA.setWaitStrategy(Wait.forLogMessage(".*Starting listening for CQL clients.*", 1)
                .withStartupTimeout(Duration.ofMinutes(3)));
    }
    @Container static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine");

    private static RedisClient redis;
    private static CqlSession cql;
    private static RedisCqlJobStore store;

    static RedisCqlSettings settings() {
        return new RedisCqlSettings("redis://" + REDIS.getHost() + ":" + REDIS.getMappedPort(6379),
                List.of(CASSANDRA.getHost() + ":" + CASSANDRA.getMappedPort(9042)), CASSANDRA.getLocalDatacenter(),
                "webcrawler", "{'class': 'SimpleStrategy', 'replication_factor': 1}",
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword(), 4, true,
                Duration.ofMinutes(5), Duration.ofSeconds(1));
    }

    @BeforeAll static void start() {
        RedisCqlSettings s = settings();
        redis = RedisClient.create(s.redisUri());
        cql = RedisCqlSchema.cqlSession(s);
        store = RedisCqlJobStore.open(s, redis, cql);
    }

    @AfterAll static void stop() {
        if (store != null) store.close();
        if (cql != null) cql.close();
        if (redis != null) redis.shutdown();
    }

    @Override protected JobStore store() { return store; }

    @Test void aSecondNodeSeesTheSameJobAndItsTerminalEvent() throws Exception {
        RedisCqlSettings s = settings();
        try (RedisCqlJobStore other = RedisCqlJobStore.open(s, redis, cql)) {
            CrawlJob job = store.createOrGet(new CrawlJob(
                    UUID.randomUUID().toString(), "t-" + UUID.randomUUID(), null,
                    CrawlRequest.builder("t", "https://a.com/").build(),
                    JobStatus.RUNNING, Instant.now(), null, null));
            CompletableFuture<String> heard = new CompletableFuture<>();
            other.onTerminal(j -> { if (j.jobId().equals(job.jobId())) heard.complete(j.status().name()); });
            assertTrue(store.transition(job.jobId(), job.status(),
                    JobStatus.COMPLETED, null));
            assertEquals("COMPLETED", heard.get(5, TimeUnit.SECONDS),
                    "a terminal transition on one node wakes listeners on every node");
        }
    }

    @Test void hostScheduleIsSharedAndExpires() throws Exception {
        Clock clock = Clock.systemUTC();
        try (RedisHostSchedule writer = new RedisHostSchedule(redis, clock);
             RedisHostSchedule reader = new RedisHostSchedule(redis, clock)) {
            String host = "h-" + UUID.randomUUID() + ".com";
            Instant at = Instant.ofEpochMilli(clock.millis() + 300);
            writer.put(host, at);
            assertEquals(at, reader.notBefore(host).orElseThrow());
            Thread.sleep(500);
            assertTrue(reader.notBefore(host).isEmpty(), "gone once its time has passed");
        }
    }
}
