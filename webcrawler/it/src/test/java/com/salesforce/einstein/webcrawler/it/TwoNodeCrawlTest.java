package com.salesforce.einstein.webcrawler.it;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.salesforce.einstein.webcrawler.FakeWeb;
import com.salesforce.einstein.webcrawler.WebCrawlerApplication;
import com.salesforce.einstein.webcrawler.adapter.kafka.KafkaFrontier;
import com.salesforce.einstein.webcrawler.adapter.rediscql.RedisCqlJobStore;
import com.salesforce.einstein.webcrawler.adapter.rediscql.RedisHostSchedule;
import com.salesforce.einstein.webcrawler.engine.CrawlEngine;
import com.salesforce.einstein.webcrawler.fetch.Fetcher;
import com.salesforce.einstein.webcrawler.frontier.HostSchedule;
import com.salesforce.einstein.webcrawler.model.CrawlJob;
import com.salesforce.einstein.webcrawler.model.JobStatus;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.TypeExcludeFilter;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.core.env.Environment;
import org.springframework.core.type.classreading.MetadataReader;
import org.springframework.core.type.classreading.MetadataReaderFactory;
import org.testcontainers.cassandra.CassandraContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.IntStream;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The deployment of INTERVIEW §11.1 in one JVM: two Spring Boot nodes of the unchanged application, made distributed
 * by configuration alone ({@code crawler.adapters.*}), sharing nothing but Kafka, Redis, Cassandra, Postgres and a
 * content directory. A job submitted over HTTP on one node is crawled by both and read back on the other.
 */
@Testcontainers(disabledWithoutDocker = true)
class TwoNodeCrawlTest {

    @Container static final ConfluentKafkaContainer KAFKA = new ConfluentKafkaContainer("confluentinc/cp-kafka:7.6.0");
    @Container static final GenericContainer<?> REDIS = new GenericContainer<>("redis:7");
    static { REDIS.addExposedPort(6379); }
    /** Ready when CQL is up; the default strategy polls with cqlsh and logs every refused attempt as an ERROR. */
    @Container static final CassandraContainer CASSANDRA = new CassandraContainer("cassandra:4.1")
            .waitingFor(Wait.forLogMessage(".*Starting listening for CQL clients.*", 1)
                    .withStartupTimeout(Duration.ofMinutes(3)));
    @Container static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine");

    private static final int PARTITIONS = 8;
    private static final String TENANT = "t-it";
    private static final String KEY = "k-1";
    private static final List<String> HOSTS = IntStream.range(0, 8).mapToObj(i -> "https://h" + i + ".com").toList();

    /** The internet both nodes see; which node fetched each host is recorded by the fetcher. */
    static final FakeWeb WEB = new FakeWeb();
    static final Map<String, Set<String>> NODES_PER_HOST = new ConcurrentHashMap<>();

    @TempDir static Path blobs;
    private static ConfigurableApplicationContext a, b;
    private final HttpClient http = HttpClient.newHttpClient();
    private final ObjectMapper json = new ObjectMapper();

    /** Replaces the HTTP fetcher; a plain class (not scanned), passed to each node as a source. */
    static class Node {
        @Bean @Primary Fetcher fakeWeb(Environment env) {
            String node = env.getProperty("crawler.kafka.client-id");
            return req -> {
                NODES_PER_HOST.computeIfAbsent(req.url().getHost(), k -> ConcurrentHashMap.newKeySet()).add(node);
                return WEB.fetch(req);
            };
        }
    }

    /** The core test-jar is on the classpath, inside the application's scan; its test configurations stay out. */
    static final class SkipTestClasses extends TypeExcludeFilter {
        @Override public boolean match(MetadataReader r, MetadataReaderFactory f) {
            return r.getAnnotationMetadata().hasAnnotation(TestConfiguration.class.getName())
                    || r.getClassMetadata().getClassName().contains("Test");
        }
    }

    private static ConfigurableApplicationContext node(String name, String topic) {
        return new SpringApplicationBuilder(WebCrawlerApplication.class, Node.class)
                .initializers(ctx -> ctx.getBeanFactory().registerSingleton("skipTestClasses", new SkipTestClasses()))
                .run(                                     // arguments: they override application.yaml
                        "--server.port=0",
                        "--crawler.workers=4",
                        "--crawler.politeness-delay=0s",
                        "--crawler.retry-backoff=10ms",
                        "--crawler.adapters.frontier=kafka",
                        "--crawler.adapters.job-store=redis-cql",
                        "--crawler.adapters.content-store=filesystem",
                        "--crawler.adapters.content-root=" + blobs,
                        "--crawler.kafka.bootstrap-servers=" + KAFKA.getBootstrapServers(),
                        "--crawler.kafka.topic=" + topic,
                        "--crawler.kafka.partitions=" + PARTITIONS,
                        "--crawler.kafka.replication-factor=1",
                        "--crawler.kafka.group-id=g-" + topic,
                        "--crawler.kafka.client-id=" + name,
                        "--crawler.kafka.session-timeout=6s",
                        "--crawler.kafka.commit-interval=200ms",
                        "--crawler.kafka.fetch-lease=3s",
                        "--crawler.redis-cql.redis-uri=redis://" + REDIS.getHost() + ":" + REDIS.getMappedPort(6379),
                        "--crawler.redis-cql.cassandra-contact-points=" + CASSANDRA.getHost() + ":" + CASSANDRA.getMappedPort(9042),
                        "--crawler.redis-cql.cassandra-local-datacenter=" + CASSANDRA.getLocalDatacenter(),
                        "--crawler.redis-cql.jdbc-url=" + POSTGRES.getJdbcUrl(),
                        "--crawler.redis-cql.jdbc-user=" + POSTGRES.getUsername(),
                        "--crawler.redis-cql.jdbc-password=" + POSTGRES.getPassword());
    }

    @BeforeAll static void start() {
        WEB.page("https://hub.com/", links(HOSTS.stream().map(h -> h + "/").toArray(String[]::new)));
        for (String h : HOSTS) {
            WEB.page(h + "/", links("/1", "/2", "/3"));
            for (int i = 1; i <= 3; i++) WEB.page(h + "/" + i, links("/", "/" + i));
        }
        String topic = "frontier-" + UUID.randomUUID();
        a = node("node-a", topic);
        b = node("node-b", topic);
        awaitBalanced(a.getBean(KafkaFrontier.class), b.getBean(KafkaFrontier.class));
    }

    @AfterAll static void stop() {
        if (a != null) a.close();
        if (b != null) b.close();
    }

    @Test void aJobSubmittedOnOneNodeIsCrawledByBothAndReadOnTheOther() throws Exception {
        assertEquals(RedisCqlJobStore.class, a.getBean(CrawlEngine.class).jobs().getClass(), "selected by configuration");
        assertEquals(RedisHostSchedule.class, a.getBean(HostSchedule.class).getClass());

        String body = "{\"seeds\":[\"https://hub.com/\"],\"maxDepth\":2,\"scope\":\"ANY\",\"respectRobots\":false}";
        HttpResponse<String> created = submit(a, body);
        assertEquals(202, created.statusCode(), created.body());
        String jobId = json.readTree(created.body()).get("jobId").asText();

        CrawlJob done = b.getBean(CrawlEngine.class).await(jobId, Duration.ofSeconds(90));
        assertEquals(JobStatus.COMPLETED, done.status(), "node-b heard node-a's job finish");

        assertEquals(1 + 8 * 4, WEB.totalHitsExcludingRobots(), "every page fetched exactly once, cluster-wide");
        NODES_PER_HOST.forEach((h, ns) -> assertEquals(1, ns.size(), h + " was fetched by " + ns));
        Set<String> workers = new HashSet<>();
        NODES_PER_HOST.values().forEach(workers::addAll);
        assertEquals(Set.of("node-a", "node-b"), workers, "both nodes crawled");

        JsonNode view = json.readTree(get(b, "/v1/crawls/" + jobId).body());
        assertEquals("COMPLETED", view.get("status").asText());
        assertEquals(33, view.get("stats").get("pages").asLong());
        assertEquals(0, view.get("stats").get("pending").asLong());

        JsonNode pages = json.readTree(get(b, "/v1/crawls/" + jobId + "/pages?limit=100").body());
        assertTrue(pages.toString().contains("https://h7.com/3"), "the graph is readable from any node");

        HttpResponse<String> again = submit(b, body);
        assertEquals(jobId, json.readTree(again.body()).get("jobId").asText(), "idempotent across nodes");
        assertEquals(33, WEB.totalHitsExcludingRobots(), "and nothing was crawled again");
    }

    // ------------------------------------------------------------------ helpers

    private static String links(String... hrefs) {
        StringBuilder sb = new StringBuilder("<html><body>");
        for (String h : hrefs) sb.append("<a href=\"").append(h).append("\">x</a>");
        return sb.append("</body></html>").toString();
    }

    private static URI uri(ConfigurableApplicationContext node, String path) {
        return URI.create("http://localhost:" + node.getEnvironment().getProperty("local.server.port") + path);
    }

    /** POST /v1/crawls, always with {@link #KEY}: a second submit is the idempotency check. */
    private HttpResponse<String> submit(ConfigurableApplicationContext node, String body) throws Exception {
        return http.send(HttpRequest.newBuilder(uri(node, "/v1/crawls")).header("X-Tenant-Id", TENANT)
                .header("Idempotency-Key", KEY).header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body)).build(), HttpResponse.BodyHandlers.ofString());
    }

    private HttpResponse<String> get(ConfigurableApplicationContext node, String path) throws Exception {
        HttpResponse<String> r = http.send(HttpRequest.newBuilder(uri(node, path)).header("X-Tenant-Id", TENANT).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        assertEquals(200, r.statusCode(), r.body());
        return r;
    }

    /** Every partition owned, by both nodes, steady for a second. */
    private static void awaitBalanced(KafkaFrontier... fs) {
        await("the group settles").atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofMillis(100))
                .during(Duration.ofSeconds(1)).until(() -> balanced(fs));
    }

    private static boolean balanced(KafkaFrontier... fs) {
        Set<Integer> all = new HashSet<>();
        int total = 0;
        for (KafkaFrontier f : fs) {
            Set<Integer> p = f.partitions();
            if (p.isEmpty()) return false;
            all.addAll(p);
            total += p.size();
        }
        return all.size() == PARTITIONS && total == PARTITIONS;
    }
}
