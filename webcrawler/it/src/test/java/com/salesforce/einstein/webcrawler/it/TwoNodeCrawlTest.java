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
import com.salesforce.einstein.webcrawler.graph.age.AgeSitemapGraphs;
import com.salesforce.einstein.webcrawler.model.CrawlJob;
import com.salesforce.einstein.webcrawler.model.JobStatus;
import com.salesforce.einstein.webcrawler.sitemap.NavigationSitemapParser;
import com.salesforce.einstein.webcrawler.sitemap.SitemapGraphs;
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
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.IntStream;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The deployment of INTERVIEW §11.1 in one JVM: two Spring Boot nodes of the unchanged application, made distributed
 * by configuration alone ({@code crawler.adapters.*}), sharing nothing but Kafka, Redis, Cassandra, Postgres and a
 * content directory. A job submitted over HTTP on one node is crawled by both and read back on the other. Sitemap
 * graphs are in Apache AGE, in the same Postgres as the jobs.
 */
@Testcontainers(disabledWithoutDocker = true)
class TwoNodeCrawlTest {

    @Container static final ConfluentKafkaContainer KAFKA = new ConfluentKafkaContainer("confluentinc/cp-kafka:7.6.0");
    @Container static final GenericContainer<?> REDIS = new GenericContainer<>("redis:7");
    static { REDIS.addExposedPort(6379); }
    /** Ready when CQL is up; the default strategy polls with cqlsh and logs every refused attempt as an ERROR. */
    @Container static final CassandraContainer CASSANDRA = new CassandraContainer("cassandra:4.1");
    static {
        CASSANDRA.setWaitStrategy(Wait.forLogMessage(".*Starting listening for CQL clients.*", 1)
                .withStartupTimeout(Duration.ofMinutes(3)));
    }
    /** Postgres with AGE; the server restarts once after initdb, so it is ready at the second message. */
    @Container static final GenericContainer<?> POSTGRES = new GenericContainer<>("apache/age:release_PG16_1.5.0");
    static {
        POSTGRES.addEnv("POSTGRES_PASSWORD", "it");
        POSTGRES.addExposedPort(5432);
        POSTGRES.setWaitStrategy(Wait.forLogMessage(".*database system is ready to accept connections.*\\s", 2)
                .withStartupTimeout(Duration.ofMinutes(2)));
    }

    private static final int PARTITIONS = 8;
    private static final String TENANT = "t-it";
    private static final List<String> HOSTS = IntStream.range(0, 8).mapToObj(i -> "https://h" + i + ".com").toList();
    /** The sitemap crawls' sites, apart from {@link #HOSTS}: a hit count per URL is then per test. */
    private static final List<String> SITEMAP_HOSTS = IntStream.range(0, 8).mapToObj(i -> "https://s" + i + ".com").toList();
    private static final String NAV = "https://nav.com/nav.xml";

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

    private static String jdbcUrl() {
        return "jdbc:postgresql://" + POSTGRES.getHost() + ":" + POSTGRES.getMappedPort(5432) + "/postgres";
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
                        "--crawler.adapters.sitemap-graph=age",
                        "--crawler.sitemap.shards=4",
                        "--crawler.sitemap.age.url=" + jdbcUrl(),
                        "--crawler.sitemap.age.password=it",
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
                        "--crawler.redis-cql.jdbc-url=" + jdbcUrl(),
                        "--crawler.redis-cql.jdbc-user=postgres",
                        "--crawler.redis-cql.jdbc-password=it");
    }

    @BeforeAll static void start() {
        WEB.page("https://hub.com/", links(HOSTS.stream().map(h -> h + "/").toArray(String[]::new)));
        for (String h : HOSTS) {
            WEB.page(h + "/", links("/1", "/2", "/3"));
            for (int i = 1; i <= 3; i++) WEB.page(h + "/" + i, links("/", "/" + i));
        }
        for (String s : SITEMAP_HOSTS) {                             // every page links off the sitemap
            WEB.page(s + "/", links("/off"));
            WEB.page(s + "/1", links("/off", "/2"));
            WEB.page(s + "/2", links("/off"));
            WEB.page(s + "/off", links("/"));
        }
        WEB.asset(NAV, "application/xml", sitemapXml().getBytes(StandardCharsets.UTF_8));
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

        int hitsBefore = WEB.totalHitsExcludingRobots();
        String body = "{\"seeds\":[\"https://hub.com/\"],\"maxDepth\":2,\"scope\":\"ANY\",\"respectRobots\":false}";
        HttpResponse<String> created = submit(a, "k-1", body);
        assertEquals(202, created.statusCode(), created.body());
        String jobId = json.readTree(created.body()).get("jobId").asText();

        CrawlJob done = b.getBean(CrawlEngine.class).await(jobId, Duration.ofSeconds(90));
        assertEquals(JobStatus.COMPLETED, done.status(), "node-b heard node-a's job finish");

        assertEquals(1 + 8 * 4, WEB.totalHitsExcludingRobots() - hitsBefore, "every page fetched exactly once, cluster-wide");
        assertOneNodePerHostAndBothCrawled(HOSTS);

        JsonNode view = json.readTree(get(b, "/v1/crawls/" + jobId).body());
        assertEquals("COMPLETED", view.get("status").asText());
        assertEquals(33, view.get("stats").get("pages").asLong());
        assertEquals(0, view.get("stats").get("pending").asLong());

        JsonNode pages = json.readTree(get(b, "/v1/crawls/" + jobId + "/pages?limit=100").body());
        assertTrue(pages.toString().contains("https://h7.com/3"), "the graph is readable from any node");

        HttpResponse<String> again = submit(b, "k-1", body);
        assertEquals(jobId, json.readTree(again.body()).get("jobId").asText(), "idempotent across nodes");
        assertEquals(33, WEB.totalHitsExcludingRobots() - hitsBefore, "and nothing was crawled again");
    }

    @Test void aSitemapJobIsCrawledByBothNodesFromTheGraphInAge() throws Exception {
        SitemapGraphs graphs = b.getBean(SitemapGraphs.class);
        assertEquals(AgeSitemapGraphs.class, graphs.getClass(), "selected by configuration");

        String body = "{\"sitemapUrl\":\"" + NAV + "\",\"maxDepth\":5,\"respectRobots\":false}";
        HttpResponse<String> created = submit(a, "k-sitemap", body);
        assertEquals(202, created.statusCode(), created.body());
        String jobId = json.readTree(created.body()).get("jobId").asText();
        String graph = "sitemap_" + jobId.replace("-", "");

        CrawlJob done = b.getBean(CrawlEngine.class).await(jobId, Duration.ofSeconds(90));
        assertEquals(JobStatus.COMPLETED, done.status(), done.error());
        for (String s : SITEMAP_HOSTS) {
            for (String path : List.of("/", "/1", "/2")) assertEquals(1, WEB.hits(s + path), s + path + " fetched once");
            assertEquals(0, WEB.hits(s + "/off"), "an HTML link is not followed");
        }
        assertOneNodePerHostAndBothCrawled(SITEMAP_HOSTS);

        JsonNode view = json.readTree(get(b, "/v1/crawls/" + jobId).body());
        assertEquals(NAV, view.get("request").get("sitemapUrl").asText());
        assertEquals(8 * 3, view.get("stats").get("pages").asLong());
        String pages = get(b, "/v1/crawls/" + jobId + "/pages?limit=100").body();
        for (JsonNode page : json.readTree(pages).get("items")) {
            String url = page.get("url").asText();
            int depth = url.equals("https://s0.com/") ? 0 : url.startsWith("https://s0.com/") || url.endsWith(".com/") ? 1 : 2;
            assertEquals(depth, page.get("depth").asInt(), url + ": its BFS depth in the sitemap");
        }
        await("the job's graph is dropped").atMost(Duration.ofSeconds(10)).until(() -> !graphs.exists(graph));
    }

    @Test void aSitemapGraphJobCrawlsASliceOfALoadedGraph() throws Exception {
        String graph = "it_catalog";
        String catalog = "https://nav.com/catalog.xml";
        WEB.asset(catalog, "application/xml", catalogXml().getBytes(StandardCharsets.UTF_8));
        HttpResponse<String> loading = send(a, "PUT", "/v1/sitemap-graphs/" + graph, "{\"sitemapUrl\":\"" + catalog + "\"}");
        assertEquals(202, loading.statusCode(), loading.body());

        JsonNode ready = await("node-b sees node-a's load end").atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofMillis(100))
                .until(() -> json.readTree(get(b, "/v1/sitemap-graphs/" + graph).body()),
                        v -> !v.get("status").asText().equals("LOADING"));
        assertEquals("READY", ready.get("status").asText(), ready.toString());
        assertEquals("https://c0.com/", ready.get("roots").get(0).asText(), "the page nothing links to");

        String body = "{\"sitemapGraph\":\"" + graph + "\",\"seeds\":[\"https://c0.com/\"],\"maxDepth\":2,"
                + "\"respectRobots\":false}";
        HttpResponse<String> created = submit(b, "k-graph", body);
        assertEquals(202, created.statusCode(), created.body());
        String jobId = json.readTree(created.body()).get("jobId").asText();

        CrawlJob done = a.getBean(CrawlEngine.class).await(jobId, Duration.ofSeconds(90));
        assertEquals(JobStatus.COMPLETED, done.status(), done.error());
        for (int i = 0; i < 5; i++) {
            assertEquals(i <= 2 ? 1 : 0, WEB.hits("https://c" + i + ".com/"), "c" + i + ": in the slice iff depth <= 2");
        }
        assertEquals(3, json.readTree(get(a, "/v1/crawls/" + jobId).body()).get("stats").get("pages").asLong());

        SitemapGraphs graphs = a.getBean(SitemapGraphs.class);
        assertTrue(graphs.exists(graph), "a named graph outlives its jobs");
        assertEquals(204, send(b, "DELETE", "/v1/sitemap-graphs/" + graph, null).statusCode());
        assertFalse(graphs.exists(graph), "deleted on node-b, gone for node-a");
    }

    // ------------------------------------------------------------------ helpers

    private static void assertOneNodePerHostAndBothCrawled(List<String> origins) {
        Set<String> workers = new HashSet<>();
        for (String origin : origins) {
            Set<String> nodes = NODES_PER_HOST.get(URI.create(origin).getHost());
            assertEquals(1, nodes.size(), origin + " was fetched by " + nodes);
            workers.addAll(nodes);
        }
        assertEquals(Set.of("node-a", "node-b"), workers, "both nodes crawled");
    }

    /** {@code s0/ -> s0/1, s0/2, s1/ .. s7/}; {@code sN/ -> sN/1, sN/2} for the others. */
    private static String sitemapXml() {
        List<String[]> urls = new ArrayList<>();
        List<String> root = new ArrayList<>(List.of(SITEMAP_HOSTS.get(0) + "/", SITEMAP_HOSTS.get(0) + "/1", SITEMAP_HOSTS.get(0) + "/2"));
        SITEMAP_HOSTS.subList(1, SITEMAP_HOSTS.size()).forEach(s -> root.add(s + "/"));
        urls.add(root.toArray(String[]::new));
        for (String s : SITEMAP_HOSTS.subList(1, SITEMAP_HOSTS.size())) urls.add(new String[] {s + "/", s + "/1", s + "/2"});
        for (String s : SITEMAP_HOSTS) {
            urls.add(new String[] {s + "/1"});
            urls.add(new String[] {s + "/2"});
        }
        return xml(urls);
    }

    /** A chain {@code c0/ -> c1/ -> .. -> c4/}, served by the catalog's own pages. */
    private static String catalogXml() {
        List<String[]> urls = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            WEB.page("https://c" + i + ".com/", links("/"));
            urls.add(i < 4 ? new String[] {"https://c" + i + ".com/", "https://c" + (i + 1) + ".com/"}
                    : new String[] {"https://c" + i + ".com/"});
        }
        return xml(urls);
    }

    /** One {@code <url>} per entry: its {@code loc}, then its navigation links. */
    private static String xml(List<String[]> urls) {
        StringBuilder sb = new StringBuilder("<urlset xmlns=\"http://www.sitemaps.org/schemas/sitemap/0.9\""
                + " xmlns:nav=\"" + NavigationSitemapParser.NAV_NS + "\">");
        for (String[] url : urls) {
            sb.append("<url><loc>").append(url[0]).append("</loc>");
            for (int i = 1; i < url.length; i++) sb.append("<nav:link href=\"").append(url[i]).append("\"/>");
            sb.append("</url>");
        }
        return sb.append("</urlset>").toString();
    }

    private static String links(String... hrefs) {
        StringBuilder sb = new StringBuilder("<html><body>");
        for (String h : hrefs) sb.append("<a href=\"").append(h).append("\">x</a>");
        return sb.append("</body></html>").toString();
    }

    private static URI uri(ConfigurableApplicationContext node, String path) {
        return URI.create("http://localhost:" + node.getEnvironment().getProperty("local.server.port") + path);
    }

    /** POST /v1/crawls; a second submit with the same key is the idempotency check. */
    private HttpResponse<String> submit(ConfigurableApplicationContext node, String key, String body) throws Exception {
        return http.send(HttpRequest.newBuilder(uri(node, "/v1/crawls")).header("X-Tenant-Id", TENANT)
                .header("Idempotency-Key", key).header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body)).build(), HttpResponse.BodyHandlers.ofString());
    }

    private HttpResponse<String> get(ConfigurableApplicationContext node, String path) throws Exception {
        HttpResponse<String> r = http.send(HttpRequest.newBuilder(uri(node, path)).header("X-Tenant-Id", TENANT).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        assertEquals(200, r.statusCode(), r.body());
        return r;
    }

    private HttpResponse<String> send(ConfigurableApplicationContext node, String method, String path, String body)
            throws Exception {
        return http.send(HttpRequest.newBuilder(uri(node, path)).header("X-Tenant-Id", TENANT)
                .header("Content-Type", "application/json")
                .method(method, body == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body))
                .build(), HttpResponse.BodyHandlers.ofString());
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
