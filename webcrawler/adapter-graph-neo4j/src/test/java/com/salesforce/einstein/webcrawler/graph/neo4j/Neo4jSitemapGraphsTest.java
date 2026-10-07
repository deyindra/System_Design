package com.salesforce.einstein.webcrawler.graph.neo4j;

import com.salesforce.einstein.webcrawler.sitemap.SitemapGraphs;
import com.salesforce.einstein.webcrawler.sitemap.SitemapGraphsContract;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;

/** The {@link SitemapGraphsContract} on Neo4j. */
@Testcontainers(disabledWithoutDocker = true)
class Neo4jSitemapGraphsTest extends SitemapGraphsContract {

    private static final String PASSWORD = "sitemap-test";

    @Container static final GenericContainer<?> NEO4J = new GenericContainer<>("neo4j:5");
    static {
        NEO4J.addEnv("NEO4J_AUTH", "neo4j/" + PASSWORD);
        NEO4J.addExposedPort(7687);
        NEO4J.setWaitStrategy(Wait.forLogMessage(".*Started\\..*\\s", 1).withStartupTimeout(Duration.ofMinutes(2)));
    }

    private static Neo4jSitemapGraphs graphs;

    @BeforeAll static void connect() {
        graphs = Neo4jSitemapGraphs.connect(new Neo4jSitemapSettings(
                "bolt://" + NEO4J.getHost() + ":" + NEO4J.getMappedPort(7687), "neo4j", PASSWORD, "neo4j", 2), 4);
    }

    @AfterAll static void close() {
        if (graphs != null) graphs.close();
    }

    @Override protected SitemapGraphs graphsUnderTest() { return graphs; }
}
