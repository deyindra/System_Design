package com.salesforce.einstein.webcrawler.graph.age;

import com.salesforce.einstein.webcrawler.sitemap.SitemapGraphs;
import com.salesforce.einstein.webcrawler.sitemap.SitemapGraphsContract;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;

/** The {@link SitemapGraphsContract} on PostgreSQL with Apache AGE. */
@Testcontainers(disabledWithoutDocker = true)
class AgeSitemapGraphsTest extends SitemapGraphsContract {

    /** The server restarts once after initdb, so it is ready at the second message. */
    @Container static final GenericContainer<?> AGE = new GenericContainer<>("apache/age:release_PG16_1.5.0");
    static {
        AGE.addEnv("POSTGRES_PASSWORD", "age");
        AGE.addExposedPort(5432);
        AGE.setWaitStrategy(Wait.forLogMessage(".*database system is ready to accept connections.*\\s", 2)
                .withStartupTimeout(Duration.ofMinutes(2)));
    }

    private static AgeSitemapGraphs graphs;

    @BeforeAll static void connect() {
        graphs = AgeSitemapGraphs.connect(new AgeSitemapSettings(
                "jdbc:postgresql://" + AGE.getHost() + ":" + AGE.getMappedPort(5432) + "/postgres",
                "postgres", "age", 4, 2), 4);                          // batches of 2: several queries per load
    }

    @AfterAll static void close() {
        if (graphs != null) graphs.close();
    }

    @Override protected SitemapGraphs graphsUnderTest() { return graphs; }
}
