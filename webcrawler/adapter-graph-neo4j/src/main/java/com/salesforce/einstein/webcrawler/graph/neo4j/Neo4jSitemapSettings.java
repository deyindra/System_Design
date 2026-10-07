package com.salesforce.einstein.webcrawler.graph.neo4j;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * {@code crawler.sitemap.neo4j.*}: the Neo4j database that holds the sitemap graphs.
 *
 * @param batchSize nodes or relationships per query when a sitemap is loaded
 */
@ConfigurationProperties("crawler.sitemap.neo4j")
public record Neo4jSitemapSettings(
        @DefaultValue("bolt://localhost:7687") String uri,
        @DefaultValue("neo4j") String user,
        @DefaultValue("") String password,
        @DefaultValue("neo4j") String database,
        @DefaultValue("500") int batchSize) {
}
