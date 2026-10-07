package com.salesforce.einstein.webcrawler.graph.age;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * {@code crawler.sitemap.age.*}: the PostgreSQL database with the AGE extension that holds the sitemap graphs.
 *
 * @param batchSize nodes or edges per query when a sitemap is loaded
 */
@ConfigurationProperties("crawler.sitemap.age")
public record AgeSitemapSettings(
        @DefaultValue("jdbc:postgresql://localhost:5432/postgres") String url,
        @DefaultValue("postgres") String user,
        @DefaultValue("") String password,
        @DefaultValue("8") int poolSize,
        @DefaultValue("1000") int batchSize) {
}
