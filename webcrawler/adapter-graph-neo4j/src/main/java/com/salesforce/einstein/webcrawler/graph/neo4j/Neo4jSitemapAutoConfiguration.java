package com.salesforce.einstein.webcrawler.graph.neo4j;

import com.salesforce.einstein.webcrawler.config.CrawlerProperties;
import com.salesforce.einstein.webcrawler.sitemap.SitemapGraphs;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

/** {@code crawler.adapters.sitemap-graph: neo4j}. Backs off to a {@link SitemapGraphs} the application defines. */
@AutoConfiguration
@ConditionalOnProperty(prefix = "crawler.adapters", name = "sitemap-graph", havingValue = "neo4j")
@EnableConfigurationProperties(Neo4jSitemapSettings.class)
public class Neo4jSitemapAutoConfiguration {

    @Bean(destroyMethod = "close") @ConditionalOnMissingBean(SitemapGraphs.class)
    Neo4jSitemapGraphs neo4jSitemapGraphs(Neo4jSitemapSettings s, CrawlerProperties p) {
        return Neo4jSitemapGraphs.connect(s, p.sitemap().shards());
    }
}
