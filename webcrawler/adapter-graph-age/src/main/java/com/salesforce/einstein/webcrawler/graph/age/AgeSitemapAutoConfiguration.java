package com.salesforce.einstein.webcrawler.graph.age;

import com.salesforce.einstein.webcrawler.config.CrawlerProperties;
import com.salesforce.einstein.webcrawler.sitemap.SitemapGraphs;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

/** {@code crawler.adapters.sitemap-graph: age}. Backs off to a {@link SitemapGraphs} the application defines. */
@AutoConfiguration
@ConditionalOnProperty(prefix = "crawler.adapters", name = "sitemap-graph", havingValue = "age")
@EnableConfigurationProperties(AgeSitemapSettings.class)
public class AgeSitemapAutoConfiguration {

    @Bean(destroyMethod = "close") @ConditionalOnMissingBean(SitemapGraphs.class)
    AgeSitemapGraphs ageSitemapGraphs(AgeSitemapSettings s, CrawlerProperties p) {
        return AgeSitemapGraphs.connect(s, p.sitemap().shards());
    }
}
