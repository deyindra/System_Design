package com.salesforce.einstein.webcrawler;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

/** Single-process build: every SPI on its in-memory implementation (see {@code CrawlerConfiguration}). */
@SpringBootApplication
@ConfigurationPropertiesScan
public class WebCrawlerApplication {
    public static void main(String[] args) {
        SpringApplication.run(WebCrawlerApplication.class, args);
    }
}
