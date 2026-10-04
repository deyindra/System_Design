package com.salesforce.einstein.tagging;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.flyway.FlywayAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.scheduling.annotation.EnableScheduling;

/** Data sources are built per shard by {@code TaggingConfiguration}, so Boot's single-DataSource setup is off. */
@SpringBootApplication(exclude = {DataSourceAutoConfiguration.class, FlywayAutoConfiguration.class})
@EnableScheduling
public class TaggingApplication {
    public static void main(String[] args) {
        SpringApplication.run(TaggingApplication.class, args);
    }
}
