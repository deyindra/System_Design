package com.salesforce.einstein.hierarchy;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.flyway.FlywayAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.JdbcTemplateAutoConfiguration;
import org.springframework.scheduling.annotation.EnableScheduling;

/** Data sources are built per shard by {@code HierarchyConfiguration}, so Boot's single-DataSource setup is off. */
@SpringBootApplication(exclude = {DataSourceAutoConfiguration.class, DataSourceTransactionManagerAutoConfiguration.class,
        JdbcTemplateAutoConfiguration.class, FlywayAutoConfiguration.class})
@EnableScheduling
public class HierarchyApplication {
    public static void main(String[] args) {
        SpringApplication.run(HierarchyApplication.class, args);
    }
}
